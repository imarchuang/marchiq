package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const groupsVersion = 1

// Group is a v0 consumer group, intentionally small (PLAN): static membership,
// one committed offset per (topic, partition), no rebalance protocol. Members
// join in order; ordinal = join index; partition p goes to the member with
// ordinal == p % size. A second member never steals partitions — it gets the
// partitions its ordinal covers, or the group is full (409 at the HTTP edge).
type Group struct {
	Name   string
	topics map[string]*groupTopic
}

type groupTopic struct {
	Size    int            // declared member count, fixed by the first join
	Members []string       // join order = ordinal for static assignment
	Offsets map[int]Offset // partition -> last committed offset (absent = -1)
}

// Assignment is the result of joining a group: the member's static partitions.
type Assignment struct {
	Group      string `json:"group"`
	Member     string `json:"member"`
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

// PartitionFetch is one partition's slice of a group fetch response.
type PartitionFetch struct {
	Partition     int      `json:"partition"`
	Records       []Record `json:"records"`
	NextOffset    Offset   `json:"next_offset"`
	HighWatermark Offset   `json:"high_watermark"`
}

// groupsFile is the versioned envelope for meta/groups.json. Like the topic
// catalog, groups are written in name order for stable, reviewable diffs.
type groupsFile struct {
	Version int          `json:"version"`
	Groups  []groupState `json:"groups"`
}

type groupState struct {
	Name   string            `json:"name"`
	Topics []groupTopicState `json:"topics"`
}

type groupTopicState struct {
	Topic   string         `json:"topic"`
	Size    int            `json:"size"`
	Members []string       `json:"members"`
	Offsets map[int]Offset `json:"offsets"`
}

// JoinGroup registers a member and returns its static partition assignment.
// The first join for (group, topic) fixes the declared size; later joins must
// match it. Rejoining with the same member id is idempotent. Membership is
// persisted so restart keeps assignments deterministic.
func (b *Broker) JoinGroup(group, topic, member string, size int) (Assignment, error) {
	if !topicName.MatchString(group) {
		return Assignment{}, fmt.Errorf("invalid group name %q", group)
	}
	if size < 1 {
		return Assignment{}, fmt.Errorf("members must be >= 1")
	}
	t, err := b.getTopic(topic)
	if err != nil {
		return Assignment{}, err
	}
	b.gmu.Lock()
	defer b.gmu.Unlock()
	if b.closed {
		return Assignment{}, ErrClosed
	}
	g := b.groups[group]
	if g == nil {
		g = &Group{Name: group, topics: map[string]*groupTopic{}}
		b.groups[group] = g
	}
	gt := g.topics[topic]
	if gt == nil {
		gt = &groupTopic{Size: size, Offsets: map[int]Offset{}}
		g.topics[topic] = gt
	}
	if gt.Size != size {
		return Assignment{}, fmt.Errorf("%w: %s/%s declared size %d, got %d", ErrGroupConflict, group, topic, gt.Size, size)
	}
	ordinal := -1
	for i, m := range gt.Members {
		if m == member {
			ordinal = i
			break
		}
	}
	if ordinal < 0 {
		if len(gt.Members) >= gt.Size {
			return Assignment{}, fmt.Errorf("%w: group %s full (%d members)", ErrGroupConflict, group, gt.Size)
		}
		if member == "" {
			member = fmt.Sprintf("member-%d", len(gt.Members)+1)
		}
		gt.Members = append(gt.Members, member)
		ordinal = len(gt.Members) - 1
	}
	if err := b.saveGroupsLocked(); err != nil {
		return Assignment{}, fmt.Errorf("persist groups: %w", err)
	}
	parts := make([]int, 0, t.config.Partitions)
	for p := 0; p < t.config.Partitions; p++ {
		if p%gt.Size == ordinal {
			parts = append(parts, p)
		}
	}
	return Assignment{Group: group, Member: member, Topic: topic, Partitions: parts}, nil
}

// CommitOffset records the last PROCESSED offset for (group, topic, partition);
// the next group fetch starts at offset+1. Durability protocol matches the
// topic catalog: persist first, keep the in-memory change only on success.
// Rewinding (committing an older offset) is allowed — replay is a feature.
func (b *Broker) CommitOffset(group, topic string, partition int, offset Offset) error {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return err
	}
	off, err := p.Offsets()
	if err != nil {
		return err
	}
	if offset < 0 || offset >= off.Latest {
		return fmt.Errorf("%w: commit offset %d not in [0, %d)", ErrOffsetOutOfRange, offset, off.Latest)
	}
	b.gmu.Lock()
	defer b.gmu.Unlock()
	if b.closed {
		return ErrClosed
	}
	gt, err := b.groupTopicLocked(group, topic)
	if err != nil {
		return err
	}
	old, existed := gt.Offsets[partition]
	gt.Offsets[partition] = offset
	if err := b.saveGroupsLocked(); err != nil {
		if existed {
			gt.Offsets[partition] = old
		} else {
			delete(gt.Offsets, partition)
		}
		return fmt.Errorf("persist groups: %w", err)
	}
	return nil
}

// FetchGroup returns records from committed_offset+1 for every partition
// assigned to the calling member (at-least-once: commit after processing).
// member may be omitted only when the group has exactly one member.
func (b *Broker) FetchGroup(group, topic, member string, maxRecords int, maxBytes int64) ([]PartitionFetch, error) {
	t, err := b.getTopic(topic)
	if err != nil {
		return nil, err
	}
	b.gmu.RLock()
	if b.closed {
		b.gmu.RUnlock()
		return nil, ErrClosed
	}
	gt, err := b.groupTopicLocked(group, topic)
	if err != nil {
		b.gmu.RUnlock()
		return nil, err
	}
	ordinal := 0
	switch {
	case len(gt.Members) == 1 && (member == "" || member == gt.Members[0]):
	case member == "":
		b.gmu.RUnlock()
		return nil, fmt.Errorf("%w: group %s has %d members", ErrMemberRequired, group, len(gt.Members))
	default:
		ordinal = -1
		for i, m := range gt.Members {
			if m == member {
				ordinal = i
				break
			}
		}
		if ordinal < 0 {
			b.gmu.RUnlock()
			return nil, fmt.Errorf("%w: member %q not in group %s", ErrGroupNotFound, member, group)
		}
	}
	committed := make(map[int]Offset, len(gt.Offsets))
	for k, v := range gt.Offsets {
		committed[k] = v
	}
	size := gt.Size
	b.gmu.RUnlock()
	out := make([]PartitionFetch, 0, t.config.Partitions)
	for pi := 0; pi < t.config.Partitions; pi++ {
		if pi%size != ordinal {
			continue
		}
		off, err := t.partitions[pi].Offsets()
		if err != nil {
			return nil, err
		}
		start := Offset(0) // nothing committed yet => read from the beginning
		if c, ok := committed[pi]; ok {
			start = c + 1
		}
		if start > off.Latest { // defensive: committed never exceeds LEO-1
			start = off.Latest
		}
		recs := []Record{}
		if start < off.Latest {
			if recs, err = t.partitions[pi].ReadFrom(start, maxRecords, maxBytes); err != nil {
				return nil, err
			}
		}
		next := start
		if len(recs) > 0 {
			next = recs[len(recs)-1].Offset + 1
		}
		out = append(out, PartitionFetch{Partition: pi, Records: recs, NextOffset: next, HighWatermark: off.Latest})
	}
	return out, nil
}

func (b *Broker) groupTopicLocked(group, topic string) (*groupTopic, error) {
	g := b.groups[group]
	if g == nil {
		return nil, fmt.Errorf("%w: %s (join first)", ErrGroupNotFound, group)
	}
	gt := g.topics[topic]
	if gt == nil {
		return nil, fmt.Errorf("%w: %s never joined topic %s", ErrGroupNotFound, group, topic)
	}
	return gt, nil
}

func (b *Broker) getTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	t, ok := b.topics[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	return t, nil
}

// loadGroups reads meta/groups.json; a missing file means no groups yet.
func loadGroups(dataDir string) (map[string]*Group, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "meta", "groups.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*Group{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read groups: %w", err)
	}
	var gf groupsFile
	if err := json.Unmarshal(data, &gf); err != nil {
		return nil, fmt.Errorf("parse groups: %w", err)
	}
	if gf.Version != groupsVersion {
		return nil, fmt.Errorf("unsupported groups version %d", gf.Version)
	}
	out := make(map[string]*Group, len(gf.Groups))
	for _, gs := range gf.Groups {
		if !topicName.MatchString(gs.Name) {
			return nil, fmt.Errorf("groups: invalid group name %q", gs.Name)
		}
		if _, dup := out[gs.Name]; dup {
			return nil, fmt.Errorf("groups: duplicate group %q", gs.Name)
		}
		g := &Group{Name: gs.Name, topics: make(map[string]*groupTopic, len(gs.Topics))}
		for _, ts := range gs.Topics {
			if ts.Size < 1 {
				return nil, fmt.Errorf("groups: %s/%s size %d", gs.Name, ts.Topic, ts.Size)
			}
			if ts.Offsets == nil {
				ts.Offsets = map[int]Offset{}
			}
			g.topics[ts.Topic] = &groupTopic{Size: ts.Size, Members: ts.Members, Offsets: ts.Offsets}
		}
		out[gs.Name] = g
	}
	return out, nil
}

// saveGroupsLocked persists the full group state with the same atomic protocol
// as the topic catalog: tmp file → sync → rename → sync the meta directory.
func (b *Broker) saveGroupsLocked() error {
	gf := groupsFile{Version: groupsVersion, Groups: make([]groupState, 0, len(b.groups))}
	for _, g := range b.groups {
		gs := groupState{Name: g.Name, Topics: make([]groupTopicState, 0, len(g.topics))}
		for topic, gt := range g.topics {
			gs.Topics = append(gs.Topics, groupTopicState{
				Topic: topic, Size: gt.Size, Members: gt.Members, Offsets: gt.Offsets,
			})
		}
		sort.Slice(gs.Topics, func(i, j int) bool { return gs.Topics[i].Topic < gs.Topics[j].Topic })
		gf.Groups = append(gf.Groups, gs)
	}
	sort.Slice(gf.Groups, func(i, j int) bool { return gf.Groups[i].Name < gf.Groups[j].Name })
	data, err := json.MarshalIndent(gf, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Join(b.dataDir, "meta")
	tmp := filepath.Join(dir, "groups.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "groups.json")); err != nil {
		return err
	}
	return syncDir(dir)
}
