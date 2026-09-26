package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"
)

// groupsVersion is the current meta/groups.json envelope version. v2 replaces
// the Slice 4 static-size model with dynamic membership: size is gone and
// every (group, topic) binding carries a generation. v1 files are migrated
// on load (see parseGroupsV1).
const groupsVersion = 2

const (
	// DefaultSessionTimeout is how long a group member may go without a
	// heartbeat before the reaper evicts it. (Kafka's session.timeout.ms
	// defaults to 45s; demos want something visible.)
	DefaultSessionTimeout = 10 * time.Second
	// DefaultGroupReapInterval is how often the background reaper runs one
	// pass. Tests call ReapExpiredMembers directly instead of sleeping.
	DefaultGroupReapInterval = time.Second
)

// Group is a consumer group with DYNAMIC membership (Slice 7): members join
// and leave over time, a heartbeat-refreshed session keeps them alive, and
// the reaper evicts members whose session expired. Assignment is recomputed
// over the LIVE member list on every change — the member at index i owns
// partition p iff i == p % len(members) — so a join, leave, or eviction
// redistributes partitions automatically (docs/kafka-notes.md §3).
type Group struct {
	Name   string
	topics map[string]*groupTopic
}

type groupTopic struct {
	Members    []string             // live members in join order; persisted
	Generation int                  // bumped on every membership change; persisted
	Offsets    map[int]Offset       // partition -> last committed offset (absent = -1)
	LastSeen   map[string]time.Time // last heartbeat per member; IN-MEMORY ONLY (sessions are ephemeral)
}

// Assignment is the result of joining a group: the member's partitions under
// the returned generation. More members than partitions is NOT an error —
// trailing members get an empty assignment (Kafka's idle consumer).
type Assignment struct {
	Group      string `json:"group"`
	Member     string `json:"member"`
	Topic      string `json:"topic"`
	Generation int    `json:"generation"`
	Partitions []int  `json:"partitions"`
}

// HeartbeatResult is the successful heartbeat reply: the group's current
// generation and the member's assignment under it, so a client picks up
// rebalanced partitions by pulling, nothing is pushed.
type HeartbeatResult struct {
	Generation int   `json:"generation"`
	Partitions []int `json:"partitions"`
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
	Topic      string         `json:"topic"`
	Generation int            `json:"generation"`
	Members    []string       `json:"members"`
	Offsets    map[int]Offset `json:"offsets"`
}

// JoinGroup adds a member to the group and returns its assignment under the
// resulting generation. A brand-new member is appended to the join order and
// the generation is bumped — a membership change IS the rebalance signal, so
// the very first join of a fresh (group, topic) yields generation 1.
// Rejoining while still a member is idempotent (no bump) and refreshes the
// session; rejoining after an eviction is simply a new join — the member
// lands at the END of the order, unlike Kafka static membership where a
// fenced instance id reclaims its old assignment. Membership and generation
// are persisted; the session (LastSeen) is not.
func (b *Broker) JoinGroup(group, topic, member string) (Assignment, error) {
	if !topicName.MatchString(group) {
		return Assignment{}, fmt.Errorf("invalid group name %q", group)
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
	createdG := g == nil
	if createdG {
		g = &Group{Name: group, topics: map[string]*groupTopic{}}
		b.groups[group] = g
	}
	gt := g.topics[topic]
	createdGT := gt == nil
	if createdGT {
		gt = &groupTopic{Offsets: map[int]Offset{}, LastSeen: map[string]time.Time{}}
		g.topics[topic] = gt
	}
	now := time.Now()
	if member != "" && slices.Contains(gt.Members, member) {
		// Idempotent rejoin: no membership change, but still a sign of life.
		gt.LastSeen[member] = now
		return Assignment{Group: group, Member: member, Topic: topic,
			Generation: gt.Generation, Partitions: assignmentFor(gt, member, t.config.Partitions)}, nil
	}
	if member == "" {
		member = nextMemberID(gt)
	}
	gt.Members = append(gt.Members, member)
	gt.Generation++
	gt.LastSeen[member] = now
	if err := b.saveGroupsLocked(); err != nil {
		// Disk is the truth: roll the join back out of memory too.
		gt.Members = gt.Members[:len(gt.Members)-1]
		gt.Generation--
		delete(gt.LastSeen, member)
		if createdGT {
			delete(g.topics, topic)
		}
		if createdG {
			delete(b.groups, group)
		}
		return Assignment{}, fmt.Errorf("persist groups: %w", err)
	}
	return Assignment{Group: group, Member: member, Topic: topic,
		Generation: gt.Generation, Partitions: assignmentFor(gt, member, t.config.Partitions)}, nil
}

// nextMemberID picks the smallest free member-N id. The Slice 4 len+1 scheme
// collides once evictions shrink the list (members [member-1, member-2],
// evict member-1, join "" would mint "member-2" again), so scan instead.
func nextMemberID(gt *groupTopic) string {
	for n := 1; ; n++ {
		id := fmt.Sprintf("member-%d", n)
		if !slices.Contains(gt.Members, id) {
			return id
		}
	}
}

// assignmentFor computes the member's partitions under the live-member rule:
// index i owns p iff i == p % len(members). An unknown member or an empty
// member list yields an empty (non-nil) slice — callers resolve members
// first, this is defensive.
func assignmentFor(gt *groupTopic, member string, partitions int) []int {
	parts := []int{}
	n := len(gt.Members)
	if n == 0 {
		return parts
	}
	i := slices.Index(gt.Members, member)
	if i < 0 {
		return parts
	}
	for p := 0; p < partitions; p++ {
		if p%n == i {
			parts = append(parts, p)
		}
	}
	return parts
}

// Heartbeat keeps a member's session alive and is the client's REBALANCE
// SENSOR: the reply carries the current generation and the member's current
// assignment, and a stale generation is rejected with ErrGenerationFence
// naming the current one — clients learn about rebalances by pulling. Any
// heartbeat from a LIVE member refreshes its session, even a fenced one: the
// member proved it is alive, and refusing the refresh would let the reaper
// evict a healthy-but-behind client. An evicted (unknown) member gets
// ErrGroupNotFound — the zombie's signal to rejoin from scratch.
func (b *Broker) Heartbeat(group, topic, member string, generation int) (HeartbeatResult, error) {
	t, err := b.getTopic(topic)
	if err != nil {
		return HeartbeatResult{}, err
	}
	b.gmu.Lock()
	defer b.gmu.Unlock()
	if b.closed {
		return HeartbeatResult{}, ErrClosed
	}
	gt, err := b.groupTopicLocked(group, topic)
	if err != nil {
		return HeartbeatResult{}, err
	}
	if !slices.Contains(gt.Members, member) {
		return HeartbeatResult{}, fmt.Errorf("%w: member %q not in group %s", ErrGroupNotFound, member, group)
	}
	gt.LastSeen[member] = time.Now()
	if generation != gt.Generation {
		return HeartbeatResult{}, fmt.Errorf("%w: heartbeat generation %d, current %d", ErrGenerationFence, generation, gt.Generation)
	}
	return HeartbeatResult{Generation: gt.Generation, Partitions: assignmentFor(gt, member, t.config.Partitions)}, nil
}

// LeaveGroup removes a member and bumps the generation: the survivors'
// assignments are recomputed over the shrunken live list. The group itself is
// NEVER deleted — committed offsets must survive the last member leaving
// because the retention clamp depends on them.
func (b *Broker) LeaveGroup(group, topic, member string) error {
	b.gmu.Lock()
	defer b.gmu.Unlock()
	if b.closed {
		return ErrClosed
	}
	gt, err := b.groupTopicLocked(group, topic)
	if err != nil {
		return err
	}
	i := slices.Index(gt.Members, member)
	if i < 0 {
		return fmt.Errorf("%w: member %q not in group %s", ErrGroupNotFound, member, group)
	}
	oldMembers := gt.Members
	oldSeen, hadSeen := gt.LastSeen[member]
	gt.Members = slices.Delete(slices.Clone(oldMembers), i, i+1)
	gt.Generation++
	delete(gt.LastSeen, member)
	if err := b.saveGroupsLocked(); err != nil {
		gt.Members = oldMembers
		gt.Generation--
		if hadSeen {
			gt.LastSeen[member] = oldSeen
		}
		return fmt.Errorf("persist groups: %w", err)
	}
	return nil
}

// CommitOffset records the last PROCESSED offset for (group, topic,
// partition); the next group fetch starts at offset+1. The generation must
// equal the group's current generation — this is the FENCE that stops a
// zombie member (evicted after a GC pause, still believing it owns the
// partition) from clobbering the group's progress (docs/kafka-notes.md §3).
// Fencing guards the WRITE path only; reads stay unfenced. The commit stays
// a group-level write (no member in the payload): the generation IS the
// capability. Durability protocol matches the topic catalog: persist first,
// keep the in-memory change only on success. Rewinding (committing an older
// offset) is allowed — replay is a feature.
func (b *Broker) CommitOffset(group, topic string, partition int, offset Offset, generation int) error {
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
	if generation != gt.Generation {
		return fmt.Errorf("%w: commit generation %d, current %d", ErrGenerationFence, generation, gt.Generation)
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
// member may be omitted only when the group has exactly one member. Fetches
// are deliberately NOT generation-fenced (Kafka-faithful: a duplicate read
// costs a duplicate process, a polluted commit costs the group's cursor),
// but the member must be in the CURRENT live list — an evicted zombie's
// fetch fails member-not-found here.
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
	n := len(gt.Members) // >= 1: member resolution above failed otherwise
	b.gmu.RUnlock()
	out := make([]PartitionFetch, 0, t.config.Partitions)
	for pi := 0; pi < t.config.Partitions; pi++ {
		if pi%n != ordinal {
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
		if start < off.Earliest {
			// Retention already reclaimed everything below earliest. This can
			// only happen for a group that joined after the deletion with no
			// commits (the retention clamp protects every joined group's
			// cursor): start at the oldest surviving record —
			// auto.offset.reset=earliest semantics.
			start = off.Earliest
		}
		if start > off.Latest {
			// acks=0 data loss can rewind LEO below committed+1 (an OS crash
			// took records the group already committed — DURABILITY.md).
			// Clamp to an empty poll at LEO: never error, never skip data,
			// never rewrite the committed offset; new records arriving at the
			// reused offsets are delivered from here.
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
		b.countFetch(recs)
		out = append(out, PartitionFetch{Partition: pi, Records: recs, NextOffset: next, HighWatermark: off.Latest})
	}
	return out, nil
}

// ReapExpiredMembers runs ONE reaper pass: every member whose last heartbeat
// is older than the session timeout at `now` is evicted — removed from the
// live list, generation bumped, groups.json republished. It never sleeps and
// never touches partition data: the ticker loop is a thin wrapper and tests
// drive this method directly with a synthetic `now` (the same pattern as
// EnforceRetention). Committed offsets are never the reaper's business.
//
// Lock order: mu (read the timeout) is released before gmu is taken; the
// pass itself takes gmu only.
func (b *Broker) ReapExpiredMembers(now time.Time) (int, error) {
	b.mu.RLock()
	timeout := b.sessionTimeout
	b.mu.RUnlock()
	if timeout <= 0 {
		timeout = DefaultSessionTimeout
	}
	b.gmu.Lock()
	defer b.gmu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	// Two phases per binding — collect, then mutate — so a persist failure
	// can roll the whole pass back to match the disk.
	type change struct {
		gt         *groupTopic
		oldMembers []string
		evicted    map[string]time.Time // member -> LastSeen, for rollback
	}
	var changes []change
	evicted := 0
	for _, g := range b.groups {
		for _, gt := range g.topics {
			ch := change{gt: gt, oldMembers: gt.Members, evicted: map[string]time.Time{}}
			kept := make([]string, 0, len(gt.Members))
			for _, m := range gt.Members {
				last, ok := gt.LastSeen[m]
				if ok && now.Sub(last) <= timeout {
					kept = append(kept, m)
					continue
				}
				// A member with no LastSeen violates the join invariant;
				// fail closed by evicting it too.
				ch.evicted[m] = last
			}
			if len(ch.evicted) == 0 {
				continue
			}
			for m := range ch.evicted {
				delete(gt.LastSeen, m)
			}
			gt.Members = kept
			gt.Generation++
			evicted += len(ch.evicted)
			changes = append(changes, ch)
		}
	}
	if len(changes) == 0 {
		return 0, nil
	}
	if err := b.saveGroupsLocked(); err != nil {
		for _, ch := range changes { // disk is the truth: roll the evictions back
			ch.gt.Members = ch.oldMembers
			ch.gt.Generation--
			for m, seen := range ch.evicted {
				ch.gt.LastSeen[m] = seen
			}
		}
		return 0, fmt.Errorf("persist groups: %w", err)
	}
	return evicted, nil
}

// groupReapLoop mirrors retentionLoop: a timer around the deterministic
// single pass, re-reading the interval each cycle (under mu, never held
// across the pass) so SetGroupReapInterval is race-free.
func (b *Broker) groupReapLoop() {
	defer b.reapWg.Done()
	for {
		b.mu.RLock()
		interval := b.reapInterval
		b.mu.RUnlock()
		if interval <= 0 {
			interval = DefaultGroupReapInterval
		}
		t := time.NewTimer(interval)
		select {
		case <-b.reapStop:
			t.Stop()
			return
		case <-t.C:
		}
		n, err := b.ReapExpiredMembers(time.Now())
		if err != nil && !errors.Is(err, ErrClosed) {
			log.Printf("marchiq group reaper: %v", err)
		}
		if n > 0 {
			// Evictions are the visible heartbeat of the rebalance protocol —
			// worth one log line each tick in a teaching system.
			log.Printf("marchiq group reaper: evicted %d expired member(s)", n)
		}
	}
}

// SetSessionTimeout changes how long a member may go without a heartbeat
// before the reaper evicts it. Non-positive values are ignored.
func (b *Broker) SetSessionTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	b.mu.Lock()
	b.sessionTimeout = d
	b.mu.Unlock()
}

// SetGroupReapInterval changes how often the background reaper ticks. The
// loop re-reads the interval each cycle, so the new value applies from the
// next tick on — no kick channel, because this interval is small (1s
// default) and set before the broker starts serving.
func (b *Broker) SetGroupReapInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	b.mu.Lock()
	b.reapInterval = d
	b.mu.Unlock()
}

// PartitionLag is one partition's group cursor against the log end. Committed
// is null when the group never committed here. Lag = latest - next_fetch; it
// goes NEGATIVE when data loss (e.g. acks=0 + OS crash) rewound LEO below the
// committed offset — that is the observable signature, not an error to hide.
type PartitionLag struct {
	Partition int     `json:"partition"`
	Committed *Offset `json:"committed"`
	NextFetch Offset  `json:"next_fetch"`
	Latest    Offset  `json:"latest"`
	Lag       Offset  `json:"lag"`
}

// GroupLag is one (group, topic) binding's lag report; members and the
// generation are included for demo readability (the generation is the fence
// commits must carry).
type GroupLag struct {
	Group      string         `json:"group"`
	Topic      string         `json:"topic"`
	Generation int            `json:"generation"`
	Members    []string       `json:"members"`
	Partitions []PartitionLag `json:"partitions"`
}

// GroupLag reports every joined (group, topic, partition) cursor against the
// current LEO; group "" reports all groups. Lock order: gmu snapshot first,
// released before mu/partition locks — mu → gmu is the only allowed nesting,
// and this method avoids nesting entirely (same pattern as FetchGroup).
func (b *Broker) GroupLag(group string) ([]GroupLag, error) {
	type snap struct {
		group, topic string
		generation   int
		members      []string
		offsets      map[int]Offset
	}
	b.gmu.RLock()
	if b.closed {
		b.gmu.RUnlock()
		return nil, ErrClosed
	}
	var snaps []snap
	for _, g := range b.groups {
		if group != "" && g.Name != group {
			continue
		}
		for topic, gt := range g.topics {
			offsets := make(map[int]Offset, len(gt.Offsets))
			for p, o := range gt.Offsets {
				offsets[p] = o
			}
			snaps = append(snaps, snap{g.Name, topic, gt.Generation, slices.Clone(gt.Members), offsets})
		}
	}
	b.gmu.RUnlock()
	if group != "" && len(snaps) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrGroupNotFound, group)
	}
	out := make([]GroupLag, 0, len(snaps))
	for _, s := range snaps {
		t, err := b.getTopic(s.topic)
		if err != nil {
			return nil, err
		}
		gl := GroupLag{Group: s.group, Topic: s.topic, Generation: s.generation, Members: s.members,
			Partitions: make([]PartitionLag, 0, len(t.partitions))}
		for pi := range t.partitions {
			off, err := t.partitions[pi].Offsets()
			if err != nil {
				return nil, err
			}
			pl := PartitionLag{Partition: pi, Latest: off.Latest}
			if c, ok := s.offsets[pi]; ok {
				pl.Committed = &c
				pl.NextFetch = c + 1
			}
			pl.Lag = pl.Latest - pl.NextFetch
			gl.Partitions = append(gl.Partitions, pl)
		}
		out = append(out, gl)
	}
	// Map iteration order is random; sort for stable, reviewable output —
	// the same reason catalogs persist in name order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Topic < out[j].Topic
	})
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
// v1 files (Slice 4–6: static size, no generation) are migrated on load;
// anything newer than v2 fails closed, like the topic catalog.
func loadGroups(dataDir string) (map[string]*Group, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "meta", "groups.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*Group{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read groups: %w", err)
	}
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("parse groups: %w", err)
	}
	switch probe.Version {
	case 1:
		return parseGroupsV1(data)
	case groupsVersion:
		return parseGroups(data)
	default:
		return nil, fmt.Errorf("unsupported groups version %d", probe.Version)
	}
}

func parseGroups(data []byte) (map[string]*Group, error) {
	var gf groupsFile
	if err := json.Unmarshal(data, &gf); err != nil {
		return nil, fmt.Errorf("parse groups: %w", err)
	}
	out := make(map[string]*Group, len(gf.Groups))
	for _, gs := range gf.Groups {
		if err := checkGroupState(gs.Name, out); err != nil {
			return nil, err
		}
		g := &Group{Name: gs.Name, topics: make(map[string]*groupTopic, len(gs.Topics))}
		for _, ts := range gs.Topics {
			if err := checkGroupTopicState(gs.Name, ts.Topic, ts.Generation, ts.Members); err != nil {
				return nil, err
			}
			g.topics[ts.Topic] = newGroupTopic(ts.Generation, ts.Members, ts.Offsets)
		}
		out[gs.Name] = g
	}
	return out, nil
}

// groupsFileV1 is the Slice 4–6 envelope, kept only so loadGroups can migrate
// it forward: members keep their join order, generation starts at 1, and the
// static size is discarded — assignment now derives from the live member list.
type groupsFileV1 struct {
	Version int            `json:"version"`
	Groups  []groupStateV1 `json:"groups"`
}

type groupStateV1 struct {
	Name   string              `json:"name"`
	Topics []groupTopicStateV1 `json:"topics"`
}

type groupTopicStateV1 struct {
	Topic   string         `json:"topic"`
	Size    int            `json:"size"`
	Members []string       `json:"members"`
	Offsets map[int]Offset `json:"offsets"`
}

func parseGroupsV1(data []byte) (map[string]*Group, error) {
	var gf groupsFileV1
	if err := json.Unmarshal(data, &gf); err != nil {
		return nil, fmt.Errorf("parse groups v1: %w", err)
	}
	out := make(map[string]*Group, len(gf.Groups))
	for _, gs := range gf.Groups {
		if err := checkGroupState(gs.Name, out); err != nil {
			return nil, err
		}
		g := &Group{Name: gs.Name, topics: make(map[string]*groupTopic, len(gs.Topics))}
		for _, ts := range gs.Topics {
			if err := checkGroupTopicState(gs.Name, ts.Topic, 1, ts.Members); err != nil {
				return nil, err
			}
			g.topics[ts.Topic] = newGroupTopic(1, ts.Members, ts.Offsets)
		}
		out[gs.Name] = g
	}
	return out, nil
}

func checkGroupState(name string, seen map[string]*Group) error {
	if !topicName.MatchString(name) {
		return fmt.Errorf("groups: invalid group name %q", name)
	}
	if _, dup := seen[name]; dup {
		return fmt.Errorf("groups: duplicate group %q", name)
	}
	return nil
}

// checkGroupTopicState rejects bindings that would corrupt the assignment
// math: generation must be positive and member ids non-empty and unique
// (a duplicate member would own the same partitions twice).
func checkGroupTopicState(group, topic string, generation int, members []string) error {
	if generation < 1 {
		return fmt.Errorf("groups: %s/%s generation %d", group, topic, generation)
	}
	for i, m := range members {
		if m == "" {
			return fmt.Errorf("groups: %s/%s empty member id at index %d", group, topic, i)
		}
		if slices.Contains(members[:i], m) {
			return fmt.Errorf("groups: %s/%s duplicate member %q", group, topic, m)
		}
	}
	return nil
}

func newGroupTopic(generation int, members []string, offsets map[int]Offset) *groupTopic {
	if members == nil {
		members = []string{}
	}
	if offsets == nil {
		offsets = map[int]Offset{}
	}
	return &groupTopic{
		Members: members, Generation: generation, Offsets: offsets,
		LastSeen: map[string]time.Time{},
	}
}

// saveGroupsLocked persists the full group state with the same atomic protocol
// as the topic catalog: tmp file → sync → rename → sync the meta directory.
// LastSeen is deliberately NOT persisted — sessions are ephemeral; Open grants
// every loaded member a fresh session-timeout grace period.
func (b *Broker) saveGroupsLocked() error {
	gf := groupsFile{Version: groupsVersion, Groups: make([]groupState, 0, len(b.groups))}
	for _, g := range b.groups {
		gs := groupState{Name: g.Name, Topics: make([]groupTopicState, 0, len(g.topics))}
		for topic, gt := range g.topics {
			gs.Topics = append(gs.Topics, groupTopicState{
				Topic: topic, Generation: gt.Generation, Members: gt.Members, Offsets: gt.Offsets,
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
