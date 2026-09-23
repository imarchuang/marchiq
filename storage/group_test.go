package storage

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func produceN(t *testing.T, b *Broker, topic string, partition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := b.Produce(topic, partition, nil, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func fetchOffsets(t *testing.T, b *Broker, group, topic, member string) []PartitionFetch {
	t.Helper()
	parts, err := b.FetchGroup(group, topic, member, 1000, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

// Static assignment: partition p goes to the member whose ordinal == p % size.
// Assignments are disjoint, cover every partition, and rejoining is idempotent.
func TestJoinGroupStaticAssignment(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	a, err := b.JoinGroup("g1", "events", "a", 2)
	if err != nil || !slices.Equal(a.Partitions, []int{0, 2}) {
		t.Fatalf("%+v %v", a, err)
	}
	bb, err := b.JoinGroup("g1", "events", "b", 2)
	if err != nil || !slices.Equal(bb.Partitions, []int{1, 3}) {
		t.Fatalf("%+v %v", bb, err)
	}
	again, err := b.JoinGroup("g1", "events", "a", 2) // idempotent rejoin
	if err != nil || !slices.Equal(again.Partitions, []int{0, 2}) {
		t.Fatalf("%+v %v", again, err)
	}
	if _, err := b.JoinGroup("g1", "events", "c", 2); !errors.Is(err, ErrGroupConflict) { // full
		t.Fatalf("err=%v", err)
	}
	if _, err := b.JoinGroup("g1", "events", "a", 3); !errors.Is(err, ErrGroupConflict) { // size mismatch
		t.Fatalf("err=%v", err)
	}
	if _, err := b.JoinGroup("g1", "ghost", "a", 1); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.JoinGroup("g1", "events", "a", 0); err == nil {
		t.Fatal("size 0 must fail")
	}
}

// Core acceptance: fetch from committed+1; committing the last processed
// offset makes the next fetch resume after it; rewinding replays.
func TestGroupFetchCommitResume(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 10)
	if _, err := b.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	parts := fetchOffsets(t, b, "g1", "events", "")
	if len(parts) != 1 || len(parts[0].Records) != 10 || parts[0].NextOffset != 10 || parts[0].HighWatermark != 10 {
		t.Fatalf("%+v", parts)
	}
	if parts[0].Records[0].Offset != 0 || parts[0].Records[9].Offset != 9 {
		t.Fatalf("%+v", parts[0].Records)
	}
	if err := b.CommitOffset("g1", "events", 0, 9); err != nil {
		t.Fatal(err)
	}
	parts = fetchOffsets(t, b, "g1", "events", "")
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 {
		t.Fatalf("%+v", parts[0])
	}
	if err := b.CommitOffset("g1", "events", 0, 4); err != nil { // rewind -> replay 5..9
		t.Fatal(err)
	}
	parts = fetchOffsets(t, b, "g1", "events", "")
	if len(parts[0].Records) != 5 || parts[0].Records[0].Offset != 5 || parts[0].NextOffset != 10 {
		t.Fatalf("%+v", parts[0])
	}
}

// PLAN acceptance: consume 10, commit 9, restart, next fetch starts at 10.
// Membership and committed offsets survive the restart via meta/groups.json.
func TestGroupRestartNoReplay(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10)
	if _, err := b1.JoinGroup("g1", "events", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if got := fetchOffsets(t, b1, "g1", "events", "m1"); len(got[0].Records) != 10 {
		t.Fatalf("%+v", got[0])
	}
	if err := b1.CommitOffset("g1", "events", 0, 9); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "groups.json"))
	if err != nil || !strings.Contains(string(data), `"version": 1`) || !strings.Contains(string(data), `"m1"`) {
		t.Fatalf("groups.json: %q %v", data, err)
	}
	b2 := openTestBroker(t, dir)
	parts := fetchOffsets(t, b2, "g1", "events", "m1") // no re-join needed
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 {
		t.Fatalf("replayed after restart: %+v", parts[0])
	}
}

func TestCommitValidation(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 3)
	if err := b.CommitOffset("ghost", "events", 0, 0); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset("g1", "ghost", 0, 0); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, 3); !errors.Is(err, ErrOffsetOutOfRange) { // == LEO
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, -1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 1, 0); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("err=%v", err)
	}
}

// With several members the caller must name one; each member only ever sees
// its own partitions, and one member's commit never moves another's cursor.
func TestFetchGroupMemberResolution(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.JoinGroup("g1", "events", "m1", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := b.JoinGroup("g1", "events", "m2", 2); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 3)
	produceN(t, b, "events", 1, 5)
	if _, err := b.FetchGroup("g1", "events", "", 1000, 1<<20); !errors.Is(err, ErrMemberRequired) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.FetchGroup("g1", "events", "ghost", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.FetchGroup("ghost", "events", "m1", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	p1 := fetchOffsets(t, b, "g1", "events", "m1")
	if len(p1) != 1 || p1[0].Partition != 0 || len(p1[0].Records) != 3 {
		t.Fatalf("%+v", p1)
	}
	p2 := fetchOffsets(t, b, "g1", "events", "m2")
	if len(p2) != 1 || p2[0].Partition != 1 || len(p2[0].Records) != 5 {
		t.Fatalf("%+v", p2)
	}
	if err := b.CommitOffset("g1", "events", 0, 2); err != nil {
		t.Fatal(err)
	}
	if got := fetchOffsets(t, b, "g1", "events", "m1"); len(got[0].Records) != 0 {
		t.Fatalf("%+v", got[0])
	}
	if got := fetchOffsets(t, b, "g1", "events", "m2"); len(got[0].Records) != 5 { // unaffected
		t.Fatalf("%+v", got[0])
	}
}

// A group that joined one topic has no cursor for another; an empty poll
// returns records: [] with next_offset pinned at the high watermark.
func TestFetchGroupEmptyAndUnknownTopic(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "other", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.FetchGroup("g1", "other", "", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	parts := fetchOffsets(t, b, "g1", "events", "")
	if len(parts) != 1 || parts[0].Records == nil || len(parts[0].Records) != 0 || parts[0].NextOffset != 0 {
		t.Fatalf("%+v", parts)
	}
}
