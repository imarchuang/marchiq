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
		if _, _, err := b.Produce(topic, partition, nil, []byte{byte(i)}, AcksLeader); err != nil {
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

// --- Slice 6: lag reporting + the acks=0 crash-rewind interaction ---

// GroupLag: lag = latest - (committed+1); a partition the group never
// committed on reports committed=nil and lag = latest. Members are listed
// for demo readability; an unknown group filter is ErrGroupNotFound.
func TestGroupLag(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 10)
	produceN(t, b, "events", 1, 4)
	if _, err := b.JoinGroup("g1", "events", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset("g1", "events", 0, 6); err != nil {
		t.Fatal(err)
	}
	lags, err := b.GroupLag("")
	if err != nil || len(lags) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
	gl := lags[0]
	if gl.Group != "g1" || gl.Topic != "events" || !slices.Equal(gl.Members, []string{"m1"}) {
		t.Fatalf("%+v", gl)
	}
	if len(gl.Partitions) != 2 {
		t.Fatalf("%+v", gl.Partitions)
	}
	if p0 := gl.Partitions[0]; p0.Committed == nil || *p0.Committed != 6 ||
		p0.NextFetch != 7 || p0.Latest != 10 || p0.Lag != 3 {
		t.Fatalf("%+v", p0)
	}
	if p1 := gl.Partitions[1]; p1.Committed != nil || p1.NextFetch != 0 ||
		p1.Latest != 4 || p1.Lag != 4 {
		t.Fatalf("%+v", p1)
	}
	if _, err := b.GroupLag("ghost"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if lags, err = b.GroupLag("g1"); err != nil || len(lags) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
}

// acks=0 records a group already committed can vanish in an OS crash,
// rewinding LEO BELOW committed+1 — simulated by truncating the log to a
// frame boundary while the broker is down (what losing unflushed page-cache
// pages looks like on disk). FetchGroup clamps to an empty poll at the new
// LEO: no error, and the committed offset is never rewritten. The clamp is
// a safety valve, not a recovery: the cursor stays authoritative, so the
// replacements later written at the lost offsets are NOT re-delivered (the
// group already committed their lost predecessors); delivery resumes at the
// first offset past the cursor. Negative lag is the visible signature of
// the rewind while it lasts (DURABILITY.md).
func TestFetchGroupLEORewindBelowCommitted(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10) // 22-byte frames: nil key, 1-byte value
	if _, err := b1.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if got := fetchOffsets(t, b1, "g1", "events", ""); len(got[0].Records) != 10 {
		t.Fatalf("%+v", got[0])
	}
	if err := b1.CommitOffset("g1", "events", 0, 9); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	// "OS crash": the last 5 frames never reached the disk.
	logPath := filepath.Join(dir, "topics", "events", "0", segmentName(0))
	if err := os.Truncate(logPath, 5*22); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	if off, err := b2.GetOffsets("events", 0); err != nil || off.Latest != 5 {
		t.Fatalf("%+v %v", off, err)
	}
	// Empty poll at the rewound LEO — not an error, not a replay of 6..9.
	parts := fetchOffsets(t, b2, "g1", "events", "")
	if len(parts) != 1 || len(parts[0].Records) != 0 ||
		parts[0].NextOffset != 5 || parts[0].HighWatermark != 5 {
		t.Fatalf("%+v", parts)
	}
	lags, err := b2.GroupLag("g1")
	if err != nil || len(lags) != 1 || len(lags[0].Partitions) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
	if pl := lags[0].Partitions[0]; pl.Committed == nil || *pl.Committed != 9 ||
		pl.NextFetch != 10 || pl.Latest != 5 || pl.Lag != -5 {
		t.Fatalf("%+v", pl)
	}
	// Refill: new records reuse offsets 5..9, but the group's cursor is
	// already at 10 — it is NOT re-shown the replacements (its commit says
	// "processed through 9"). The poll stays empty until LEO passes 10.
	produceN(t, b2, "events", 0, 5)
	parts = fetchOffsets(t, b2, "g1", "events", "")
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 || parts[0].HighWatermark != 10 {
		t.Fatalf("%+v", parts[0])
	}
	produceN(t, b2, "events", 0, 1) // offset 10: the first genuinely new record
	parts = fetchOffsets(t, b2, "g1", "events", "")
	if len(parts[0].Records) != 1 || parts[0].Records[0].Offset != 10 || parts[0].NextOffset != 11 {
		t.Fatalf("%+v", parts[0])
	}
	// The committed offset was never rewritten, and committing past the new
	// LEO still fails closed.
	if err := b2.CommitOffset("g1", "events", 0, 12); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
}
