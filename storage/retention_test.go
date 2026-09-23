package storage

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newRetentionBroker opens a broker with 100-byte segments (22-byte frames =>
// exactly 4 records per segment), creates the topic, and produces n records
// to partition 0.
func newRetentionBroker(t *testing.T, cfg TopicConfig, n int) (*Broker, string) {
	t.Helper()
	dir := t.TempDir()
	b := openTestBroker(t, dir)
	b.maxSegmentBytes = 100
	if err := b.CreateTopic(cfg); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, cfg.Name, 0, n)
	return b, dir
}

// ageAllSegments backdates every .log file of the partition — including the
// active one — so age-based retention triggers without any sleeping.
func ageAllSegments(t *testing.T, b *Broker, topic string, partition int, age time.Duration) {
	t.Helper()
	p, err := b.getPartition(topic, partition)
	if err != nil {
		t.Fatal(err)
	}
	segs, err := p.DescribeSegments()
	if err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-age)
	for _, s := range segs {
		if err := os.Chtimes(filepath.Join(p.dir, segmentName(s.BaseOffset)), past, past); err != nil {
			t.Fatal(err)
		}
	}
}

// PLAN acceptance, and the no-group case: with no group joined there is no
// clamp, so age-based retention deletes every expired sealed segment; earliest
// advances to the first survivor, records before earliest are gone, records
// after survive, and the active segment is kept even though it is just as old.
func TestRetentionAgeAdvancesEarliest(t *testing.T) {
	b, dir := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 10)
	// 10 records => segments [0..3] [4..7] [8..9 active]
	segs, err := b.DescribeSegments("events", 0)
	if err != nil || len(segs) != 3 {
		t.Fatalf("%+v %v", segs, err)
	}
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 2 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 8, Latest: 10}) {
		t.Fatalf("%+v %v", off, err)
	}
	recs, _, err := b.Fetch("events", 0, 8, 100, 1<<20)
	if err != nil || len(recs) != 2 || recs[0].Offset != 8 || recs[1].Offset != 9 {
		t.Fatalf("%+v %v", recs, err)
	}
	for _, base := range []Offset{0, 4} { // both .log and .index unlinked
		for _, name := range []string{segmentName(base), indexName(base)} {
			if _, err := os.Stat(filepath.Join(dir, "topics", "events", "0", name)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("%s still on disk: %v", name, err)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "topics", "events", "0", segmentName(8))); err != nil {
		t.Fatal("active segment deleted:", err)
	}
	// A second pass converges instead of erroring on the files it already removed.
	deleted, err = b.EnforceRetention()
	if err != nil || deleted != 0 {
		t.Fatalf("second pass deleted=%d err=%v", deleted, err)
	}
}

// The clamp: segments entirely below committed+1 are deleted even though every
// segment is equally old; the segment containing the group's next read
// (committed+1) survives, and the group keeps reading without a gap.
func TestRetentionGroupClamp(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 20)
	// segments: [0..3] [4..7] [8..11] [12..15] [16..19 active]
	if _, err := b.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset("g1", "events", 0, 9); err != nil { // clamp B = 10
		t.Fatal(err)
	}
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 2 { // only [0..3] and [4..7] end at or below 10
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	segs, err := b.DescribeSegments("events", 0)
	if err != nil || len(segs) != 3 || segs[0].BaseOffset != 8 {
		t.Fatalf("%+v %v", segs, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off.Earliest != 8 {
		t.Fatalf("%+v %v", off, err)
	}
	parts := fetchOffsets(t, b, "g1", "events", "")
	if len(parts[0].Records) != 10 || parts[0].Records[0].Offset != 10 || parts[0].NextOffset != 20 {
		t.Fatalf("%+v", parts[0])
	}
}

// The clamp is the MIN over joined groups: the slowest consumer pins retention.
func TestRetentionClampIsMinAcrossGroups(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 20)
	if _, err := b.JoinGroup("fast", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.JoinGroup("slow", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset("fast", "events", 0, 19); err != nil { // B = 20
		t.Fatal(err)
	}
	if err := b.CommitOffset("slow", "events", 0, 4); err != nil { // B = 5 wins
		t.Fatal(err)
	}
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 1 { // only [0..3] is entirely below 5
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off.Earliest != 4 {
		t.Fatalf("%+v %v", off, err)
	}
}

// A group that joined but never committed contributes clamp 0: its next read
// is offset 0, so nothing on that partition may be deleted.
func TestRetentionUncommittedGroupBlocksAll(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 10)
	if _, err := b.JoinGroup("g1", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 0 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 0, Latest: 10}) {
		t.Fatalf("%+v %v", off, err)
	}
}

// Size-based retention deletes oldest-first until the partition is back under
// budget; no group is needed.
func TestRetentionBytesDeletesOldest(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionBytes: 200}, 20)
	// 4 sealed segments x 88B + full active = 440B > 200B budget
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 3 { // 440 - 3*88 = 176 <= 200
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	segs, err := b.DescribeSegments("events", 0)
	if err != nil || len(segs) != 2 || segs[0].BaseOffset != 12 {
		t.Fatalf("%+v %v", segs, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 12, Latest: 20}) {
		t.Fatalf("%+v %v", off, err)
	}
}

// The active segment is never deleted, even if it alone exceeds the budget.
func TestRetentionBytesKeepsActiveSegment(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionBytes: 1}, 21)
	// 1-byte budget: all 5 sealed segments go; the active one (1 record, 22B
	// over budget by itself) stays.
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 5 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	off, err := b.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 20, Latest: 21}) {
		t.Fatalf("%+v %v", off, err)
	}
	recs, _, err := b.Fetch("events", 0, 20, 10, 1<<20)
	if err != nil || len(recs) != 1 || recs[0].Offset != 20 {
		t.Fatalf("%+v %v", recs, err)
	}
}

// Restart after deletion: the segments on disk are the source of truth, so
// earliest persists. A brand-new group (joined after the deletion, no
// commits) starts at earliest — not 0, not an error.
func TestRetentionRestartPreservesEarliest(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b1.maxSegmentBytes = 100
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10)
	ageAllSegments(t, b1, "events", 0, 2*time.Minute)
	if deleted, err := b1.EnforceRetention(); err != nil || deleted != 2 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	off, err := b2.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 8, Latest: 10}) {
		t.Fatalf("after restart %+v %v", off, err)
	}
	if _, err := b2.JoinGroup("new", "events", "", 1); err != nil {
		t.Fatal(err)
	}
	parts := fetchOffsets(t, b2, "new", "events", "")
	if len(parts) != 1 || len(parts[0].Records) != 2 || parts[0].Records[0].Offset != 8 || parts[0].NextOffset != 10 {
		t.Fatalf("%+v", parts)
	}
}

// Explicit fetch below earliest is out of range (Kafka behavior), not a
// silent clamp: an explicit-offset reader must see that the data is gone.
func TestFetchBelowEarliestOutOfRange(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 10)
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	if _, err := b.EnforceRetention(); err != nil {
		t.Fatal(err)
	} // earliest is now 8
	for _, bad := range []Offset{0, 7} {
		if _, _, err := b.Fetch("events", 0, bad, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("offset %d: err=%v", bad, err)
		}
	}
	recs, _, err := b.Fetch("events", 0, 8, 10, 1<<20)
	if err != nil || len(recs) != 2 {
		t.Fatalf("%+v %v", recs, err)
	}
}

// Retention config persists in catalog v1, and catalogs written before Slice 5
// (no retention keys) keep loading with zero = unlimited.
func TestRetentionConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "old", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "new", Partitions: 1, RetentionMS: 1000, RetentionBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "topics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Version int              `json:"version"`
		Topics  []map[string]any `json:"topics"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Version != 1 || len(meta.Topics) != 2 {
		t.Fatalf("%s", data)
	}
	byName := map[string]map[string]any{}
	for _, tc := range meta.Topics {
		byName[tc["name"].(string)] = tc
	}
	if _, ok := byName["old"]["retention_ms"]; ok {
		t.Fatalf("zero retention written explicitly (old-format drift): %s", data)
	}
	if byName["new"]["retention_ms"] != float64(1000) || byName["new"]["retention_bytes"] != float64(4096) {
		t.Fatalf("%s", data)
	}
	b2 := openTestBroker(t, dir)
	got := map[string]TopicConfig{}
	for _, c := range b2.ListTopics() {
		got[c.Name] = c
	}
	if got["old"].RetentionMS != 0 || got["old"].RetentionBytes != 0 {
		t.Fatalf("%+v", got["old"])
	}
	if got["new"].RetentionMS != 1000 || got["new"].RetentionBytes != 4096 {
		t.Fatalf("%+v", got["new"])
	}
}

func TestRetentionNegativeRejected(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	for _, cfg := range []TopicConfig{
		{Name: "negms", Partitions: 1, RetentionMS: -1},
		{Name: "negbytes", Partitions: 1, RetentionBytes: -1},
	} {
		if err := b.CreateTopic(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
}

// The background janitor really runs: with a shortened interval it enforces
// retention without the test ever calling EnforceRetention.
func TestRetentionTickerRuns(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 10)
	b.SetRetentionInterval(10 * time.Millisecond)
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deadline := time.Now().Add(5 * time.Second)
	for {
		off, err := b.GetOffsets("events", 0)
		if err != nil {
			t.Fatal(err)
		}
		if off.Earliest == 8 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("janitor never ran: %+v", off)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
