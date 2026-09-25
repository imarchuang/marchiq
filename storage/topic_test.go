package storage

import (
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func openTestBroker(t *testing.T, dir string) *Broker {
	t.Helper()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// Acceptance 1+4: create N empty partitions, publish the catalog, and let two
// partitions each assign their own offset 0.
func TestCreateTopicAndProduce(t *testing.T) {
	dir := t.TempDir()
	b := openTestBroker(t, dir)
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "topics.json"))
	if err != nil || !strings.Contains(string(data), `"events"`) {
		t.Fatalf("catalog: %q %v", data, err)
	}
	for i := 0; i < 2; i++ {
		path := filepath.Join(dir, "topics", "events", strconv.Itoa(i), segmentName(0))
		if info, err := os.Stat(path); err != nil || info.Size() != 0 {
			t.Fatalf("%s: %v %v", path, info, err)
		}
	}
	r0, _, err := b.Produce("events", 0, []byte("k"), []byte("a"), AcksLeader)
	if err != nil || r0.Offset != 0 {
		t.Fatalf("%+v %v", r0, err)
	}
	r1, _, err := b.Produce("events", 1, nil, []byte("b"), AcksLeader)
	if err != nil || r1.Offset != 0 {
		t.Fatalf("%+v %v", r1, err)
	}
	if _, _, err := b.Produce("events", 2, nil, nil, AcksLeader); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, _, err := b.Produce("ghost", 0, nil, nil, AcksLeader); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if got := b.ListTopics(); len(got) != 1 || got[0].Name != "events" || got[0].Partitions != 2 {
		t.Fatalf("%+v", got)
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("err=%v", err)
	}
}

func TestCreateTopicInvalid(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	for _, cfg := range []TopicConfig{
		{Name: "bad/name", Partitions: 1},
		{Name: "events", Partitions: 0},
		{Name: "events", Partitions: 1025},
	} {
		if err := b.CreateTopic(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	if got := b.ListTopics(); len(got) != 0 {
		t.Fatalf("invalid creates leaked: %+v", got)
	}
}

// An interrupted create leaves an unregistered directory: never adopt or
// delete it silently — report a conflict and let a human clean up.
func TestCreateTopicLeftoverDirConflicts(t *testing.T) {
	dir := t.TempDir()
	b := openTestBroker(t, dir)
	if err := os.MkdirAll(filepath.Join(dir, "topics", "ghost", "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "ghost", Partitions: 1}); err == nil ||
		!strings.Contains(err.Error(), "manual cleanup") {
		t.Fatalf("err=%v", err)
	}
}

// Acceptance 5 (broker level): Close → Open continues offsets from the log.
func TestBrokerReopenContinues(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := b.Produce("events", 0, nil, []byte{byte(i)}, AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	off, err := b2.GetOffsets("events", 0)
	if err != nil || off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("%+v %v", off, err)
	}
	r, _, err := b2.Produce("events", 0, nil, []byte("next"), AcksLeader)
	if err != nil || r.Offset != 3 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestOpenCorruptCatalog(t *testing.T) {
	for name, content := range map[string]string{
		"not json":      `{`,
		"bad version":   `{"version": 2, "topics": []}`,
		"duplicate":     `{"version": 1, "topics": [{"name":"a","partitions":1},{"name":"a","partitions":1}]}`,
		"invalid topic": `{"version": 1, "topics": [{"name":"bad/name","partitions":1}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := InitDirs(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "meta", "topics.json"), []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir); err == nil {
				t.Fatal("opened a broker with a corrupt catalog")
			}
		})
	}
}

// A registered topic whose log is missing must fail the open, never silently
// recreate an empty log (that would fake "no data lost").
func TestOpenMissingSegmentFails(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "topics", "events", "1", segmentName(0))); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("opened a broker with a missing segment")
	}
}

func TestBrokerCloseFencesOperations(t *testing.T) {
	b, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Produce("events", 0, nil, nil, AcksLeader); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "more", Partitions: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := b.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
}

// Slice 2: broker-level roll and reopen across segments.
func TestBrokerRollAndReopen(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.maxSegmentBytes = 100
	b.indexIntervalBytes = 30
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, _, err := b.Produce("events", 0, nil, []byte("v"), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := b.DescribeSegments("events", 0)
	if err != nil || len(segs) < 2 {
		t.Fatalf("%+v %v", segs, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	off, err := b2.GetOffsets("events", 0)
	if err != nil || off.Latest != 10 {
		t.Fatalf("%+v %v", off, err)
	}
	segs2, err := b2.DescribeSegments("events", 0)
	if err != nil || len(segs2) != len(segs) {
		t.Fatalf("%+v vs %+v %v", segs2, segs, err)
	}
}

func TestListTopicsSorted(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	for _, name := range []string{"zeta", "alpha", "mid"} {
		if err := b.CreateTopic(TopicConfig{Name: name, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	got := b.ListTopics()
	if len(got) != 3 || got[0].Name != "alpha" || got[1].Name != "mid" || got[2].Name != "zeta" {
		t.Fatalf("%+v", got)
	}
}

// --- Slice 6: broker-side partitioner, acks=0, stats ---

// partition -1 lets the broker pick: a present key hashes to one partition
// deterministically (fnv32a(key) % N); keyless records round-robin evenly;
// an explicit partition always wins over the key's hash.
func TestProducePartitionMinusOne(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	sticky := -1
	for i := 0; i < 10; i++ {
		_, p, err := b.Produce("events", -1, []byte("sticky"), nil, AcksLeader)
		if err != nil {
			t.Fatal(err)
		}
		if sticky < 0 {
			sticky = p
		}
		if p != sticky {
			t.Fatalf("same key landed on %d then %d", sticky, p)
		}
	}
	// The placement is exactly fnv32a(key) % N (PLAN open decisions).
	h := fnv.New32a()
	_, _ = h.Write([]byte("sticky"))
	if want := int(h.Sum32() % 4); sticky != want {
		t.Fatalf("hash placement %d, want fnv32a%%4=%d", sticky, want)
	}
	if off, err := b.GetOffsets("events", sticky); err != nil || off.Latest != 10 {
		t.Fatalf("%+v %v", off, err)
	}
	// Keyless: exact round-robin, 5 per partition on top of what's there.
	for i := 0; i < 20; i++ {
		if _, _, err := b.Produce("events", -1, nil, []byte("v"), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	for p := 0; p < 4; p++ {
		want := Offset(5)
		if p == sticky {
			want = 15
		}
		if off, err := b.GetOffsets("events", p); err != nil || off.Latest != want {
			t.Fatalf("partition %d: %+v want LEO=%d, err=%v", p, off, want, err)
		}
	}
	// Explicit partition beats the key's hash.
	if _, p, err := b.Produce("events", 2, []byte("sticky"), nil, AcksLeader); err != nil || p != 2 {
		t.Fatalf("explicit partition lost to hash: p=%d err=%v", p, err)
	}
	// -1 on an unknown topic is still "not found", never a silent create.
	if _, _, err := b.Produce("ghost", -1, nil, nil, AcksLeader); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, _, err := b.Produce("events", -2, nil, nil, AcksLeader); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("err=%v", err)
	}
}

// acks=0 returns an assigned offset and the record is readable in the same
// process — only the fsync is skipped (durability, not visibility).
func TestProduceAcksNoneReadableInProcess(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	r, _, err := b.Produce("events", 0, nil, []byte("fast"), AcksNone)
	if err != nil || r.Offset != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, off, err := b.Fetch("events", 0, 0, 10, 1<<20)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "fast" || off.Latest != 1 {
		t.Fatalf("%+v %+v %v", recs, off, err)
	}
}

// Stats count records and payload bytes (key+value) in and out, and reset at
// Open: process-lifetime counters, never persisted.
func TestStatsCounters(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if s := b.Stats(); s != (Stats{}) {
		t.Fatalf("fresh broker stats %+v", s)
	}
	for i := 0; i < 3; i++ { // key "k" (1B) + value "vv" (2B) = 3 payload bytes
		if _, _, err := b.Produce("events", 0, []byte("k"), []byte("vv"), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	recs, _, err := b.Fetch("events", 0, 0, 2, 1<<20)
	if err != nil || len(recs) != 2 {
		t.Fatalf("%+v %v", recs, err)
	}
	if s := b.Stats(); s != (Stats{ProduceRecords: 3, ProduceBytes: 9, FetchRecords: 2, FetchBytes: 6}) {
		t.Fatalf("%+v", s)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	if s := b2.Stats(); s != (Stats{}) {
		t.Fatalf("stats survived restart: %+v", s)
	}
}
