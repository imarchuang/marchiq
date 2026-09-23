package storage

import (
	"errors"
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
	r0, err := b.Produce("events", 0, []byte("k"), []byte("a"))
	if err != nil || r0.Offset != 0 {
		t.Fatalf("%+v %v", r0, err)
	}
	r1, err := b.Produce("events", 1, nil, []byte("b"))
	if err != nil || r1.Offset != 0 {
		t.Fatalf("%+v %v", r1, err)
	}
	if _, err := b.Produce("events", 2, nil, nil); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.Produce("ghost", 0, nil, nil); !errors.Is(err, ErrTopicNotFound) {
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
		if _, err := b.Produce("events", 0, nil, []byte{byte(i)}); err != nil {
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
	r, err := b2.Produce("events", 0, nil, []byte("next"))
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
	if _, err := b.Produce("events", 0, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "more", Partitions: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := b.Close(); err != nil { // idempotent
		t.Fatal(err)
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
