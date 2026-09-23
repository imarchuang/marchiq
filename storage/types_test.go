package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTopicValidation(t *testing.T) {
	for _, name := range []string{"events", "a.b-c_1"} {
		if err := (TopicConfig{Name: name, Partitions: 2}).Validate(); err != nil { t.Fatal(err) }
	}
	for _, c := range []TopicConfig{
		{Name: "", Partitions: 1}, {Name: "..", Partitions: 1},
		{Name: "../events", Partitions: 1}, {Name: "a/b", Partitions: 1},
		{Name: strings.Repeat("a", 250), Partitions: 1},
		{Name: "events", Partitions: 0}, {Name: "events", Partitions: 1025},
	} {
		if err := c.Validate(); err == nil { t.Fatalf("accepted %+v", c) }
	}
}

func TestInitDirs(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2; i++ {
		if err := InitDirs(dir); err != nil { t.Fatal(err) }
	}
	for _, name := range []string{"meta", "topics"} {
		entries, err := os.ReadDir(filepath.Join(dir, name))
		if err != nil || len(entries) != 0 { t.Fatalf("%s: entries=%v err=%v", name, entries, err) }
	}
	if got := segmentName(0); got != "00000000000000000000.log" { t.Fatal(got) }
}
