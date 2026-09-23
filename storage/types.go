// Package storage implements the marchiq log format. Slice 1 implements
// topic creation, partition append, and offset scans per SKETCH.md.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrClosed            = errors.New("broker or partition is closed")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	// ErrPartitionFenced rejects operations on a partition whose earlier
	// write/sync failed; appending after a partial frame would corrupt offsets.
	ErrPartitionFenced = errors.New("partition fenced after failure")
	// ErrResultUncertain means a write/sync failed after the record may have
	// become durable; the partition is fenced and a retry may duplicate.
	ErrResultUncertain = errors.New("operation result uncertain")
)

// Offset is local to a partition, not a topic or the whole broker.
type Offset int64

// Offsets describes the half-open readable range [Earliest, Latest).
// An empty Slice 1 partition has Earliest == Latest == 0.
type Offsets struct {
	Earliest Offset `json:"earliest"`
	Latest   Offset `json:"latest"` // LEO: next offset to assign, not last record.
}

// Record gets its timestamp from the broker and its offset from a sequential
// scan: segment base offset + record ordinal. Offset is NOT encoded on disk.
// Nil and empty Key/Value are equivalent in v1 (no tombstone semantics).
type Record struct {
	Offset      Offset
	TimestampNS int64
	Key         []byte
	Value       []byte
}

type TopicConfig struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	// Retention options are intentionally deferred until Slice 5.
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`)

func (c TopicConfig) Validate() error {
	if !topicName.MatchString(c.Name) {
		return fmt.Errorf("invalid topic name %q", c.Name)
	}
	if c.Partitions < 1 || c.Partitions > 1024 {
		return fmt.Errorf("partitions must be in [1, 1024]")
	}
	return nil
}

// TopicsMetadata is the versioned envelope for meta/topics.json.
// The slice is written in name order for human-readable, stable diffs.
type TopicsMetadata struct {
	Version int           `json:"version"`
	Topics  []TopicConfig `json:"topics"`
}

// BrokerAPI is the Slice 1 storage surface. It requires explicit partitions
// and always syncs before acknowledging. HTTP, routing, acks=0 and consumer
// groups do not belong in the storage layer.
type BrokerAPI interface {
	CreateTopic(TopicConfig) error
	ListTopics() []TopicConfig
	Produce(topic string, partition int, key, value []byte) (Record, error)
	GetOffsets(topic string, partition int) (Offsets, error)
	Close() error
}

// Ownership graph. One process owns a dataDir; these mutexes do NOT protect
// against another broker process.
type Broker struct {
	mu      sync.RWMutex // topic catalog; partition locks serialize appends
	dataDir string
	topics  map[string]*Topic
	closed  bool
}

type Topic struct {
	config     TopicConfig
	partitions []*PartitionLog
}

type PartitionLog struct {
	mu         sync.RWMutex // covers offset assignment, write, sync, publication
	dir        string
	nextOffset Offset // reconstructed by scanning log; never an independent truth
	active     *Segment
	failed     error // write/sync failure fences further operations until reopen
	closed     bool
}

// syncFile is the segment file surface. *os.File satisfies it; tests wrap it
// to inject short writes and sync failures.
type syncFile interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

type Segment struct {
	baseOffset Offset
	file       syncFile // O_CREATE|O_RDWR|O_APPEND; reads use ReadAt/SectionReader
	sizeBytes  int64    // successfully published byte boundary
	records    int64    // nextOffset = baseOffset + records for this single segment
}

func segmentName(base Offset) string { return fmt.Sprintf("%020d.log", base) }

// InitDirs is the only implemented broker-level operation in the sketch.
// It creates no catalog or log files and never rewrites existing data.
func InitDirs(dataDir string) error {
	for _, name := range []string{"meta", "topics"} {
		if err := os.MkdirAll(filepath.Join(dataDir, name), 0o755); err != nil {
			return fmt.Errorf("create %s directory: %w", name, err)
		}
	}
	return nil
}
