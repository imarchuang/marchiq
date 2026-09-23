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
	DescribeSegments(topic string, partition int) ([]SegmentInfo, error)
	Close() error
}

const (
	// DefaultMaxSegmentBytes rolls the active segment when the next frame
	// would exceed this size. Tests use small values to exercise roll.
	DefaultMaxSegmentBytes int64 = 16 << 20 // 16 MiB
	// DefaultIndexIntervalBytes adds one sparse index entry per this many
	// bytes of log data written (Kafka-style relative-offset index).
	DefaultIndexIntervalBytes int64 = 4096 // 4 KiB
)

// Ownership graph. One process owns a dataDir; these mutexes do NOT protect
// against another broker process.
type Broker struct {
	mu      sync.RWMutex // topic catalog; partition locks serialize appends
	dataDir string
	topics  map[string]*Topic
	closed  bool

	maxSegmentBytes    int64 // roll threshold, from Open defaults
	indexIntervalBytes int64 // sparse index density
}

type Topic struct {
	config     TopicConfig
	partitions []*PartitionLog
}

type PartitionLog struct {
	mu         sync.RWMutex // covers offset assignment, roll, write, sync, publication
	dir        string
	nextOffset Offset // reconstructed by scanning logs; never an independent truth
	segments   []*Segment
	failed     error // write/sync failure fences further operations until reopen
	closed     bool

	maxSegmentBytes    int64
	indexIntervalBytes int64
}

// active returns the segment currently being appended to (always the last).
func (p *PartitionLog) active() *Segment { return p.segments[len(p.segments)-1] }

// syncFile is the segment file surface. *os.File satisfies it; tests wrap it
// to inject short writes and sync failures. Truncate is used only for active
// segment torn-tail repair at open.
type syncFile interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// Segment is one immutable-once-sealed log file plus its sparse offset index.
// Sealed segments are never written again; only the active segment appends.
type Segment struct {
	baseOffset Offset
	file       syncFile // O_CREATE|O_RDWR|O_APPEND; reads use ReadAt/SectionReader
	indexFile  syncFile // sparse index, rewritten atomically at open
	sizeBytes  int64    // successfully published byte boundary
	records    int64    // nextOffset = last segment base + its records

	index          []indexEntry // in-memory sparse index, rebuilt by scanning at open
	lastIndexedPos int64        // file position of the last indexed record
}

// SegmentInfo is the read-only view of a segment for debugging/observability.
type SegmentInfo struct {
	BaseOffset Offset `json:"base_offset"`
	SizeBytes  int64  `json:"size_bytes"`
	Records    int64  `json:"records"`
}

func segmentName(base Offset) string { return fmt.Sprintf("%020d.log", base) }
func indexName(base Offset) string   { return fmt.Sprintf("%020d.index", base) }

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
