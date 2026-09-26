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
	"sync/atomic"
	"time"
)

var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrClosed            = errors.New("broker or partition is closed")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	// ErrGroupNotFound: the group (or its topic binding, or the member in its
	// live list) does not exist; join first.
	ErrGroupNotFound = errors.New("consumer group not found")
	// ErrGenerationFence: the request carries a stale group generation — a
	// membership change (join/leave/timeout-evict) happened since the caller
	// learned it. Fencing guards the offset WRITE path (commits) and is how
	// heartbeats tell a client to resync (docs/kafka-notes.md §3).
	ErrGenerationFence = errors.New("generation fenced by a newer group generation")
	// ErrMemberRequired: the group has several members; the caller must name one.
	ErrMemberRequired = errors.New("member param required")
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
	// Retention (Slice 5): zero means unlimited. omitempty keeps catalogs
	// written before Slice 5 byte-compatible in both directions.
	RetentionMS    int64 `json:"retention_ms,omitempty"`
	RetentionBytes int64 `json:"retention_bytes,omitempty"`
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`)

func (c TopicConfig) Validate() error {
	if !topicName.MatchString(c.Name) {
		return fmt.Errorf("invalid topic name %q", c.Name)
	}
	if c.Partitions < 1 || c.Partitions > 1024 {
		return fmt.Errorf("partitions must be in [1, 1024]")
	}
	if c.RetentionMS < 0 || c.RetentionBytes < 0 {
		return fmt.Errorf("retention must be >= 0")
	}
	return nil
}

// TopicsMetadata is the versioned envelope for meta/topics.json.
// The slice is written in name order for human-readable, stable diffs.
type TopicsMetadata struct {
	Version int           `json:"version"`
	Topics  []TopicConfig `json:"topics"`
}

// Acks selects how much durability work a produce waits for before
// returning. The values match the HTTP wire form (acks=0|1).
type Acks int

const (
	// AcksNone returns as soon as the frame reaches the page cache: no
	// fsync. An OS crash can lose recent records and rewind LEO — the
	// interaction with committed offsets is documented in DURABILITY.md.
	AcksNone Acks = 0
	// AcksLeader (the default) fsyncs segment and index before returning.
	AcksLeader Acks = 1
)

// ParseAcks validates the wire form of the acks parameter: "0" or "1".
func ParseAcks(s string) (Acks, error) {
	switch s {
	case "0":
		return AcksNone, nil
	case "1":
		return AcksLeader, nil
	default:
		return AcksLeader, fmt.Errorf("unknown acks %q (want 0 or 1)", s)
	}
}

// Stats is the broker's process-lifetime produce/fetch counters. They are
// never persisted and reset at Open: they answer "what has THIS process
// moved since it started", not anything about the data on disk. Bytes are
// payload bytes (len(key)+len(value)), not on-disk frame bytes.
type Stats struct {
	ProduceRecords int64 `json:"produce_records"`
	ProduceBytes   int64 `json:"produce_bytes"`
	FetchRecords   int64 `json:"fetch_records"`
	FetchBytes     int64 `json:"fetch_bytes"`
}

// BrokerAPI is the storage surface exposed to the HTTP layer. Produce
// accepts partition -1 (broker-side partitioner) and an acks policy; group
// and debug reads live here too so the HTTP package stays a thin adapter.
type BrokerAPI interface {
	CreateTopic(TopicConfig) error
	ListTopics() []TopicConfig
	Produce(topic string, partition int, key, value []byte, acks Acks) (Record, int, error)
	GetOffsets(topic string, partition int) (Offsets, error)
	Fetch(topic string, partition int, offset Offset, maxRecords int, maxBytes int64) ([]Record, Offsets, error)
	DescribeSegments(topic string, partition int) ([]SegmentInfo, error)
	JoinGroup(group, topic, member string) (Assignment, error)
	Heartbeat(group, topic, member string, generation int) (HeartbeatResult, error)
	LeaveGroup(group, topic, member string) error
	CommitOffset(group, topic string, partition int, offset Offset, generation int) error
	FetchGroup(group, topic, member string, maxRecords int, maxBytes int64) ([]PartitionFetch, error)
	GroupLag(group string) ([]GroupLag, error)
	Stats() Stats
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

	gmu    sync.RWMutex      // consumer groups: membership + committed offsets
	groups map[string]*Group // persisted to meta/groups.json

	maxSegmentBytes    int64 // roll threshold, from Open defaults
	indexIntervalBytes int64 // sparse index density

	// Retention janitor (Slice 5). retentionInterval is guarded by mu; the
	// loop re-reads it each tick so SetRetentionInterval takes effect without
	// restarting the broker. retentionKick is replaced+closed by the setter
	// so a pending wait re-times immediately instead of finishing the old
	// interval. retentionStop + retentionWg let Close stop the goroutine
	// before any segment file is closed.
	retentionInterval time.Duration
	retentionKick     chan struct{}
	retentionStop     chan struct{}
	retentionWg       sync.WaitGroup

	// Group sessions + reaper (Slice 7). sessionTimeout is how long a member
	// may go without a heartbeat before ReapExpiredMembers evicts it;
	// reapInterval is how often the background reaper runs one pass. Both are
	// guarded by mu and re-read by the loop each tick, so the setters take
	// effect without a restart; tests drive ReapExpiredMembers directly.
	sessionTimeout time.Duration
	reapInterval   time.Duration
	reapStop       chan struct{}
	reapWg         sync.WaitGroup

	// Slice 6 stats counters. Atomics, never persisted, reset at Open.
	produceRecords atomic.Int64
	produceBytes   atomic.Int64
	fetchRecords   atomic.Int64
	fetchBytes     atomic.Int64
}

type Topic struct {
	config     TopicConfig
	partitions []*PartitionLog
	// rr is the round-robin cursor for keyless produce with partition -1.
	// In-memory only: a restart re-deals from partition 0, which is fine —
	// round-robin promises spread, not continuity.
	rr atomic.Uint64
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
