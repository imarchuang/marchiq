package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const (
	testMaxSegmentBytes    = 1 << 20 // effectively "never roll" unless overridden
	testIndexIntervalBytes = 4096
)

// newTestPartition builds a partition whose log file is optionally wrapped
// for fault injection (the index file stays real).
func newTestPartition(t *testing.T, wrap func(syncFile) syncFile) *PartitionLog {
	t.Helper()
	dir := t.TempDir()
	logF, err := os.OpenFile(filepath.Join(dir, segmentName(0)), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	idxF, err := os.OpenFile(filepath.Join(dir, indexName(0)), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var sf syncFile = logF
	if wrap != nil {
		sf = wrap(sf)
	}
	p := &PartitionLog{
		dir:                dir,
		maxSegmentBytes:    testMaxSegmentBytes,
		indexIntervalBytes: testIndexIntervalBytes,
		segments:           []*Segment{{baseOffset: 0, file: sf, indexFile: idxF}},
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// createSegment materializes an empty base=0 log so openPartition can find it.
func createSegment(t *testing.T, dir string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, segmentName(0)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func openTestPartition(t *testing.T, dir string) *PartitionLog {
	t.Helper()
	p, err := openPartition(dir, testMaxSegmentBytes, testIndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// readAll reads without a byte limit (the common case for older tests).
func readAll(p *PartitionLog, offset Offset, maxRecords int) ([]Record, error) {
	return p.ReadFrom(offset, maxRecords, math.MaxInt64)
}

// Acceptance: append 3 records, LEO=3, records readable by offset.
func TestAppendAndReadBack(t *testing.T) {
	p := newTestPartition(t, nil)
	for i, v := range []string{"one", "two", "three"} {
		r, err := p.Append(nil, []byte(v), AcksLeader)
		if err != nil || r.Offset != Offset(i) {
			t.Fatalf("append %d: %+v %v", i, r, err)
		}
	}
	off, err := p.Offsets()
	if err != nil || off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("%+v %v", off, err)
	}
	recs, err := readAll(p, 1, 10)
	if err != nil || len(recs) != 2 || recs[0].Offset != 1 || string(recs[1].Value) != "three" {
		t.Fatalf("%+v %v", recs, err)
	}
	if recs, err = readAll(p, 3, 10); err != nil || len(recs) != 0 {
		t.Fatalf("at LEO: %+v %v", recs, err)
	}
	if _, err = readAll(p, -1, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatal(err)
	}
	if _, err = readAll(p, 4, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatal(err)
	}
	if _, err = readAll(p, 0, 0); err == nil {
		t.Fatal("maxRecords=0 accepted")
	}
	if recs, err = readAll(p, 0, 2); err != nil || len(recs) != 2 || recs[1].Offset != 1 {
		t.Fatalf("maxRecords: %+v %v", recs, err)
	}
}

// Concurrent appends get unique offsets and frames never interleave — every
// record decodes back at its own offset.
func TestAppendConcurrent(t *testing.T) {
	p := newTestPartition(t, nil)
	const n = 64
	offsets := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := p.Append(nil, []byte(fmt.Sprintf("v%d", i)), AcksLeader)
			if err != nil {
				t.Error(err)
				return
			}
			offsets[i] = int64(r.Offset)
		}(i)
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, o := range offsets {
		if o < 0 || o >= n || seen[o] {
			t.Fatalf("offset %d duplicate or out of range", o)
		}
		seen[o] = true
	}
	if off, _ := p.Offsets(); off.Latest != n {
		t.Fatalf("LEO=%d", off.Latest)
	}
	recs, err := readAll(p, 0, n)
	if err != nil || len(recs) != n {
		t.Fatalf("%d %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != Offset(i) {
			t.Fatalf("record %d decoded at offset %d", i, r.Offset)
		}
	}
}

// Reopen continues the offset sequence instead of restarting at 0.
func TestReopenContinuesOffset(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p := openTestPartition(t, dir)
	for i := 0; i < 3; i++ {
		if _, err := p.Append(nil, []byte{byte(i)}, AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := openTestPartition(t, dir)
	defer p2.Close()
	if off, _ := p2.Offsets(); off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("reopened %+v", off)
	}
	r, err := p2.Append(nil, []byte("after-reopen"), AcksLeader)
	if err != nil || r.Offset != 3 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, err := readAll(p2, 0, 10)
	if err != nil || len(recs) != 4 || string(recs[3].Value) != "after-reopen" {
		t.Fatalf("%+v %v", recs, err)
	}
}

type failSync struct{ syncFile }

func (failSync) Sync() error { return errors.New("injected sync failure") }

type shortWrite struct{ syncFile }

func (f shortWrite) Write(b []byte) (int, error) { return len(b) - 1, nil }

// Write/sync failures never ack success and fence the partition.
func TestSyncFailureFencesPartition(t *testing.T) {
	p := newTestPartition(t, func(f syncFile) syncFile { return failSync{f} })
	if _, err := p.Append(nil, []byte("x"), AcksLeader); !errors.Is(err, ErrResultUncertain) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("y"), AcksLeader); !errors.Is(err, ErrPartitionFenced) {
		t.Fatalf("err=%v", err)
	}
	if off, err := p.Offsets(); err != nil || off.Latest != 0 { // nothing published
		t.Fatalf("%+v %v", off, err)
	}
}

func TestShortWriteFencesPartition(t *testing.T) {
	p := newTestPartition(t, func(f syncFile) syncFile { return shortWrite{f} })
	if _, err := p.Append(nil, []byte("x"), AcksLeader); !errors.Is(err, ErrResultUncertain) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("y"), AcksLeader); !errors.Is(err, ErrPartitionFenced) {
		t.Fatalf("err=%v", err)
	}
}

// Slice 2: a torn tail on the ACTIVE segment is truncated to the last good
// boundary and synced; the partition keeps working and offsets continue.
func TestOpenPartitionTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p := openTestPartition(t, dir)
	for i := 0; i < 3; i++ {
		if _, err := p.Append(nil, []byte{byte(i)}, AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentName(0))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	goodSize := info.Size()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{recordMagic, 0, 0}); err != nil { // partial header = torn write
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := openTestPartition(t, dir) // must succeed now, not refuse
	defer p2.Close()
	if off, _ := p2.Offsets(); off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("after truncate %+v", off)
	}
	if info2, _ := os.Stat(path); info2.Size() != goodSize {
		t.Fatalf("truncated to %d, want %d", info2.Size(), goodSize)
	}
	r, err := p2.Append(nil, []byte("continues"), AcksLeader)
	if err != nil || r.Offset != 3 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, err := readAll(p2, 0, 10)
	if err != nil || len(recs) != 4 || string(recs[3].Value) != "continues" {
		t.Fatalf("%+v %v", recs, err)
	}
}

// A torn tail on a SEALED segment is corruption, not a crash artifact: the
// open must refuse rather than repair immutable data.
func TestOpenPartitionSealedTornTailRefuses(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p, err := openPartition(dir, 60, testIndexIntervalBytes) // ~2 frames per segment
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ { // forces at least one roll
		if _, err := p.Append(nil, []byte(fmt.Sprintf("value-%d", i)), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.segments) < 2 {
		t.Fatalf("expected roll, got %d segments", len(p.segments))
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	sealed := filepath.Join(dir, segmentName(0))
	f, err := os.OpenFile(sealed, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{recordMagic, 0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openPartition(dir, 60, testIndexIntervalBytes); err == nil {
		t.Fatal("opened a partition with a torn sealed segment")
	}
}

// Corrupt frames still refuse the open, even on the active segment.
func TestOpenPartitionCorruptFrameRefuses(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p := openTestPartition(t, dir)
	if _, err := p.Append(nil, []byte("good"), AcksLeader); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentName(0))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 0); err != nil { // corrupt the magic byte
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openPartition(dir, testMaxSegmentBytes, testIndexIntervalBytes); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenPartitionMissingLog(t *testing.T) {
	if _, err := openPartition(t.TempDir(), testMaxSegmentBytes, testIndexIntervalBytes); err == nil {
		t.Fatal("opened a partition with no segment file")
	}
}

// An oversized record is a caller error: it must NOT fence the partition.
func TestAppendTooLargeDoesNotFence(t *testing.T) {
	p := newTestPartition(t, nil)
	if _, err := p.Append(nil, make([]byte, MaxRecordBytes), AcksLeader); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("ok"), AcksLeader); err != nil {
		t.Fatalf("healthy partition fenced: %v", err)
	}
}

func TestCloseFencesOperations(t *testing.T) {
	p := newTestPartition(t, nil)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(nil, nil, AcksLeader); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Offsets(); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if _, err := readAll(p, 0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := p.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
}

// Slice 3: maxBytes caps the frame bytes returned, but at least one record is
// always delivered even when a single frame exceeds the budget.
func TestReadFromMaxBytes(t *testing.T) {
	p := newTestPartition(t, nil)
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, []byte("v"), AcksLeader); err != nil { // 23-byte frames
			t.Fatal(err)
		}
	}
	recs, err := p.ReadFrom(0, 100, 50) // 2 frames fit (46B), 3rd would exceed
	if err != nil || len(recs) != 2 || recs[1].Offset != 1 {
		t.Fatalf("%+v %v", recs, err)
	}
	recs, err = p.ReadFrom(0, 100, 1) // budget smaller than one frame: still one record
	if err != nil || len(recs) != 1 || recs[0].Offset != 0 {
		t.Fatalf("%+v %v", recs, err)
	}
	if _, err = p.ReadFrom(0, 100, 0); err == nil {
		t.Fatal("maxBytes=0 accepted")
	}
	// Byte limit across a segment boundary.
	p.maxSegmentBytes = 60 // ~2 frames per segment
	q := newTestPartition(t, nil)
	q.maxSegmentBytes = 60
	for i := 0; i < 6; i++ {
		if _, err := q.Append(nil, []byte("v"), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	recs, err = q.ReadFrom(0, 100, 70) // 3 frames (69B), spans into segment 2
	if err != nil || len(recs) != 3 || recs[2].Offset != 2 {
		t.Fatalf("%+v %v", recs, err)
	}
	if len(q.segments) < 2 {
		t.Fatal("expected the byte-limited read to span segments")
	}
}

// --- Slice 2: roll, sparse index, cross-segment reads ---

// Roll produces 000...000.log + 000...00N.log where N is the first offset of
// the new segment; the sealed segment stays within the size limit.
func TestSegmentRoll(t *testing.T) {
	p := newTestPartition(t, nil)
	p.maxSegmentBytes = 100 // ~23-byte frames => 4 per segment
	var last Offset
	for i := 0; i < 10; i++ {
		r, err := p.Append(nil, []byte("v"), AcksLeader)
		if err != nil {
			t.Fatal(err)
		}
		last = r.Offset
	}
	if last != 9 {
		t.Fatalf("last offset %d", last)
	}
	if len(p.segments) != 3 { // 4 + 4 + 2
		t.Fatalf("segments=%d", len(p.segments))
	}
	if p.segments[0].sizeBytes > 100 {
		t.Fatalf("sealed segment exceeds limit: %d", p.segments[0].sizeBytes)
	}
	if p.segments[1].baseOffset != 4 || p.segments[2].baseOffset != 8 {
		t.Fatalf("bases: %d %d", p.segments[1].baseOffset, p.segments[2].baseOffset)
	}
	for _, name := range []string{segmentName(0), segmentName(4), segmentName(8), indexName(0), indexName(4), indexName(8)} {
		if _, err := os.Stat(filepath.Join(p.dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	if off, _ := p.Offsets(); off != (Offsets{Earliest: 0, Latest: 10}) {
		t.Fatalf("%+v", off)
	}
}

// Reads span segment boundaries with contiguous offsets.
func TestReadFromSpansSegments(t *testing.T) {
	p := newTestPartition(t, nil)
	p.maxSegmentBytes = 100
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, []byte(fmt.Sprintf("v%d", i)), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := readAll(p, 0, 100)
	if err != nil || len(recs) != 10 {
		t.Fatalf("%d %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != Offset(i) || string(r.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %d: %+v", i, r)
		}
	}
	// Start mid-segment, cross into the next.
	recs, err = readAll(p, 3, 4)
	if err != nil || len(recs) != 4 || recs[0].Offset != 3 || recs[3].Offset != 6 {
		t.Fatalf("%+v %v", recs, err)
	}
	// Start exactly at a segment base.
	recs, err = readAll(p, 4, 2)
	if err != nil || len(recs) != 2 || recs[0].Offset != 4 {
		t.Fatalf("%+v %v", recs, err)
	}
}

// The sparse index is persisted and every entry points at a record boundary.
func TestIndexEntriesWritten(t *testing.T) {
	p := newTestPartition(t, nil)
	p.indexIntervalBytes = 30 // ~23-byte frames => index roughly every other record
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, []byte("v"), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(p.dir, indexName(0)))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || len(data)%indexEntrySize != 0 {
		t.Fatalf("index size %d", len(data))
	}
	f, err := os.Open(filepath.Join(p.dir, segmentName(0)))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var prevRel int64 = -1
	for pos := 0; pos < len(data); pos += indexEntrySize {
		rel := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		filePos := int64(binary.BigEndian.Uint64(data[pos+8 : pos+16]))
		if rel <= prevRel {
			t.Fatalf("index not monotonic: %d after %d", rel, prevRel)
		}
		prevRel = rel
		// Every entry must point at a decodable record with that exact offset.
		r, _, err := DecodeRecord(io.NewSectionReader(f, filePos, math.MaxInt64), Offset(rel))
		if err != nil || r.Offset != Offset(rel) {
			t.Fatalf("entry (%d,%d): %+v %v", rel, filePos, r, err)
		}
	}
	// First entry must be (0, 0).
	if rel := int64(binary.BigEndian.Uint64(data[0:8])); rel != 0 {
		t.Fatalf("first entry rel=%d", rel)
	}
}

// Reopen with multiple segments: offsets continue, all records readable, and
// the on-disk index is rebuilt consistently.
func TestReopenMultiSegment(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p, err := openPartition(dir, 100, 30)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, []byte(fmt.Sprintf("v%d", i)), AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
	nSegs := len(p.segments)
	if nSegs < 2 {
		t.Fatalf("expected roll, got %d", nSegs)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := openPartition(dir, 100, 30)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if len(p2.segments) != nSegs {
		t.Fatalf("reopened with %d segments, want %d", len(p2.segments), nSegs)
	}
	if off, _ := p2.Offsets(); off.Latest != 10 {
		t.Fatalf("%+v", off)
	}
	r, err := p2.Append(nil, []byte("next"), AcksLeader)
	if err != nil || r.Offset != 10 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, err := readAll(p2, 0, 100)
	if err != nil || len(recs) != 11 {
		t.Fatalf("%d %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != Offset(i) {
			t.Fatalf("record %d at offset %d", i, r.Offset)
		}
	}
}

// Concurrent appends with a tiny segment limit: roll under contention must
// still produce unique offsets and fully decodable segments.
func TestConcurrentAppendWithRoll(t *testing.T) {
	p := newTestPartition(t, nil)
	p.maxSegmentBytes = 128
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := p.Append(nil, []byte(fmt.Sprintf("value-%d", i)), AcksLeader); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if off, _ := p.Offsets(); off.Latest != n {
		t.Fatalf("LEO=%d", off.Latest)
	}
	recs, err := readAll(p, 0, n)
	if err != nil || len(recs) != n {
		t.Fatalf("%d %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != Offset(i) {
			t.Fatalf("record %d at offset %d", i, r.Offset)
		}
	}
	// Segment bases must be contiguous: each base == previous base + records.
	for i := 1; i < len(p.segments); i++ {
		prev, cur := p.segments[i-1], p.segments[i]
		if cur.baseOffset != prev.baseOffset+Offset(prev.records) {
			t.Fatalf("gap between segments %d and %d", i-1, i)
		}
	}
}

// --- Slice 6: acks ---

// acks=0 never calls Sync: on a file whose Sync always fails, the acks=0
// append still succeeds and is readable in-process, while acks=1 on the same
// file fails and fences the partition — and the fence then covers acks=0 too.
func TestAppendAcksNoneSkipsSync(t *testing.T) {
	p := newTestPartition(t, func(f syncFile) syncFile { return failSync{f} })
	r, err := p.Append(nil, []byte("x"), AcksNone)
	if err != nil || r.Offset != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, err := readAll(p, 0, 10)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "x" {
		t.Fatalf("%+v %v", recs, err)
	}
	if _, err := p.Append(nil, []byte("y"), AcksLeader); !errors.Is(err, ErrResultUncertain) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("z"), AcksNone); !errors.Is(err, ErrPartitionFenced) {
		t.Fatalf("err=%v", err)
	}
}

// An unknown acks value is a caller error: rejected before any write, and it
// does NOT fence the partition.
func TestAppendUnknownAcksRejected(t *testing.T) {
	p := newTestPartition(t, nil)
	if _, err := p.Append(nil, []byte("x"), Acks(7)); err == nil {
		t.Fatal("accepted acks=7")
	}
	if off, _ := p.Offsets(); off.Latest != 0 {
		t.Fatalf("rejected append moved LEO: %+v", off)
	}
	if _, err := p.Append(nil, []byte("ok"), AcksLeader); err != nil {
		t.Fatalf("healthy partition fenced: %v", err)
	}
}
