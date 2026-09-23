package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestPartition builds a partition whose segment file is optionally wrapped
// for fault injection.
func newTestPartition(t *testing.T, wrap func(syncFile) syncFile) *PartitionLog {
	t.Helper()
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, segmentName(0)), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var sf syncFile = f
	if wrap != nil {
		sf = wrap(sf)
	}
	p := &PartitionLog{dir: dir, active: &Segment{baseOffset: 0, file: sf}}
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

// Acceptance 2+3: append 3 records, LEO=3, records readable by offset scan.
func TestAppendAndReadBack(t *testing.T) {
	p := newTestPartition(t, nil)
	for i, v := range []string{"one", "two", "three"} {
		r, err := p.Append(nil, []byte(v))
		if err != nil || r.Offset != Offset(i) {
			t.Fatalf("append %d: %+v %v", i, r, err)
		}
	}
	off, err := p.Offsets()
	if err != nil || off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("%+v %v", off, err)
	}
	recs, err := p.ReadFrom(1, 10)
	if err != nil || len(recs) != 2 || recs[0].Offset != 1 || string(recs[1].Value) != "three" {
		t.Fatalf("%+v %v", recs, err)
	}
	if recs, err = p.ReadFrom(3, 10); err != nil || len(recs) != 0 {
		t.Fatalf("at LEO: %+v %v", recs, err)
	}
	if _, err = p.ReadFrom(-1, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatal(err)
	}
	if _, err = p.ReadFrom(4, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatal(err)
	}
	if _, err = p.ReadFrom(0, 0); err == nil {
		t.Fatal("maxRecords=0 accepted")
	}
	if recs, err = p.ReadFrom(0, 2); err != nil || len(recs) != 2 || recs[1].Offset != 1 {
		t.Fatalf("maxRecords: %+v %v", recs, err)
	}
}

// Acceptance 4 (same partition): concurrent appends get unique offsets and
// frames never interleave — every record decodes back at its own offset.
func TestAppendConcurrent(t *testing.T) {
	p := newTestPartition(t, nil)
	const n = 64
	offsets := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := p.Append(nil, []byte(fmt.Sprintf("v%d", i)))
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
	recs, err := p.ReadFrom(0, n)
	if err != nil || len(recs) != n {
		t.Fatalf("%d %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != Offset(i) {
			t.Fatalf("record %d decoded at offset %d", i, r.Offset)
		}
	}
}

// Acceptance 5: reopen continues the offset sequence instead of restarting at 0.
func TestReopenContinuesOffset(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p, err := openPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := p.Append(nil, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := openPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if off, _ := p2.Offsets(); off != (Offsets{Earliest: 0, Latest: 3}) {
		t.Fatalf("reopened %+v", off)
	}
	r, err := p2.Append(nil, []byte("after-reopen"))
	if err != nil || r.Offset != 3 {
		t.Fatalf("%+v %v", r, err)
	}
	recs, err := p2.ReadFrom(0, 10)
	if err != nil || len(recs) != 4 || string(recs[3].Value) != "after-reopen" {
		t.Fatalf("%+v %v", recs, err)
	}
}

type failSync struct{ syncFile }

func (failSync) Sync() error { return errors.New("injected sync failure") }

type shortWrite struct{ syncFile }

func (f shortWrite) Write(b []byte) (int, error) { return len(b) - 1, nil }

// Acceptance 6: write/sync failures never ack success and fence the partition.
func TestSyncFailureFencesPartition(t *testing.T) {
	p := newTestPartition(t, func(f syncFile) syncFile { return failSync{f} })
	if _, err := p.Append(nil, []byte("x")); !errors.Is(err, ErrResultUncertain) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("y")); !errors.Is(err, ErrPartitionFenced) {
		t.Fatalf("err=%v", err)
	}
	if off, err := p.Offsets(); err != nil || off.Latest != 0 { // nothing published
		t.Fatalf("%+v %v", off, err)
	}
}

func TestShortWriteFencesPartition(t *testing.T) {
	p := newTestPartition(t, func(f syncFile) syncFile { return shortWrite{f} })
	if _, err := p.Append(nil, []byte("x")); !errors.Is(err, ErrResultUncertain) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("y")); !errors.Is(err, ErrPartitionFenced) {
		t.Fatalf("err=%v", err)
	}
}

// Acceptance 7: a torn tail or corrupt frame refuses the open; Slice 1 never
// modifies the file (truncation repair is Slice 2).
func TestOpenPartitionTornTailRefuses(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p, err := openPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := p.Append(nil, []byte{byte(i)}); err != nil {
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
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{recordMagic, 0, 0}); err != nil { // partial header
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openPartition(dir); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err=%v", err)
	}
	if info2, _ := os.Stat(path); info2.Size() != info.Size()+3 {
		t.Fatalf("open modified file: %d -> %d", info.Size(), info2.Size())
	}
}

func TestOpenPartitionCorruptFrameRefuses(t *testing.T) {
	dir := t.TempDir()
	createSegment(t, dir)
	p, err := openPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(nil, []byte("good")); err != nil {
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
	if _, err := openPartition(dir); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenPartitionMissingLog(t *testing.T) {
	if _, err := openPartition(t.TempDir()); err == nil {
		t.Fatal("opened a partition with no segment file")
	}
}

// An oversized record is a caller error: it must NOT fence the partition.
func TestAppendTooLargeDoesNotFence(t *testing.T) {
	p := newTestPartition(t, nil)
	if _, err := p.Append(nil, make([]byte, MaxRecordBytes)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Append(nil, []byte("ok")); err != nil {
		t.Fatalf("healthy partition fenced: %v", err)
	}
}

func TestCloseFencesOperations(t *testing.T) {
	p := newTestPartition(t, nil)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Offsets(); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.ReadFrom(0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
	if err := p.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
}
