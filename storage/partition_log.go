package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"
)

// openPartition opens the single base=0 segment of a registered partition.
// The log file must already exist: a registered topic with a missing log is a
// data-loss signal, never a reason to silently create an empty one.
func openPartition(dir string) (*PartitionLog, error) {
	path := filepath.Join(dir, segmentName(0))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644) // no O_CREATE
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	sizeBytes, records, err := scanSegment(f, 0)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("scan segment %s: %w", path, err)
	}
	return &PartitionLog{
		dir:        dir,
		nextOffset: Offset(records), // base 0 + ordinal count
		active:     &Segment{baseOffset: 0, file: f, sizeBytes: sizeBytes, records: records},
	}, nil
}

// scanSegment replays the log from position 0, trusting only the log itself.
// A clean EOF at a frame boundary ends the scan; any torn tail or corrupt
// frame refuses the open (Slice 1 policy: never modify the file here).
func scanSegment(f syncFile, base Offset) (sizeBytes int64, records int64, err error) {
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	reader := io.NewSectionReader(f, 0, info.Size())
	for {
		_, n, err := DecodeRecord(reader, base+Offset(records))
		if errors.Is(err, io.EOF) {
			return sizeBytes, records, nil
		}
		if err != nil {
			return 0, 0, err
		}
		sizeBytes += int64(n)
		records++
	}
}

// Append assigns the next offset, then makes the record durable BEFORE it
// becomes visible: encode → write → sync → only then publish the offset.
// A short write or sync failure fences the partition (p.failed): appending
// after a partial frame would corrupt every subsequent record's identity.
func (p *PartitionLog) Append(key, value []byte) (Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Record{}, ErrClosed
	}
	if p.failed != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrPartitionFenced, p.failed)
	}
	if p.nextOffset == math.MaxInt64 {
		return Record{}, fmt.Errorf("offset overflow")
	}
	// Independent copies: the caller may reuse or mutate its buffers after return.
	r := Record{Offset: p.nextOffset, TimestampNS: time.Now().UnixNano(), Key: bytes.Clone(key), Value: bytes.Clone(value)}
	frame, err := EncodeRecord(r)
	if err != nil {
		return Record{}, err // caller error (too large); partition stays healthy
	}
	n, err := p.active.file.Write(frame)
	if err != nil || n != len(frame) {
		p.failed = fmt.Errorf("write: n=%d of %d, err=%v", n, len(frame), err)
		return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
	}
	if err := p.active.file.Sync(); err != nil {
		p.failed = fmt.Errorf("sync: %w", err)
		return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
	}
	p.active.sizeBytes += int64(len(frame))
	p.active.records++
	p.nextOffset++
	return r, nil
}

// Offsets returns the half-open readable range [0, LEO). Slice 1 has no
// retention, so Earliest is always the segment base (0).
func (p *PartitionLog) Offsets() (Offsets, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return Offsets{}, ErrClosed
	}
	return Offsets{Earliest: p.active.baseOffset, Latest: p.nextOffset}, nil
}

// ReadFrom scans from the segment base and returns up to maxRecords records
// with offset >= the requested one. Correctness first: the sparse index that
// avoids this O(N) scan arrives in Slice 2. The read lock blocks appends
// during the scan — simple and safe for Slice 1.
func (p *PartitionLog) ReadFrom(offset Offset, maxRecords int) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if maxRecords <= 0 {
		return nil, fmt.Errorf("maxRecords must be > 0")
	}
	if offset < 0 || offset > p.nextOffset {
		return nil, fmt.Errorf("%w: %d not in [0, %d]", ErrOffsetOutOfRange, offset, p.nextOffset)
	}
	out := []Record{}
	if offset == p.nextOffset { // at LEO: empty, not an error
		return out, nil
	}
	reader := io.NewSectionReader(p.active.file, 0, p.active.sizeBytes)
	for cur := p.active.baseOffset; cur < p.nextOffset && len(out) < maxRecords; cur++ {
		r, _, err := DecodeRecord(reader, cur)
		if err != nil {
			return nil, fmt.Errorf("rescan offset %d: %w", cur, err)
		}
		if cur >= offset {
			out = append(out, r)
		}
	}
	return out, nil
}

// Close takes the write lock, so it waits for any in-flight append to finish
// (or fences it) before closing the file — never closes under an active write.
func (p *PartitionLog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.active.file.Close()
}
