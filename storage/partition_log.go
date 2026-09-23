package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// openPartition opens every segment of a registered partition in base-offset
// order and replays them, trusting only the logs. A torn tail on the ACTIVE
// (last) segment is truncated and synced — that is the crash-recovery path for
// interrupted appends. A torn tail on a SEALED segment, or any corrupt frame,
// refuses the open: sealed data must be complete, and guessing is not repair.
func openPartition(dir string, maxSegmentBytes, indexIntervalBytes int64) (*PartitionLog, error) {
	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		return nil, fmt.Errorf("partition %s has no segment files", dir)
	}
	p := &PartitionLog{
		dir:                dir,
		maxSegmentBytes:    maxSegmentBytes,
		indexIntervalBytes: indexIntervalBytes,
	}
	for i, base := range bases {
		seg, err := openSegment(dir, base, i == len(bases)-1, indexIntervalBytes)
		if err != nil {
			for _, s := range p.segments {
				s.close()
			}
			return nil, err
		}
		p.segments = append(p.segments, seg)
	}
	last := p.active()
	p.nextOffset = last.baseOffset + Offset(last.records)
	return p, nil
}

func listSegmentBases(dir string) ([]Offset, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list partition dir: %w", err)
	}
	var bases []Offset
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad segment name %s: %w", name, err)
		}
		bases = append(bases, Offset(base))
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// openSegment opens one segment, scans it, repairs the active torn tail, and
// rewrites its .index from the authoritative scan.
func openSegment(dir string, base Offset, isActive bool, indexIntervalBytes int64) (*Segment, error) {
	path := filepath.Join(dir, segmentName(base))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644) // no O_CREATE
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	sizeBytes, records, index, torn, err := scanSegment(f, base, indexIntervalBytes)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("scan segment %s: %w", path, err)
	}
	if torn {
		if !isActive {
			_ = f.Close()
			return nil, fmt.Errorf("sealed segment %s has a torn tail; refusing to repair immutable data", path)
		}
		if err := f.Truncate(sizeBytes); err != nil { // sizeBytes == lastGood boundary
			_ = f.Close()
			return nil, fmt.Errorf("truncate torn tail of %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sync truncated %s: %w", path, err)
		}
	}
	ix, err := os.OpenFile(filepath.Join(dir, indexName(base)), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open index: %w", err)
	}
	if err := rewriteIndexFile(dir, base, index); err != nil {
		_ = f.Close()
		_ = ix.Close()
		return nil, fmt.Errorf("rewrite index: %w", err)
	}
	seg := &Segment{baseOffset: base, file: f, indexFile: ix, sizeBytes: sizeBytes, records: records, index: index}
	if len(index) > 0 {
		seg.lastIndexedPos = index[len(index)-1].position
	}
	return seg, nil
}

// scanSegment replays the log from position 0, building the sparse index as it
// goes. A clean EOF at a frame boundary ends the scan. io.ErrUnexpectedEOF
// means a torn tail: the scan reports it (sizeBytes stays at the last good
// boundary) and lets the caller decide — truncate for active, refuse for
// sealed. Any other corruption is fatal.
func scanSegment(f syncFile, base Offset, indexIntervalBytes int64) (sizeBytes, records int64, index []indexEntry, torn bool, err error) {
	info, err := f.Stat()
	if err != nil {
		return 0, 0, nil, false, err
	}
	reader := io.NewSectionReader(f, 0, info.Size())
	var lastIndexedPos int64
	for {
		frameStart := sizeBytes // lastGood boundary before this decode
		_, n, derr := DecodeRecord(reader, base+Offset(records))
		if errors.Is(derr, io.EOF) {
			return sizeBytes, records, index, false, nil
		}
		if derr != nil {
			if errors.Is(derr, io.ErrUnexpectedEOF) {
				return sizeBytes, records, index, true, nil
			}
			return 0, 0, nil, false, derr
		}
		if records == 0 || frameStart-lastIndexedPos >= indexIntervalBytes {
			index = append(index, indexEntry{relOffset: records, position: frameStart})
			lastIndexedPos = frameStart
		}
		sizeBytes += int64(n)
		records++
	}
}

// Append assigns the next offset, rolls the segment if the frame would exceed
// maxSegmentBytes, then makes the record durable BEFORE it becomes visible:
// index entry → write → sync → only then publish. A short write or sync
// failure fences the partition (p.failed): appending after a partial frame
// would corrupt every subsequent record's identity.
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
	if seg := p.active(); seg.records > 0 && seg.sizeBytes+int64(len(frame)) > p.maxSegmentBytes {
		if err := p.rollLocked(); err != nil {
			p.failed = fmt.Errorf("roll: %w", err)
			return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
		}
	}
	seg := p.active()
	if seg.records == 0 || seg.sizeBytes-seg.lastIndexedPos >= p.indexIntervalBytes {
		entry := indexEntry{relOffset: seg.records, position: seg.sizeBytes}
		if _, err := seg.indexFile.Write(encodeIndexEntry(entry)); err != nil {
			p.failed = fmt.Errorf("write index: %w", err)
			return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
		}
		seg.index = append(seg.index, entry)
		seg.lastIndexedPos = seg.sizeBytes
	}
	n, err := seg.file.Write(frame)
	if err != nil || n != len(frame) {
		p.failed = fmt.Errorf("write: n=%d of %d, err=%v", n, len(frame), err)
		return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
	}
	if err := seg.file.Sync(); err != nil {
		p.failed = fmt.Errorf("sync: %w", err)
		return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
	}
	if err := seg.indexFile.Sync(); err != nil {
		p.failed = fmt.Errorf("sync index: %w", err)
		return Record{}, fmt.Errorf("%w: %v", ErrResultUncertain, p.failed)
	}
	seg.sizeBytes += int64(len(frame))
	seg.records++
	p.nextOffset++
	return r, nil
}

// rollLocked seals the active segment and opens a fresh one whose base offset
// is the current LEO. Sealed segments stay open for reads but are never
// written again.
func (p *PartitionLog) rollLocked() error {
	old := p.active()
	if err := old.file.Sync(); err != nil {
		return fmt.Errorf("seal segment: %w", err)
	}
	if err := old.indexFile.Sync(); err != nil {
		return fmt.Errorf("seal index: %w", err)
	}
	base := p.nextOffset
	f, err := os.OpenFile(filepath.Join(p.dir, segmentName(base)), os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	ix, err := os.OpenFile(filepath.Join(p.dir, indexName(base)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("create index: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = ix.Close()
		return fmt.Errorf("sync new segment: %w", err)
	}
	if err := syncDir(p.dir); err != nil {
		_ = f.Close()
		_ = ix.Close()
		return fmt.Errorf("sync partition dir: %w", err)
	}
	p.segments = append(p.segments, &Segment{baseOffset: base, file: f, indexFile: ix})
	return nil
}

// Offsets returns the half-open readable range [earliest, LEO). Until
// retention lands in Slice 5, earliest is always the first segment's base (0).
func (p *PartitionLog) Offsets() (Offsets, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return Offsets{}, ErrClosed
	}
	return Offsets{Earliest: p.segments[0].baseOffset, Latest: p.nextOffset}, nil
}

// ReadFrom returns up to maxRecords records with offset >= the requested one,
// spanning segment boundaries, stopping once maxBytes of frame data would be
// exceeded — but always returns at least one record (Kafka's max_bytes rule:
// a single record larger than the limit is still delivered). The sparse index
// seeks to the nearest indexed record at or before the target, so the scan
// costs O(interval), not O(N).
func (p *PartitionLog) ReadFrom(offset Offset, maxRecords int, maxBytes int64) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if maxRecords <= 0 {
		return nil, fmt.Errorf("maxRecords must be > 0")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be > 0")
	}
	if offset < 0 || offset > p.nextOffset {
		return nil, fmt.Errorf("%w: %d not in [0, %d]", ErrOffsetOutOfRange, offset, p.nextOffset)
	}
	out := []Record{}
	if offset == p.nextOffset { // at LEO: empty, not an error
		return out, nil
	}
	var totalBytes int64
	si := sort.Search(len(p.segments), func(i int) bool {
		s := p.segments[i]
		return s.baseOffset+Offset(s.records) > offset
	})
	for ; si < len(p.segments) && len(out) < maxRecords; si++ {
		seg := p.segments[si]
		start := offset
		if start < seg.baseOffset {
			start = seg.baseOffset
		}
		entry := lookupIndex(seg.index, int64(start-seg.baseOffset))
		cur := seg.baseOffset + Offset(entry.relOffset)
		end := seg.baseOffset + Offset(seg.records)
		reader := io.NewSectionReader(seg.file, entry.position, seg.sizeBytes-entry.position)
		for cur < end && len(out) < maxRecords {
			r, n, err := DecodeRecord(reader, cur)
			if err != nil {
				return nil, fmt.Errorf("rescan offset %d: %w", cur, err)
			}
			if cur >= start {
				if len(out) > 0 && totalBytes+int64(n) > maxBytes {
					return out, nil
				}
				out = append(out, r)
				totalBytes += int64(n)
			}
			cur++
		}
	}
	return out, nil
}

// DescribeSegments returns a snapshot of the segment layout for observability.
func (p *PartitionLog) DescribeSegments() ([]SegmentInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	out := make([]SegmentInfo, 0, len(p.segments))
	for _, s := range p.segments {
		out = append(out, SegmentInfo{BaseOffset: s.baseOffset, SizeBytes: s.sizeBytes, Records: s.records})
	}
	return out, nil
}

// Close takes the write lock, so it waits for any in-flight append to finish
// before closing every segment file — never closes under an active write.
func (p *PartitionLog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var errs []error
	for _, s := range p.segments {
		if err := s.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Segment) close() error {
	return errors.Join(s.file.Close(), s.indexFile.Close())
}
