package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// indexEntrySize is the fixed on-disk row size: rel_offset(8) | position(8),
// both big-endian int64. Fixed-size rows allow random access by entry number.
const indexEntrySize = 16

// indexEntry maps a relative offset (offset - segment base) to the byte
// position of that record's frame within the segment file. Entries are
// sparse: one per indexIntervalBytes of log data, plus the first record.
type indexEntry struct {
	relOffset int64
	position  int64
}

func encodeIndexEntry(e indexEntry) []byte {
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.relOffset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.position))
	return buf[:]
}

// lookupIndex returns the entry with the largest relOffset <= target, i.e.
// the closest indexed record at or before the one we want. The scan resumes
// from entry.position, so the index turns an O(N) scan into O(interval).
// With no entry <= target (only possible for the segment's first records),
// it returns the zero entry: start of file.
func lookupIndex(index []indexEntry, target int64) indexEntry {
	i := sort.Search(len(index), func(i int) bool { return index[i].relOffset > target })
	if i == 0 {
		return indexEntry{0, 0}
	}
	return index[i-1]
}

// rewriteIndexFile atomically replaces a segment's .index file with the given
// entries (tmp → sync → rename). The index is a derived cache of the log:
// Open rebuilds it from the authoritative scan, which also drops any entries
// that pointed past a truncated torn tail.
func rewriteIndexFile(dir string, base Offset, entries []indexEntry) error {
	tmp := filepath.Join(dir, indexName(base)+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := f.Write(encodeIndexEntry(e)); err != nil {
			_ = f.Close()
			return fmt.Errorf("write index: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, indexName(base))); err != nil {
		return err
	}
	return syncDir(dir)
}
