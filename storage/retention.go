package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"
)

// DefaultRetentionCheckInterval is how often the background janitor runs one
// retention pass. Tests call EnforceRetention directly instead of sleeping.
const DefaultRetentionCheckInterval = 30 * time.Second

// SetRetentionInterval changes how often the janitor ticks and wakes a pending
// wait so the new interval applies immediately. Non-positive values are
// ignored — a zero timer would spin the janitor in a busy loop.
func (b *Broker) SetRetentionInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	b.mu.Lock()
	b.retentionInterval = d
	kick := b.retentionKick
	b.retentionKick = make(chan struct{})
	b.mu.Unlock()
	close(kick) // wake outside the lock; whoever replaced the channel closes it
}

// retentionLoop is the whole background machinery: a timer around the
// deterministic single pass. It re-reads the interval each cycle (under mu,
// never held across the pass) so the setter above is race-free, and selects
// on the kick channel so a shortened interval cannot be missed by a wait that
// already started.
func (b *Broker) retentionLoop() {
	defer b.retentionWg.Done()
	for {
		b.mu.RLock()
		interval := b.retentionInterval
		kick := b.retentionKick
		b.mu.RUnlock()
		if interval <= 0 {
			interval = DefaultRetentionCheckInterval
		}
		t := time.NewTimer(interval)
		select {
		case <-b.retentionStop:
			t.Stop()
			return
		case <-kick: // interval changed; abandon the old wait and re-time
			t.Stop()
			continue
		case <-t.C:
		}
		if _, err := b.EnforceRetention(); err != nil && !errors.Is(err, ErrClosed) {
			log.Printf("marchiq retention: %v", err)
		}
	}
}

// EnforceRetention runs ONE retention pass over every topic that has a
// retention policy and returns how many segments were deleted. It never
// sleeps: the ticker is a thin loop around this method, and tests drive it
// directly (aging segment mtimes with os.Chtimes instead of waiting).
//
// Lock order: mu (snapshot topics) is released before gmu (snapshot group
// clamps), and neither is held while taking per-partition locks — the
// established order is mu → gmu, and partition locks serialize the deletion
// against in-flight appends exactly like segment roll does.
func (b *Broker) EnforceRetention() (int, error) {
	now := time.Now()
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrClosed
	}
	type topicView struct {
		cfg        TopicConfig
		partitions []*PartitionLog
	}
	var views []topicView
	for _, t := range b.topics {
		if t.config.RetentionMS <= 0 && t.config.RetentionBytes <= 0 {
			continue // zero on both = unlimited; most topics never pay for this
		}
		views = append(views, topicView{t.config, t.partitions})
	}
	b.mu.RUnlock()

	total := 0
	var errs []error
	for _, v := range views {
		b.gmu.RLock()
		clamps, clamped := b.retentionClampsLocked(v.cfg.Name, len(v.partitions))
		b.gmu.RUnlock()
		for i, p := range v.partitions {
			n, err := p.enforceRetention(now, v.cfg, clamps[i], clamped)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s[%d]: %w", v.cfg.Name, i, err))
			}
			total += n
		}
	}
	return total, errors.Join(errs...)
}

// retentionClampsLocked computes the deletable boundary per partition of one
// topic: B = min over every group JOINED to the topic of (committed+1). A
// joined group that never committed for the partition contributes 0 — its
// next read is offset 0, so nothing may be deleted (conservative and
// correct). clamped is false when no group has joined the topic at all, and
// then retention deletes freely. Caller must hold b.gmu (read is enough).
func (b *Broker) retentionClampsLocked(topic string, partitions int) (bounds []Offset, clamped bool) {
	bounds = make([]Offset, partitions)
	for i := range bounds {
		bounds[i] = math.MaxInt64
	}
	for _, g := range b.groups {
		gt := g.topics[topic]
		if gt == nil {
			continue
		}
		clamped = true
		for p := 0; p < partitions; p++ {
			bound := Offset(0)
			if c, ok := gt.Offsets[p]; ok {
				bound = c + 1
			}
			if bound < bounds[p] {
				bounds[p] = bound
			}
		}
	}
	return bounds, clamped
}

// enforceRetention deletes SEALED segments that are both entirely below the
// group clamp and expired by the topic's age or size policy. The active
// segment is never a candidate, even if it alone exceeds the size budget.
// Segments are contiguous (base[i+1] = base[i] + records[i]), so segments
// below the clamp always form a prefix and the scan stops at the first one
// that reaches it.
//
// Deletion mirrors the durability discipline of segment roll in reverse:
// unlink .log + .index, then sync the partition directory. A crash mid-delete
// can only lose data the clamp already released, and a lost dir sync can at
// worst resurrect a clamp-approved segment, which the next pass deletes
// again — deletion is idempotent (missing files are not errors).
func (p *PartitionLog) enforceRetention(now time.Time, cfg TopicConfig, clamp Offset, clamped bool) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrClosed
	}
	if p.failed != nil {
		return 0, nil // fenced partition: keep every byte for forensics
	}
	belowClamp := func(s *Segment) bool {
		return !clamped || s.baseOffset+Offset(s.records) <= clamp
	}
	sealed := p.segments[:len(p.segments)-1]
	doomed := make(map[Offset]struct{}, len(sealed))
	if cfg.RetentionMS > 0 {
		cutoff := now.Add(-time.Duration(cfg.RetentionMS) * time.Millisecond)
		for _, s := range sealed {
			if !belowClamp(s) {
				break
			}
			info, err := s.file.Stat()
			if err != nil {
				return 0, fmt.Errorf("stat segment %d: %w", s.baseOffset, err)
			}
			// A sealed segment's .log mtime ≈ when it was sealed: the last
			// append happened while it was active, and index rebuilds never
			// touch the .log — so the mtime survives restarts.
			if info.ModTime().Before(cutoff) {
				doomed[s.baseOffset] = struct{}{}
			}
		}
	}
	if cfg.RetentionBytes > 0 {
		var total int64
		for _, s := range p.segments {
			total += s.sizeBytes
		}
		for _, s := range sealed { // oldest first until back under budget
			if total <= cfg.RetentionBytes || !belowClamp(s) {
				break
			}
			doomed[s.baseOffset] = struct{}{}
			total -= s.sizeBytes
		}
	}
	if len(doomed) == 0 {
		return 0, nil
	}
	kept := make([]*Segment, 0, len(p.segments)-len(doomed))
	var errs []error
	deleted := 0
	for _, s := range p.segments {
		if _, ok := doomed[s.baseOffset]; !ok {
			kept = append(kept, s)
			continue
		}
		logErr := os.Remove(filepath.Join(p.dir, segmentName(s.baseOffset)))
		idxErr := os.Remove(filepath.Join(p.dir, indexName(s.baseOffset)))
		if err := firstRealError(logErr, idxErr); err != nil {
			// Still on disk and open: keep serving it, retry next pass.
			errs = append(errs, fmt.Errorf("unlink segment %d: %w", s.baseOffset, err))
			kept = append(kept, s)
			continue
		}
		// Unlinked: open fds kept the inode alive for in-flight reads until now.
		if err := s.close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %d: %w", s.baseOffset, err))
		}
		deleted++
	}
	if deleted > 0 {
		if err := syncDir(p.dir); err != nil { // deletion must be durable too
			errs = append(errs, fmt.Errorf("sync partition dir: %w", err))
		}
	}
	p.segments = kept
	return deleted, errors.Join(errs...)
}

// firstRealError returns the first error that is not "already gone": a
// missing file means a previous pass died mid-delete, and retrying must be
// able to finish the job.
func firstRealError(errs ...error) error {
	for _, err := range errs {
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}
