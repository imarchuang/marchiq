package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const catalogVersion = 1

// Open loads the catalog and replays every registered partition log. It only
// trusts the logs: nextOffset and sizes are rebuilt by scanning, never cached.
// Any corrupt catalog, missing log, or bad frame fails the whole open — a
// broker that cannot account for registered data must not serve writes.
func Open(dataDir string) (*Broker, error) {
	return OpenWithConfig(dataDir, DefaultMaxSegmentBytes, DefaultIndexIntervalBytes)
}

// OpenWithConfig is Open with explicit roll/index density limits, so demos and
// tests can exercise segment roll without writing gigabytes.
func OpenWithConfig(dataDir string, maxSegmentBytes, indexIntervalBytes int64) (*Broker, error) {
	if maxSegmentBytes <= 0 || indexIntervalBytes <= 0 {
		return nil, fmt.Errorf("segment/index limits must be positive")
	}
	if err := InitDirs(dataDir); err != nil {
		return nil, err
	}
	meta, err := loadCatalog(dataDir)
	if err != nil {
		return nil, err
	}
	groups, err := loadGroups(dataDir)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		dataDir:            dataDir,
		topics:             make(map[string]*Topic, len(meta.Topics)),
		groups:             groups,
		maxSegmentBytes:    maxSegmentBytes,
		indexIntervalBytes: indexIntervalBytes,
		retentionInterval:  DefaultRetentionCheckInterval,
		retentionKick:      make(chan struct{}),
		retentionStop:      make(chan struct{}),
	}
	for _, cfg := range meta.Topics {
		t, err := b.openTopic(cfg)
		if err != nil {
			_ = b.Close()
			return nil, fmt.Errorf("open topic %q: %w", cfg.Name, err)
		}
		b.topics[cfg.Name] = t
	}
	// The janitor starts only after every partition opened cleanly, so it can
	// never run against a half-constructed broker.
	b.retentionWg.Add(1)
	go b.retentionLoop()
	return b, nil
}

func (b *Broker) openTopic(cfg TopicConfig) (*Topic, error) {
	t := &Topic{config: cfg, partitions: make([]*PartitionLog, 0, cfg.Partitions)}
	for i := 0; i < cfg.Partitions; i++ {
		p, err := openPartition(filepath.Join(b.dataDir, "topics", cfg.Name, strconv.Itoa(i)),
			b.maxSegmentBytes, b.indexIntervalBytes)
		if err != nil {
			for _, q := range t.partitions {
				_ = q.Close()
			}
			return nil, err
		}
		t.partitions = append(t.partitions, p)
	}
	return t, nil
}

// CreateTopic creates partition dirs and empty logs, syncs them, publishes the
// catalog atomically (tmp → sync → rename → sync dir), and only then exposes
// the topic in memory. A topic that failed catalog publication is never
// visible to producers; leftover unregistered directories cause a conflict on
// retry and require manual cleanup — they are never adopted or deleted here.
func (b *Broker) CreateTopic(config TopicConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if _, ok := b.topics[config.Name]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, config.Name)
	}
	topicDir := filepath.Join(b.dataDir, "topics", config.Name)
	if _, err := os.Stat(topicDir); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("unregistered directory %s exists; manual cleanup required", topicDir)
	}
	files := make([]*os.File, 0, 2*config.Partitions)
	fail := func(err error) error { // close what we opened; keep dirs for inspection
		for _, f := range files {
			_ = f.Close()
		}
		return err
	}
	for i := 0; i < config.Partitions; i++ {
		dir := filepath.Join(topicDir, strconv.Itoa(i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fail(fmt.Errorf("create partition dir: %w", err))
		}
		logF, err := os.OpenFile(filepath.Join(dir, segmentName(0)), os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return fail(fmt.Errorf("create segment: %w", err))
		}
		files = append(files, logF)
		idxF, err := os.OpenFile(filepath.Join(dir, indexName(0)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err != nil {
			return fail(fmt.Errorf("create index: %w", err))
		}
		files = append(files, idxF)
		if err := logF.Sync(); err != nil {
			return fail(fmt.Errorf("sync segment: %w", err))
		}
		if err := idxF.Sync(); err != nil {
			return fail(fmt.Errorf("sync index: %w", err))
		}
		if err := syncDir(dir); err != nil { // the directory entries themselves must be durable
			return fail(fmt.Errorf("sync partition dir: %w", err))
		}
	}
	if err := syncDir(topicDir); err != nil {
		return fail(fmt.Errorf("sync topic dir: %w", err))
	}
	if err := syncDir(filepath.Join(b.dataDir, "topics")); err != nil {
		return fail(fmt.Errorf("sync topics dir: %w", err))
	}
	meta := TopicsMetadata{Version: catalogVersion, Topics: append(b.listTopicsLocked(), config)}
	if err := saveCatalog(b.dataDir, meta); err != nil {
		return fail(fmt.Errorf("publish catalog: %w", err))
	}
	t := &Topic{config: config, partitions: make([]*PartitionLog, 0, config.Partitions)}
	for i := 0; i < config.Partitions; i++ {
		logF, idxF := files[2*i], files[2*i+1]
		t.partitions = append(t.partitions, &PartitionLog{
			dir:                filepath.Join(topicDir, strconv.Itoa(i)),
			maxSegmentBytes:    b.maxSegmentBytes,
			indexIntervalBytes: b.indexIntervalBytes,
			segments:           []*Segment{{baseOffset: 0, file: logF, indexFile: idxF}},
		})
	}
	b.topics[config.Name] = t
	return nil
}

func (b *Broker) ListTopics() []TopicConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.listTopicsLocked()
}

func (b *Broker) listTopicsLocked() []TopicConfig {
	out := make([]TopicConfig, 0, len(b.topics))
	for _, t := range b.topics {
		out = append(out, t.config)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (b *Broker) getPartition(topic string, partition int) (*PartitionLog, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	t, ok := b.topics[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	if partition < 0 || partition >= len(t.partitions) {
		return nil, fmt.Errorf("%w: %s[%d]", ErrPartitionNotFound, topic, partition)
	}
	return t.partitions[partition], nil
}

// Produce appends one record to an explicit partition (round-robin partition
// -1 is Slice 6). The partition lock, not the broker lock, serializes the
// write+sync, so fsync of one partition never blocks producers on another.
func (b *Broker) Produce(topic string, partition int, key, value []byte) (Record, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return Record{}, err
	}
	return p.Append(key, value)
}

func (b *Broker) GetOffsets(topic string, partition int) (Offsets, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return Offsets{}, err
	}
	return p.Offsets()
}

// Fetch reads records starting at an explicit offset (consumer groups and
// committed offsets are Slice 4). It also returns the partition's current
// offsets so callers can see the high-water mark alongside the records.
func (b *Broker) Fetch(topic string, partition int, offset Offset, maxRecords int, maxBytes int64) ([]Record, Offsets, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, Offsets{}, err
	}
	recs, err := p.ReadFrom(offset, maxRecords, maxBytes)
	if err != nil {
		return nil, Offsets{}, err
	}
	off, err := p.Offsets()
	if err != nil {
		return nil, Offsets{}, err
	}
	return recs, off, nil
}

// DescribeSegments lists the segment layout of one partition for debugging.
func (b *Broker) DescribeSegments(topic string, partition int) ([]SegmentInfo, error) {
	p, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, err
	}
	return p.DescribeSegments()
}

// Close marks the broker closed, stops the retention janitor, then closes each
// partition. Partition locks serialize this against in-flight appends; new
// operations see ErrClosed. gmu is taken so group operations observe closed
// under the same lock they check.
func (b *Broker) Close() error {
	b.mu.Lock()
	b.gmu.Lock()
	if b.closed {
		b.gmu.Unlock()
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	topics := b.topics
	b.gmu.Unlock()
	b.mu.Unlock()
	// Stop the janitor before closing segment files. An in-flight pass holds
	// only partition locks (never mu/gmu while waiting here), so this cannot
	// deadlock; after Wait returns no retention work is running.
	close(b.retentionStop)
	b.retentionWg.Wait()
	var errs []error
	for _, t := range topics {
		for _, p := range t.partitions {
			if err := p.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// loadCatalog reads meta/topics.json. A missing file means an empty catalog;
// unknown versions, invalid configs, and duplicate topics are all fatal.
func loadCatalog(dataDir string) (TopicsMetadata, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "meta", "topics.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return TopicsMetadata{Version: catalogVersion}, nil
	}
	if err != nil {
		return TopicsMetadata{}, fmt.Errorf("read catalog: %w", err)
	}
	var meta TopicsMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return TopicsMetadata{}, fmt.Errorf("parse catalog: %w", err)
	}
	if meta.Version != catalogVersion {
		return TopicsMetadata{}, fmt.Errorf("unsupported catalog version %d", meta.Version)
	}
	seen := make(map[string]bool, len(meta.Topics))
	for _, cfg := range meta.Topics {
		if err := cfg.Validate(); err != nil {
			return TopicsMetadata{}, fmt.Errorf("catalog: %w", err)
		}
		if seen[cfg.Name] {
			return TopicsMetadata{}, fmt.Errorf("catalog: duplicate topic %q", cfg.Name)
		}
		seen[cfg.Name] = true
	}
	return meta, nil
}

// saveCatalog writes the full catalog to a tmp file, syncs it, renames over
// the live file, then syncs the meta directory. Atomic rename alone does NOT
// make the directory entry durable — the dir sync is part of the protocol.
func saveCatalog(dataDir string, meta TopicsMetadata) error {
	sort.Slice(meta.Topics, func(i, j int) bool { return meta.Topics[i].Name < meta.Topics[j].Name })
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Join(dataDir, "meta")
	tmp := filepath.Join(dir, "topics.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "topics.json")); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
