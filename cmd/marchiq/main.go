package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/marchi/marchiq/storage"
)

const helpText = `marchiq — Slice 6: polish (acks=0, broker-side partitioner, lag + stats, cmd/demo)
POST /topics                      create topic, JSON body {"name":"events","partitions":2,
                                  "retention_ms":3600000,"retention_bytes":1073741824}
                                  retention fields optional, zero = unlimited; a background
                                  janitor deletes sealed segments older than retention_ms or
                                  over retention_bytes, but never offsets a joined group
                                  still needs (min committed+1) and never the active segment
GET  /topics                      list topics
GET  /topics/{topic}/offsets      earliest/latest per partition
POST /produce?topic=T&partition=P&key=K&acks=A
                                  append record; request body is the value.
                                  partition=-1 lets the broker pick: key present →
                                  fnv32a(key) % N (same key, same partition); no key →
                                  per-topic round-robin. acks=1 (default) fsyncs before
                                  ack; acks=0 returns after the page-cache write — an OS
                                  crash can lose recent records (see storage/DURABILITY.md)
GET  /fetch?topic=T&partition=P&offset=O&max_bytes=B&max_records=N
                                  read records from offset O; key/value are base64 in JSON
GET  /fetch?group=G&topic=T&member=M      group mode: read from committed+1 on assigned partitions
POST /groups/{group}/join?topic=T&member=M&members=N
                                  join group (N = declared size, fixed by first join); returns static assignment
POST /commit                      commit offset, JSON body {"group":"g","topic":"t","partition":0,"offset":9}
GET  /debug/segments?topic=T&partition=P  list segment files + sizes
GET  /debug/lag[?group=G]         per joined (group,topic,partition): committed, next_fetch,
                                  latest, lag = latest - next_fetch (negative when data loss
                                  rewound LEO below the committed offset)
GET  /debug/stats                 process-lifetime produce/fetch record + payload-byte counters
GET  /healthz                     liveness
`

func handler(api storage.BrokerAPI) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, helpText)
	})
	mux.HandleFunc("POST /topics", createTopic(api))
	mux.HandleFunc("GET /topics", listTopics(api))
	mux.HandleFunc("GET /topics/{topic}/offsets", topicOffsets(api))
	mux.HandleFunc("POST /produce", produce(api))
	mux.HandleFunc("GET /fetch", fetch(api))
	mux.HandleFunc("POST /groups/{group}/join", joinGroup(api))
	mux.HandleFunc("POST /commit", commitOffset(api))
	mux.HandleFunc("GET /debug/segments", debugSegments(api))
	mux.HandleFunc("GET /debug/lag", debugLag(api))
	mux.HandleFunc("GET /debug/stats", debugStats(api))
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func createTopic(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var cfg storage.TopicConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if err := api.CreateTopic(cfg); err != nil {
			if errors.Is(err, storage.ErrTopicExists) {
				writeErr(w, http.StatusConflict, err.Error())
			} else {
				writeErr(w, http.StatusBadRequest, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusCreated, cfg)
	}
}

func listTopics(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"topics": api.ListTopics()})
	}
}

type partitionOffsets struct {
	Partition int   `json:"partition"`
	Earliest  int64 `json:"earliest"`
	Latest    int64 `json:"latest"`
}

func topicOffsets(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		var cfg *storage.TopicConfig
		for _, t := range api.ListTopics() {
			if t.Name == topic {
				c := t
				cfg = &c
				break
			}
		}
		if cfg == nil {
			writeErr(w, http.StatusNotFound, "topic not found: "+topic)
			return
		}
		parts := make([]partitionOffsets, 0, cfg.Partitions)
		for i := 0; i < cfg.Partitions; i++ {
			off, err := api.GetOffsets(topic, i)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			parts = append(parts, partitionOffsets{Partition: i, Earliest: int64(off.Earliest), Latest: int64(off.Latest)})
		}
		writeJSON(w, http.StatusOK, map[string]any{"topic": topic, "partitions": parts})
	}
}

// fetchRecord is the JSON view of a record. Key/Value are []byte, which
// encoding/json renders as base64 strings — arbitrary binary stays safe.
type fetchRecord struct {
	Offset      int64  `json:"offset"`
	TimestampNS int64  `json:"timestamp_ns"`
	Key         []byte `json:"key,omitempty"`
	Value       []byte `json:"value"`
}

// parseFetchBudget reads the optional max_bytes/max_records query params.
func parseFetchBudget(q url.Values) (maxBytes int64, maxRecords int, err error) {
	maxBytes, maxRecords = 1<<20, 1000 // 1 MiB / 1000 records default fetch budget
	if s := q.Get("max_bytes"); s != "" {
		v, e := strconv.ParseInt(s, 10, 64)
		if e != nil || v <= 0 {
			return 0, 0, fmt.Errorf("max_bytes must be a positive integer")
		}
		maxBytes = v
	}
	if s := q.Get("max_records"); s != "" {
		v, e := strconv.Atoi(s)
		if e != nil || v <= 0 {
			return 0, 0, fmt.Errorf("max_records must be a positive integer")
		}
		maxRecords = v
	}
	return maxBytes, maxRecords, nil
}

// fetch dispatches: with a group param it is group mode (committed offsets),
// otherwise explicit-offset mode (Slice 3 semantics unchanged).
func fetch(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		maxBytes, maxRecords, err := parseFetchBudget(q)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if group := q.Get("group"); group != "" {
			fetchGroup(api, w, q.Get("topic"), group, q.Get("member"), maxRecords, maxBytes)
			return
		}
		fetchExplicit(api, w, q.Get("topic"), q.Get("partition"), q.Get("offset"), maxRecords, maxBytes)
	}
}

func fetchExplicit(api storage.BrokerAPI, w http.ResponseWriter, topic, partStr, offStr string, maxRecords int, maxBytes int64) {
	if topic == "" || partStr == "" || offStr == "" {
		writeErr(w, http.StatusBadRequest, "topic, partition and offset query params are required")
		return
	}
	partition, err := strconv.Atoi(partStr)
	if err != nil || partition < 0 {
		writeErr(w, http.StatusBadRequest, "partition must be a non-negative integer")
		return
	}
	offset, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil || offset < 0 {
		writeErr(w, http.StatusBadRequest, "offset must be a non-negative integer")
		return
	}
	recs, offs, err := api.Fetch(topic, partition, storage.Offset(offset), maxRecords, maxBytes)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrTopicNotFound), errors.Is(err, storage.ErrPartitionNotFound):
			writeErr(w, http.StatusNotFound, err.Error())
		case errors.Is(err, storage.ErrOffsetOutOfRange):
			writeErr(w, http.StatusBadRequest, err.Error())
		default:
			writeErr(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	next := offset
	out := make([]fetchRecord, 0, len(recs))
	for _, rec := range recs {
		out = append(out, fetchRecord{
			Offset: int64(rec.Offset), TimestampNS: rec.TimestampNS, Key: rec.Key, Value: rec.Value,
		})
		next = int64(rec.Offset) + 1
	}
	w.Header().Set("X-Marchiq-Records-Returned", strconv.Itoa(len(out)))
	w.Header().Set("X-Marchiq-High-Watermark", strconv.FormatInt(int64(offs.Latest), 10))
	writeJSON(w, http.StatusOK, map[string]any{
		"topic": topic, "partition": partition,
		"records": out, "next_offset": next, "high_watermark": int64(offs.Latest),
	})
}

// fetchGroup serves GET /fetch?group=G&topic=T[&member=M]: records from
// committed_offset+1 on every partition statically assigned to the member.
func fetchGroup(api storage.BrokerAPI, w http.ResponseWriter, topic, group, member string, maxRecords int, maxBytes int64) {
	if topic == "" {
		writeErr(w, http.StatusBadRequest, "topic query param is required")
		return
	}
	parts, err := api.FetchGroup(group, topic, member, maxRecords, maxBytes)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrGroupNotFound), errors.Is(err, storage.ErrTopicNotFound):
			writeErr(w, http.StatusNotFound, err.Error())
		case errors.Is(err, storage.ErrMemberRequired):
			writeErr(w, http.StatusBadRequest, err.Error())
		default:
			writeErr(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	type partitionResult struct {
		Partition     int           `json:"partition"`
		Records       []fetchRecord `json:"records"`
		NextOffset    int64         `json:"next_offset"`
		HighWatermark int64         `json:"high_watermark"`
	}
	out := make([]partitionResult, 0, len(parts))
	total := 0
	for _, p := range parts {
		recs := make([]fetchRecord, 0, len(p.Records))
		for _, rec := range p.Records {
			recs = append(recs, fetchRecord{
				Offset: int64(rec.Offset), TimestampNS: rec.TimestampNS, Key: rec.Key, Value: rec.Value,
			})
		}
		total += len(recs)
		out = append(out, partitionResult{
			Partition: p.Partition, Records: recs,
			NextOffset: int64(p.NextOffset), HighWatermark: int64(p.HighWatermark),
		})
	}
	w.Header().Set("X-Marchiq-Records-Returned", strconv.Itoa(total))
	writeJSON(w, http.StatusOK, map[string]any{
		"group": group, "member": member, "topic": topic, "partitions": out,
	})
}

// joinGroup serves POST /groups/{group}/join?topic=T&member=M&members=N.
// members (declared group size) defaults to 1 and is fixed by the first join;
// member defaults to a generated id. Rejoining is idempotent.
func joinGroup(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		topic := q.Get("topic")
		if topic == "" {
			writeErr(w, http.StatusBadRequest, "topic query param is required")
			return
		}
		size := 1
		if s := q.Get("members"); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v < 1 {
				writeErr(w, http.StatusBadRequest, "members must be a positive integer")
				return
			}
			size = v
		}
		asg, err := api.JoinGroup(r.PathValue("group"), topic, q.Get("member"), size)
		if err != nil {
			switch {
			case errors.Is(err, storage.ErrGroupConflict):
				writeErr(w, http.StatusConflict, err.Error())
			case errors.Is(err, storage.ErrTopicNotFound):
				writeErr(w, http.StatusNotFound, err.Error())
			default:
				writeErr(w, http.StatusBadRequest, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, asg)
	}
}

type commitRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// commitOffset serves POST /commit: the offset is the last PROCESSED record;
// the next group fetch resumes at offset+1 (at-least-once).
func commitOffset(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req commitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if req.Group == "" || req.Topic == "" {
			writeErr(w, http.StatusBadRequest, "group and topic are required")
			return
		}
		if err := api.CommitOffset(req.Group, req.Topic, req.Partition, storage.Offset(req.Offset)); err != nil {
			switch {
			case errors.Is(err, storage.ErrGroupNotFound),
				errors.Is(err, storage.ErrTopicNotFound), errors.Is(err, storage.ErrPartitionNotFound):
				writeErr(w, http.StatusNotFound, err.Error())
			case errors.Is(err, storage.ErrOffsetOutOfRange):
				writeErr(w, http.StatusBadRequest, err.Error())
			default:
				writeErr(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"group": req.Group, "topic": req.Topic, "partition": req.Partition,
			"committed": req.Offset, "next_offset": req.Offset + 1,
		})
	}
}

func debugSegments(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		topic, partStr := q.Get("topic"), q.Get("partition")
		if topic == "" || partStr == "" {
			writeErr(w, http.StatusBadRequest, "topic and partition query params are required")
			return
		}
		partition, err := strconv.Atoi(partStr)
		if err != nil || partition < 0 {
			writeErr(w, http.StatusBadRequest, "partition must be a non-negative integer")
			return
		}
		segs, err := api.DescribeSegments(topic, partition)
		if err != nil {
			if errors.Is(err, storage.ErrTopicNotFound) || errors.Is(err, storage.ErrPartitionNotFound) {
				writeErr(w, http.StatusNotFound, err.Error())
			} else {
				writeErr(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"topic": topic, "partition": partition, "segments": segs})
	}
}

// debugLag serves GET /debug/lag[?group=G]: every joined (group, topic,
// partition) cursor against the current LEO. An unknown group filter is a
// 404, not an empty report — a typo'd group name must not look like "no lag".
func debugLag(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lags, err := api.GroupLag(r.URL.Query().Get("group"))
		if err != nil {
			if errors.Is(err, storage.ErrGroupNotFound) {
				writeErr(w, http.StatusNotFound, err.Error())
			} else {
				writeErr(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"groups": lags})
	}
}

// debugStats serves GET /debug/stats: process-lifetime counters (reset at
// broker start, never persisted).
func debugStats(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, api.Stats())
	}
}

type produceResponse struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Offset      int64  `json:"offset"`
	TimestampNS int64  `json:"timestamp_ns"`
}

func produce(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		topic, partStr := q.Get("topic"), q.Get("partition")
		if topic == "" || partStr == "" {
			writeErr(w, http.StatusBadRequest, "topic and partition query params are required")
			return
		}
		partition, err := strconv.Atoi(partStr)
		if err != nil || partition < -1 {
			writeErr(w, http.StatusBadRequest, "partition must be -1 (broker picks: key hash, else round-robin) or a non-negative integer")
			return
		}
		acks := storage.AcksLeader
		if s := q.Get("acks"); s != "" {
			a, err := storage.ParseAcks(s)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			acks = a
		}
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, storage.MaxRecordBytes+1))
		if err != nil {
			writeErr(w, http.StatusRequestEntityTooLarge, "value exceeds 1 MiB frame limit")
			return
		}
		rec, chosen, err := api.Produce(topic, partition, []byte(q.Get("key")), value, acks)
		if err != nil {
			switch {
			case errors.Is(err, storage.ErrTopicNotFound), errors.Is(err, storage.ErrPartitionNotFound):
				writeErr(w, http.StatusNotFound, err.Error())
			case errors.Is(err, storage.ErrRecordTooLarge):
				writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
			default:
				writeErr(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, produceResponse{
			Topic: topic, Partition: chosen, Offset: int64(rec.Offset), TimestampNS: rec.TimestampNS,
		})
	}
}

func run() error {
	dataDir := flag.String("dataDir", "./data", "broker data directory")
	addr := flag.String("addr", ":9092", "HTTP listen address")
	segmentBytes := flag.Int64("segmentBytes", storage.DefaultMaxSegmentBytes, "roll active segment beyond this size")
	indexInterval := flag.Int64("indexIntervalBytes", storage.DefaultIndexIntervalBytes, "sparse index entry per this many bytes")
	retentionCheck := flag.Duration("retentionCheckInterval", storage.DefaultRetentionCheckInterval, "how often the retention janitor deletes expired sealed segments")
	flag.Parse()
	broker, err := storage.OpenWithConfig(*dataDir, *segmentBytes, *indexInterval)
	if err != nil {
		return err
	}
	defer broker.Close()
	broker.SetRetentionInterval(*retentionCheck)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: *addr, Handler: handler(broker), ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Printf("marchiq listening on %s (dataDir=%s)", *addr, *dataDir)
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
