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
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/marchi/marchiq/storage"
)

const helpText = `marchiq — Slice 3: fetch by explicit offset
POST /topics                      create topic, JSON body {"name":"events","partitions":2}
GET  /topics                      list topics
GET  /topics/{topic}/offsets      earliest/latest per partition
POST /produce?topic=T&partition=P&key=K   append record; request body is the value
GET  /fetch?topic=T&partition=P&offset=O&max_bytes=B&max_records=N
                                  read records from offset O; key/value are base64 in JSON
GET  /debug/segments?topic=T&partition=P  list segment files + sizes
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
	mux.HandleFunc("GET /debug/segments", debugSegments(api))
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

func fetch(api storage.BrokerAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		topic, partStr, offStr := q.Get("topic"), q.Get("partition"), q.Get("offset")
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
		maxBytes := int64(1 << 20) // 1 MiB default fetch budget
		if s := q.Get("max_bytes"); s != "" {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil || v <= 0 {
				writeErr(w, http.StatusBadRequest, "max_bytes must be a positive integer")
				return
			}
			maxBytes = v
		}
		maxRecords := 1000
		if s := q.Get("max_records"); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v <= 0 {
				writeErr(w, http.StatusBadRequest, "max_records must be a positive integer")
				return
			}
			maxRecords = v
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
		if err != nil || partition < 0 {
			writeErr(w, http.StatusBadRequest, "partition must be a non-negative integer (round-robin -1 is Slice 6)")
			return
		}
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, storage.MaxRecordBytes+1))
		if err != nil {
			writeErr(w, http.StatusRequestEntityTooLarge, "value exceeds 1 MiB frame limit")
			return
		}
		rec, err := api.Produce(topic, partition, []byte(q.Get("key")), value)
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
			Topic: topic, Partition: partition, Offset: int64(rec.Offset), TimestampNS: rec.TimestampNS,
		})
	}
}

func run() error {
	dataDir := flag.String("dataDir", "./data", "broker data directory")
	addr := flag.String("addr", ":9092", "HTTP listen address")
	segmentBytes := flag.Int64("segmentBytes", storage.DefaultMaxSegmentBytes, "roll active segment beyond this size")
	indexInterval := flag.Int64("indexIntervalBytes", storage.DefaultIndexIntervalBytes, "sparse index entry per this many bytes")
	flag.Parse()
	broker, err := storage.OpenWithConfig(*dataDir, *segmentBytes, *indexInterval)
	if err != nil {
		return err
	}
	defer broker.Close()
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
