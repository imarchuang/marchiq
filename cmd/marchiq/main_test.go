package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marchi/marchiq/storage"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	b, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return handler(b)
}

func TestHandler(t *testing.T) {
	h := testHandler(t)
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/healthz", 200}, {"GET", "/", 200},
		{"GET", "/missing", 404}, {"POST", "/healthz", 405},
		{"POST", "/produce", 400}, // registered in Slice 1; params missing
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
		if tc.path == "/healthz" && tc.method == "GET" && w.Body.String() != "ok\n" {
			t.Fatal(w.Body.String())
		}
	}
}

func TestTopicsAPI(t *testing.T) {
	h := testHandler(t)
	create := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/topics", strings.NewReader(body)))
		return w
	}
	if w := create(`{"name":"events","partitions":2}`); w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	if w := create(`{"name":"events","partitions":2}`); w.Code != 409 {
		t.Fatalf("duplicate: %d %s", w.Code, w.Body)
	}
	if w := create(`{"name":"bad/name","partitions":1}`); w.Code != 400 {
		t.Fatalf("invalid: %d %s", w.Code, w.Body)
	}
	if w := create(`{`); w.Code != 400 {
		t.Fatalf("bad JSON: %d %s", w.Code, w.Body)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/topics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"events"`) {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
}

// Slice 5: POST /topics accepts retention_ms / retention_bytes and echoes
// them; negative values are rejected.
func TestCreateTopicWithRetentionAPI(t *testing.T) {
	h := testHandler(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/topics",
		strings.NewReader(`{"name":"events","partitions":1,"retention_ms":3600000,"retention_bytes":1073741824}`)))
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	var cfg storage.TopicConfig
	if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.RetentionMS != 3600000 || cfg.RetentionBytes != 1073741824 {
		t.Fatalf("%+v", cfg)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/topics", nil))
	if !strings.Contains(w.Body.String(), `"retention_ms":3600000`) {
		t.Fatalf("list: %s", w.Body)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/topics",
		strings.NewReader(`{"name":"neg","partitions":1,"retention_ms":-5}`)))
	if w.Code != 400 {
		t.Fatalf("negative retention: %d %s", w.Code, w.Body)
	}
}

func TestProduceAndOffsetsAPI(t *testing.T) {
	h := testHandler(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/topics", strings.NewReader(`{"name":"events","partitions":2}`)))
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	produce := func(path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(body)))
		return w
	}
	for i, want := range []int64{0, 1} {
		w := produce("/produce?topic=events&partition=0&key=k", "payload")
		if w.Code != 200 {
			t.Fatalf("produce: %d %s", w.Code, w.Body)
		}
		var resp produceResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Offset != want {
			t.Fatalf("produce %d: %+v %v", i, resp, err)
		}
	}
	if w := produce("/produce?topic=ghost&partition=0", "x"); w.Code != 404 {
		t.Fatalf("unknown topic: %d", w.Code)
	}
	if w := produce("/produce?topic=events&partition=5", "x"); w.Code != 404 {
		t.Fatalf("unknown partition: %d", w.Code)
	}
	if w := produce("/produce?topic=events", "x"); w.Code != 400 {
		t.Fatalf("missing partition: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/topics/events/offsets", nil))
	if w.Code != 200 {
		t.Fatalf("offsets: %d %s", w.Code, w.Body)
	}
	var offsets struct {
		Partitions []partitionOffsets `json:"partitions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &offsets); err != nil {
		t.Fatal(err)
	}
	if len(offsets.Partitions) != 2 || offsets.Partitions[0].Latest != 2 || offsets.Partitions[1].Latest != 0 {
		t.Fatalf("%+v", offsets)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/topics/ghost/offsets", nil))
	if w.Code != 404 {
		t.Fatalf("unknown topic offsets: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/debug/segments?topic=events&partition=0", nil))
	if w.Code != 200 {
		t.Fatalf("debug segments: %d %s", w.Code, w.Body)
	}
	var dbg struct {
		Segments []storage.SegmentInfo `json:"segments"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &dbg); err != nil {
		t.Fatal(err)
	}
	if len(dbg.Segments) != 1 || dbg.Segments[0].BaseOffset != 0 || dbg.Segments[0].Records != 2 || dbg.Segments[0].SizeBytes == 0 {
		t.Fatalf("%+v", dbg)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/debug/segments?topic=ghost&partition=0", nil))
	if w.Code != 404 {
		t.Fatalf("unknown topic segments: %d", w.Code)
	}
}

// Slice 3 acceptance: produce 100 messages, fetch from offset 50, get 50.
func TestFetchAPI(t *testing.T) {
	h := testHandler(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/topics", strings.NewReader(`{"name":"events","partitions":1}`)))
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/produce?topic=events&partition=0", strings.NewReader(fmt.Sprintf("payload-%d", i))))
		if w.Code != 200 {
			t.Fatalf("produce %d: %d", i, w.Code)
		}
	}
	fetch := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	w = fetch("/fetch?topic=events&partition=0&offset=50")
	if w.Code != 200 {
		t.Fatalf("fetch: %d %s", w.Code, w.Body)
	}
	var resp struct {
		Records       []fetchRecord `json:"records"`
		NextOffset    int64         `json:"next_offset"`
		HighWatermark int64         `json:"high_watermark"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 50 || resp.Records[0].Offset != 50 || resp.NextOffset != 100 || resp.HighWatermark != 100 {
		t.Fatalf("records=%d next=%d hwm=%d", len(resp.Records), resp.NextOffset, resp.HighWatermark)
	}
	if got := w.Header().Get("X-Marchiq-Records-Returned"); got != "50" {
		t.Fatalf("header=%s", got)
	}
	// On the wire the value is base64 (JSON []byte); the struct above already
	// holds the decoded bytes.
	if string(resp.Records[0].Value) != "payload-50" {
		t.Fatalf("value %q", resp.Records[0].Value)
	}
	if !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString([]byte("payload-50"))) {
		t.Fatalf("wire value not base64: %s", w.Body.String())
	}
	// Empty poll at LEO: 200 with records: [].
	w = fetch("/fetch?topic=events&partition=0&offset=100")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"records":[]`) {
		t.Fatalf("empty poll: %d %s", w.Code, w.Body)
	}
	// Out of range / unknown topic / missing params.
	if w := fetch("/fetch?topic=events&partition=0&offset=101"); w.Code != 400 {
		t.Fatalf("out of range: %d", w.Code)
	}
	if w := fetch("/fetch?topic=ghost&partition=0&offset=0"); w.Code != 404 {
		t.Fatalf("unknown topic: %d", w.Code)
	}
	if w := fetch("/fetch?topic=events&partition=0"); w.Code != 400 {
		t.Fatalf("missing offset: %d", w.Code)
	}
	// max_bytes caps the response.
	w = fetch("/fetch?topic=events&partition=0&offset=0&max_bytes=100")
	if w.Code != 200 {
		t.Fatalf("max_bytes: %d", w.Code)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) == 0 || len(resp.Records) >= 10 {
		t.Fatalf("max_bytes=100 returned %d records", len(resp.Records))
	}
}
