package main

import (
	"encoding/json"
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
}
