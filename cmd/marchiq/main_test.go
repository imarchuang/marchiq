package main

import (
	"net/http/httptest"
	"testing"
)

func TestHandler(t *testing.T) {
	for _, tc := range []struct { method, path string; status int }{
		{"GET", "/healthz", 200}, {"GET", "/", 200},
		{"GET", "/missing", 404}, {"POST", "/healthz", 405},
		{"POST", "/produce", 404},
	} {
		w := httptest.NewRecorder()
		handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status { t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code) }
		if tc.path == "/healthz" && tc.method == "GET" && w.Body.String() != "ok\n" { t.Fatal(w.Body.String()) }
	}
}
