package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRecordGolden(t *testing.T) {
	buf, err := EncodeRecord(Record{Offset: 99, TimestampNS: 1, Key: []byte("k"), Value: []byte("v")})
	if err != nil { t.Fatal(err) }
	// Version, body length=18, timestamp=1, key length/key, value length/value.
	const want = "01000000120000000000000001000000016b0000000176"
	if hex.EncodeToString(buf) != want { t.Fatalf("frame: %x", buf) }
	r, n, err := DecodeRecord(bytes.NewReader(buf), 7)
	if err != nil || n != len(buf) || r.Offset != 7 || r.TimestampNS != 1 || string(r.Key) != "k" || string(r.Value) != "v" {
		t.Fatalf("record=%+v n=%d err=%v", r, n, err)
	}
}

func TestEveryTruncatedBoundary(t *testing.T) {
	buf, _ := EncodeRecord(Record{Value: []byte("hello")})
	for end := 0; end < len(buf); end++ {
		_, n, err := DecodeRecord(bytes.NewReader(buf[:end]), 0)
		want := io.ErrUnexpectedEOF
		if end == 0 { want = io.EOF }
		if !errors.Is(err, want) || n != end { t.Fatalf("end=%d n=%d err=%v", end, n, err) }
	}
}

func TestInvalidFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		modify func([]byte)
		want error
	}{
		{"magic", func(b []byte) { b[0] = 2 }, ErrCorruptRecord},
		{"short body", func(b []byte) { b[4] = 15 }, ErrCorruptRecord},
		{"huge body", func(b []byte) { b[1] = 0xff }, ErrRecordTooLarge},
		{"huge key", func(b []byte) { b[13] = 0xff }, ErrCorruptRecord},
		{"value mismatch", func(b []byte) { b[20] = 1 }, ErrCorruptRecord},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, _ := EncodeRecord(Record{})
			tc.modify(buf)
			_, _, err := DecodeRecord(bytes.NewReader(buf), 0)
			if !errors.Is(err, tc.want) { t.Fatalf("err=%v", err) }
		})
	}
}

func TestRecordSizeLimit(t *testing.T) {
	value := make([]byte, MaxRecordBytes-framePrefix-minBodyBytes)
	buf, err := EncodeRecord(Record{Value: value})
	if err != nil || len(buf) != MaxRecordBytes { t.Fatalf("len=%d err=%v", len(buf), err) }
	r, _, err := DecodeRecord(bytes.NewReader(buf), 0)
	if err != nil || !bytes.Equal(r.Value, value) { t.Fatal("maximum frame failed", err) }
	if _, err := EncodeRecord(Record{Key: []byte("x"), Value: value}); !errors.Is(err, ErrRecordTooLarge) { t.Fatal(err) }
}

// Codec-level file scan only: this does NOT claim Produce/GetOffsets exist yet.
func TestThreeRecordsOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), segmentName(0))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil { t.Fatal(err) }
	defer f.Close()
	for _, value := range []string{"one", "two", "three"} {
		frame, err := EncodeRecord(Record{TimestampNS: 123, Value: []byte(value)})
		if err != nil { t.Fatal(err) }
		if n, err := f.Write(frame); err != nil || n != len(frame) { t.Fatalf("write n=%d err=%v", n, err) }
	}
	if err := f.Sync(); err != nil { t.Fatal(err) }
	reader := io.NewSectionReader(f, 0, 1<<63-1)
	var next Offset
	for _, want := range []string{"one", "two", "three"} {
		r, _, err := DecodeRecord(reader, next)
		if err != nil || r.Offset != next || string(r.Value) != want { t.Fatalf("%+v %v", r, err) }
		next++
	}
	if _, _, err := DecodeRecord(reader, next); !errors.Is(err, io.EOF) { t.Fatal(err) }
	if next != 3 { t.Fatalf("LEO=%d", next) }
}

func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(int64(1), []byte("k"), []byte("v"))
	f.Add(int64(-1), []byte{}, []byte{})
	f.Fuzz(func(t *testing.T, ts int64, key, value []byte) {
		buf, err := EncodeRecord(Record{TimestampNS: ts, Key: key, Value: value})
		if errors.Is(err, ErrRecordTooLarge) { return }
		if err != nil { t.Fatal(err) }
		r, n, err := DecodeRecord(bytes.NewReader(buf), 42)
		if err != nil || n != len(buf) || r.Offset != 42 || r.TimestampNS != ts || !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, value) {
			t.Fatalf("round trip failed: %v", err)
		}
	})
}

func FuzzDecodeRecord(f *testing.F) {
	buf, _ := EncodeRecord(Record{Value: []byte("hello")})
	f.Add(buf)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, buf []byte) {
		_, n, _ := DecodeRecord(bytes.NewReader(buf), 0)
		if n < 0 || n > len(buf) { t.Fatalf("consumed %d of %d", n, len(buf)) }
	})
}
