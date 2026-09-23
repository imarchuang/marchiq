package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	recordMagic    byte = 1 // v1 format discriminator, NOT a checksum
	framePrefix        = 5 // magic(1) + body_len(4)
	minBodyBytes       = 16 // timestamp_ns(8) + key_len(4) + value_len(4)
	MaxRecordBytes     = 1 << 20 // hard limit including prefix, before allocating
)

var (
	ErrCorruptRecord  = errors.New("invalid record format")
	ErrRecordTooLarge = errors.New("record exceeds 1 MiB frame limit")
)

// EncodeRecord returns a complete v1 frame. Offset is intentionally ignored.
func EncodeRecord(r Record) ([]byte, error) {
	// Check each length before addition, avoiding int overflow.
	if len(r.Key) > MaxRecordBytes || len(r.Value) > MaxRecordBytes ||
		len(r.Key)+len(r.Value) > MaxRecordBytes-framePrefix-minBodyBytes {
		return nil, ErrRecordTooLarge
	}
	bodyLen := minBodyBytes + len(r.Key) + len(r.Value)
	buf := make([]byte, framePrefix+bodyLen)
	buf[0] = recordMagic
	binary.BigEndian.PutUint32(buf[1:5], uint32(bodyLen))
	binary.BigEndian.PutUint64(buf[5:13], uint64(r.TimestampNS))
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(r.Key)))
	pos := 17 + copy(buf[17:], r.Key)
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(r.Value)))
	copy(buf[pos+4:], r.Value)
	return buf, nil
}

// DecodeRecord decodes one frame using the offset supplied by the scanner.
// n is bytes CONSUMED, including on errors. Recovery tracks lastGood separately.
// io.EOF means a clean frame boundary; io.ErrUnexpectedEOF means a partial frame.
// A malformed complete frame fails closed: never skip it or silently truncate.
func DecodeRecord(src io.Reader, offset Offset) (record Record, n int, err error) {
	var header [framePrefix]byte
	n, err = io.ReadFull(src, header[:])
	if err != nil {
		return Record{}, n, err
	}
	if header[0] != recordMagic {
		return Record{}, n, fmt.Errorf("%w: magic/version %d", ErrCorruptRecord, header[0])
	}
	bodyLen := binary.BigEndian.Uint32(header[1:5])
	if bodyLen < minBodyBytes {
		return Record{}, n, fmt.Errorf("%w: body too short", ErrCorruptRecord)
	}
	if bodyLen > MaxRecordBytes-framePrefix {
		return Record{}, n, ErrRecordTooLarge
	}
	body := make([]byte, int(bodyLen))
	read, err := io.ReadFull(src, body)
	n += read
	if err != nil {
		// Even zero body bytes is a partial FRAME, not a clean EOF.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return Record{}, n, err
	}
	keyLen := binary.BigEndian.Uint32(body[8:12])
	if keyLen > bodyLen-minBodyBytes {
		return Record{}, n, fmt.Errorf("%w: key length", ErrCorruptRecord)
	}
	valuePos := 12 + int(keyLen)
	valueLen := binary.BigEndian.Uint32(body[valuePos:valuePos+4])
	if valueLen != bodyLen-uint32(valuePos)-4 {
		return Record{}, n, fmt.Errorf("%w: value length", ErrCorruptRecord)
	}
	return Record{
		Offset: offset,
		TimestampNS: int64(binary.BigEndian.Uint64(body[:8])),
		Key: body[12:valuePos],
		Value: body[valuePos+4:],
	}, n, nil
}
