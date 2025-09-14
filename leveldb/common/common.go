package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode"
)

// Record is an interface for common fields between record types,
// allowing them to be processed generically.
type Record interface {
	GetSequenceNumber() uint64
	GetKey() []byte
	GetOffset() int64
}

// KeyValueRecord corresponds to the KeyValueRecord class in ldb.py
type KeyValueRecord struct {
	Offset         int64  `json:"offset"`
	Key            []byte `json:"-"` // Omit raw bytes from default JSON
	Value          []byte `json:"-"`
	SequenceNumber uint64 `json:"sequence_number"`
	RecordType     byte   `json:"record_type"`
}

// GetSequenceNumber returns the sequence number.
func (r *KeyValueRecord) GetSequenceNumber() uint64 { return r.SequenceNumber }

// GetKey returns the key.
func (r *KeyValueRecord) GetKey() []byte { return r.Key }

// GetOffset returns the file offset.
func (r *KeyValueRecord) GetOffset() int64 { return r.Offset }

// ParsedInternalKey corresponds to the ParsedInternalKey class in log.py
type ParsedInternalKey struct {
	Offset         int64  `json:"offset"`
	RecordType     byte   `json:"record_type"`
	SequenceNumber uint64 `json:"sequence_number"`
	Key            []byte `json:"-"`
	Value          []byte `json:"-"`
}

// GetSequenceNumber returns the sequence number.
func (r *ParsedInternalKey) GetSequenceNumber() uint64 { return r.SequenceNumber }

// GetKey returns the key.
func (r *ParsedInternalKey) GetKey() []byte { return r.Key }

// GetOffset returns the file offset.
func (r *ParsedInternalKey) GetOffset() int64 { return r.Offset }

// JSONRecord is used for custom JSON output with escaped strings.
// This provides a clean and readable representation of record data.
type JSONRecord struct {
	Offset         int64  `json:"offset"`
	RecordType     byte   `json:"record_type"`
	SequenceNumber uint64 `json:"sequence_number"`
	Key            string `json:"key"`
	Value          string `json:"value"`
}

// ToJSONRecord converts any Record type to a JSON-friendly format.
func ToJSONRecord(rec Record) JSONRecord {
	var recordType byte
	var value []byte

	// Type switch to handle different underlying record structs.
	switch r := rec.(type) {
	case *KeyValueRecord:
		recordType = r.RecordType
		value = r.Value
	case *ParsedInternalKey:
		recordType = r.RecordType
		value = r.Value
	}

	return JSONRecord{
		Offset:         rec.GetOffset(),
		RecordType:     recordType,
		SequenceNumber: rec.GetSequenceNumber(),
		Key:            BytesToEscapedString(rec.GetKey()),
		Value:          BytesToEscapedString(value),
	}
}

// BytesToEscapedString converts a byte slice to a string, escaping non-printable characters.
func BytesToEscapedString(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		// Keep printable characters as-is, except for backslash.
		if unicode.IsPrint(rune(c)) && c != '\\' {
			sb.WriteByte(c)
		} else {
			// Escape non-printable characters and backslash.
			sb.WriteString(fmt.Sprintf("\\x%02x", c))
		}
	}
	return sb.String()
}

// LevelDBDecoder is a helper to decode data types from a stream.
type LevelDBDecoder struct {
	r   io.ReaderAt
	pos int64
}

// NewLevelDBDecoder creates a new decoder.
func NewLevelDBDecoder(r io.ReaderAt) *LevelDBDecoder {
	return &LevelDBDecoder{r: r}
}

// Pos returns the current position of the decoder.
func (d *LevelDBDecoder) Pos() int64 {
	return d.pos
}

// SetPos sets the current position of the decoder.
func (d *LevelDBDecoder) SetPos(pos int64) {
	d.pos = pos
}

// Seek sets the decoder's internal position. It currently only supports io.SeekStart.
func (d *LevelDBDecoder) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
		d.pos = offset
		return d.pos, nil
	}
	return d.pos, fmt.Errorf("unsupported seek whence: %d", whence)
}

type byteReader struct {
	r   io.ReaderAt
	pos *int64
}

func (br byteReader) ReadByte() (byte, error) {
	var b [1]byte
	n, err := br.r.ReadAt(b[:], *br.pos)
	if n > 0 {
		(*br.pos)++
	}
	return b[0], err
}

// ReadBytes reads n bytes from the current position.
func (d *LevelDBDecoder) ReadBytes(n int) (int64, []byte, error) {
	startPos := d.pos
	buf := make([]byte, n)
	sr := io.NewSectionReader(d.r, d.pos, int64(n))
	read, err := io.ReadFull(sr, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return startPos, nil, err
	}
	d.pos += int64(read)
	return startPos, buf[:read], nil
}

// DecodeUint64Varint decodes a variable-length unsigned 64-bit integer.
func (d *LevelDBDecoder) DecodeUint64Varint() (int64, uint64, error) {
	startPos := d.pos
	val, err := binary.ReadUvarint(byteReader{d.r, &d.pos})
	return startPos, val, err
}

// DecodeUint32Varint decodes a variable-length unsigned 32-bit integer.
func (d *LevelDBDecoder) DecodeUint32Varint() (int64, uint32, error) {
	startPos := d.pos
	val, err := binary.ReadUvarint(byteReader{d.r, &d.pos})
	return startPos, uint32(val), err
}

// DecodeUint64 decodes a fixed-size unsigned 64-bit integer.
func (d *LevelDBDecoder) DecodeUint64() (int64, uint64, error) {
	startPos := d.pos
	_, buf, err := d.ReadBytes(8)
	if err != nil {
		return startPos, 0, err
	}
	if len(buf) < 8 {
		return startPos, 0, io.ErrUnexpectedEOF
	}
	return startPos, binary.LittleEndian.Uint64(buf), nil
}

// DecodeUint32 decodes a fixed-size unsigned 32-bit integer.
func (d *LevelDBDecoder) DecodeUint32() (int64, uint32, error) {
	startPos := d.pos
	_, buf, err := d.ReadBytes(4)
	if err != nil {
		return startPos, 0, err
	}
	if len(buf) < 4 {
		return startPos, 0, io.ErrUnexpectedEOF
	}
	return startPos, binary.LittleEndian.Uint32(buf), nil
}

// DecodeUint16 decodes a fixed-size unsigned 16-bit integer.
func (d *LevelDBDecoder) DecodeUint16() (int64, uint16, error) {
	startPos := d.pos
	_, buf, err := d.ReadBytes(2)
	if err != nil {
		return startPos, 0, err
	}
	if len(buf) < 2 {
		return startPos, 0, io.ErrUnexpectedEOF
	}
	return startPos, binary.LittleEndian.Uint16(buf), nil
}

// DecodeUint8 decodes a single byte.
func (d *LevelDBDecoder) DecodeUint8() (int64, uint8, error) {
	startPos := d.pos
	_, buf, err := d.ReadBytes(1)
	if err != nil {
		return startPos, 0, err
	}
	if len(buf) < 1 {
		return startPos, 0, io.ErrUnexpectedEOF
	}
	return startPos, buf[0], nil
}

// DecodeBlobWithLength decodes a blob prefixed with its varint length.
func (d *LevelDBDecoder) DecodeBlobWithLength() (int64, []byte, error) {
	offset, numBytes, err := d.DecodeUint64Varint()
	if err != nil {
		return 0, nil, err
	}
	_, blob, err := d.ReadBytes(int(numBytes))
	return offset, blob, err
}
