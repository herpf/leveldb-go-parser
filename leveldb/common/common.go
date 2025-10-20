package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode"

	xunicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Record is an interface for common fields between record types,
// allowing them to be processed generically.
type Record interface {
	GetSequenceNumber() uint64
	GetKey() []byte
	GetValue() []byte
	GetType() byte
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

func (r *KeyValueRecord) GetSequenceNumber() uint64 { return r.SequenceNumber }
func (r *KeyValueRecord) GetKey() []byte            { return r.Key }
func (r *KeyValueRecord) GetValue() []byte          { return r.Value }
func (r *KeyValueRecord) GetType() byte             { return r.RecordType }
func (r *KeyValueRecord) GetOffset() int64          { return r.Offset }

// ParsedInternalKey corresponds to the ParsedInternalKey class in log.py
type ParsedInternalKey struct {
	Offset         int64  `json:"offset"`
	RecordType     byte   `json:"record_type"`
	SequenceNumber uint64 `json:"sequence_number"`
	Key            []byte `json:"-"`
	Value          []byte `json:"-"`
	Recovered      bool   `json:"-"`
}

func (r *ParsedInternalKey) GetSequenceNumber() uint64 { return r.SequenceNumber }
func (r *ParsedInternalKey) GetKey() []byte            { return r.Key }
func (r *ParsedInternalKey) GetValue() []byte          { return r.Value }      // Added method
func (r *ParsedInternalKey) GetType() byte             { return r.RecordType } // Added method
func (r *ParsedInternalKey) GetOffset() int64          { return r.Offset }

// JSONRecord is used for custom JSON output with escaped strings.
type JSONRecord struct {
	Offset         int64  `json:"offset"`
	RecordType     byte   `json:"record_type"`
	SequenceNumber uint64 `json:"sequence_number"`
	Key            string `json:"key"`
	Value          string `json:"value"`
}

// ToJSONRecord converts any Record type to a JSON-friendly format.
func ToJSONRecord(rec Record) JSONRecord {
	return JSONRecord{
		Offset:         rec.GetOffset(),
		RecordType:     rec.GetType(),
		SequenceNumber: rec.GetSequenceNumber(),
		Key:            BytesToEscapedString(rec.GetKey()),
		Value:          BytesToEscapedString(rec.GetValue()),
	}
}

// BytesToEscapedString converts a byte slice to a string, escaping non-printable characters.
func BytesToEscapedString(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		if unicode.IsPrint(rune(c)) && c != '\\' {
			sb.WriteByte(c)
		} else {
			sb.WriteString(fmt.Sprintf("\\x%02x", c))
		}
	}
	return sb.String()
}

// LevelDBDecoder is a helper to decode data types from a stream.
type LevelDBDecoder struct {
	reader io.ReadSeeker
}

// NewLevelDBDecoder creates a new decoder.
func NewLevelDBDecoder(r io.ReadSeeker) *LevelDBDecoder {
	return &LevelDBDecoder{reader: r}
}

// GetReader exposes the underlying reader.
func (d *LevelDBDecoder) GetReader() io.Reader {
	return d.reader
}

// Offset returns the current reading position.
func (d *LevelDBDecoder) Offset() int64 {
	pos, _ := d.reader.Seek(0, io.SeekCurrent)
	return pos
}

// Seek sets the decoder's internal position.
func (d *LevelDBDecoder) Seek(offset int64, whence int) (int64, error) {
	return d.reader.Seek(offset, whence)
}

// ReadBytes reads n bytes from the current position.
func (d *LevelDBDecoder) ReadBytes(n int) (int64, []byte, error) {
	startPos := d.Offset()
	buf := make([]byte, n)
	read, err := io.ReadFull(d.reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return startPos, nil, err
	}
	return startPos, buf[:read], nil
}

func (d *LevelDBDecoder) DecodeUint64() (int64, uint64, error) {
	startPos := d.Offset()
	var val uint64
	err := binary.Read(d.reader, binary.LittleEndian, &val)
	return startPos, val, err
}

func (d *LevelDBDecoder) DecodeUint32() (int64, uint32, error) {
	startPos := d.Offset()
	var val uint32
	err := binary.Read(d.reader, binary.LittleEndian, &val)
	return startPos, val, err
}

func (d *LevelDBDecoder) DecodeUint64BE() (int64, uint64, error) {
	startPos := d.Offset()
	var val uint64
	err := binary.Read(d.reader, binary.BigEndian, &val)
	return startPos, val, err
}

func (d *LevelDBDecoder) DecodeUint32BE() (int64, uint32, error) {
	startPos := d.Offset()
	var val uint32
	err := binary.Read(d.reader, binary.BigEndian, &val)
	return startPos, val, err
}

func (d *LevelDBDecoder) DecodeUint16() (int64, uint16, error) {
	startPos := d.Offset()
	var val uint16
	err := binary.Read(d.reader, binary.LittleEndian, &val)
	return startPos, val, err
}

func (d *LevelDBDecoder) DecodeUint8() (int64, uint8, error) {
	startPos := d.Offset()
	var val uint8
	err := binary.Read(d.reader, binary.LittleEndian, &val)
	return startPos, val, err
}

// DecodeInt reads a little-endian integer of a specific byte count.
func (d *LevelDBDecoder) DecodeInt(byteCount int) (int64, uint64, error) {
	startPos := d.Offset()
	if byteCount > 8 || byteCount < 1 {
		return startPos, 0, fmt.Errorf("invalid byte count for DecodeInt: %d", byteCount)
	}
	_, data, err := d.ReadBytes(byteCount)
	if err != nil {
		return startPos, 0, err
	}

	paddedData := make([]byte, 8)
	copy(paddedData, data)
	val := binary.LittleEndian.Uint64(paddedData)
	return startPos, val, nil
}

// DecodeDouble reads a 64-bit float.
func (d *LevelDBDecoder) DecodeDouble() (int64, float64, error) {
	startPos := d.Offset()
	var val float64
	err := binary.Read(d.reader, binary.LittleEndian, &val)
	return d.Offset() - startPos, val, err
}

// simpleByteReader is a helper to adapt an io.Reader to an io.ByteReader
type simpleByteReader struct{ io.Reader }

func (sbr simpleByteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := sbr.Read(b[:])
	return b[0], err
}

// DecodeVarint reads a variable-length integer (varint).
func (d *LevelDBDecoder) DecodeVarint() (int64, uint64, error) {
	startPos := d.Offset()
	val, err := binary.ReadUvarint(simpleByteReader{d.reader})
	return startPos, val, err
}

// DecodeUTF16StringWithLengthBigEndian decodes a UTF-16 BE string.
// This is used by IndexedDB keys, which (unlike V8 values) use big endian.
func (d *LevelDBDecoder) DecodeUTF16StringWithLengthBigEndian() (int64, string, error) {
	startPos := d.Offset()
	_, length, err := d.DecodeVarint()
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string length: %w", err)
	}

	_, utf16Bytes, err := d.ReadBytes(int(length))
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string content: %w", err)
	}

	// Use BigEndian decoder
	decoder := xunicode.UTF16(xunicode.BigEndian, xunicode.IgnoreBOM).NewDecoder()
	utf8Bytes, _, err := transform.Bytes(decoder, utf16Bytes)
	if err != nil {
		// Return the raw hex on failure, it's better than nothing
		return d.Offset() - startPos, fmt.Sprintf("<undecodable_utf16_be_hex:%x>", utf16Bytes), nil
	}

	return d.Offset() - startPos, string(utf8Bytes), nil
}

func (d *LevelDBDecoder) DecodeUTF16StringWithLength() (int64, string, error) {
	startPos := d.Offset()
	_, length, err := d.DecodeVarint()
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string length: %w", err)
	}

	_, utf16Bytes, err := d.ReadBytes(int(length))
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string content: %w", err)
	}

	decoder := xunicode.UTF16(xunicode.LittleEndian, xunicode.IgnoreBOM).NewDecoder()
	utf8Bytes, _, err := transform.Bytes(decoder, utf16Bytes)
	if err != nil {
		return d.Offset() - startPos, fmt.Sprintf("<undecodable_utf16_hex:%x>", utf16Bytes), nil
	}

	return d.Offset() - startPos, string(utf8Bytes), nil
}

// DecodeUTF8StringWithLength decodes a UTF-8 string.
func (d *LevelDBDecoder) DecodeUTF8StringWithLength() (int64, string, error) {
	startPos := d.Offset()
	_, length, err := d.DecodeVarint()
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string length: %w", err)
	}

	_, utf8Bytes, err := d.ReadBytes(int(length))
	if err != nil {
		return startPos, "", fmt.Errorf("failed to read string content: %w", err)
	}
	return d.Offset() - startPos, string(utf8Bytes), nil
}

// DecodeBlobWithLength decodes a blob prefixed with its varint length.
func (d *LevelDBDecoder) DecodeBlobWithLength() (int64, []byte, error) {
	startPos := d.Offset()
	_, numBytes, err := d.DecodeVarint()
	if err != nil {
		return startPos, nil, err
	}
	_, blob, err := d.ReadBytes(int(numBytes))
	return startPos, blob, err
}

// PeekBytes reads bytes without advancing the reader offset.
func (d *LevelDBDecoder) PeekBytes(count int) ([]byte, error) {
	currentOffset := d.Offset()
	_, data, err := d.ReadBytes(count)
	if err != nil {
		// Even on error, try to rewind to the original position
		d.Seek(currentOffset, io.SeekStart)
		return nil, err
	}
	d.Seek(currentOffset, io.SeekStart) // Rewind
	return data, nil
}

func (d *LevelDBDecoder) Remaining() (int64, error) {
	current := d.Offset()
	end, err := d.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	d.Seek(current, io.SeekStart)
	return end - current, nil
}
