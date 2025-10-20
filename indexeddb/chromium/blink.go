package chromium

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"

	"leveldb-parser-go/leveldb/common"

	"github.com/golang/snappy"
)

const maxAllocBytes = 1 << 20
const maxArrayLen = 1 << 20
const maxProps = 1 << 20

type JSArray struct {
	Values     []any          `json:"values,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// ObjectStoreDataValue remains the same
type ObjectStoreDataValue struct {
	Version int `json:"version"`
	Value   any `json:"value,omitempty"`
}

// BlobReference remains the same
type BlobReference struct {
	Type      string `json:"type"`
	BlobSize  uint64 `json:"blob_size"`
	BlobIndex uint64 `json:"blob_index"`
}

// This type is needed to represent the ArrayBufferView
type ArrayBufferView struct {
	Buffer []byte `json:"buffer"`
	Tag    byte   `json:"tag"` // e.g., 'C' for Uint8Array
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Flags  uint64 `json:"flags"`
}

// V8Deserializer is a stateful parser for the V8 serialization format.
type V8Deserializer struct {
	decoder  *common.LevelDBDecoder
	objects  map[uint32]any
	nextID   uint32
	version  uint32
	delegate *BlinkDeserializer
	depth    int
}

func NewV8Deserializer(data []byte) *V8Deserializer {
	return &V8Deserializer{
		decoder: common.NewLevelDBDecoder(bytes.NewReader(data)),
		objects: make(map[uint32]any),
		nextID:  0,
	}
}

func (d *V8Deserializer) assignNextID(obj any) uint32 {
	id := d.nextID
	d.objects[id] = obj
	d.nextID++
	return id
}

const latestV8Version uint32 = 15

// ReadHeader reads the V8 version tag and version number.
func (d *V8Deserializer) ReadHeader() error {
	_, tag, err := d.decoder.DecodeUint8()
	if err != nil {
		return err
	}
	if V8SerializationTag(tag) != V8VersionTag {
		return fmt.Errorf("expected V8 version tag 0xFF, got 0x%x", tag)
	}
	_, ver, err := d.decoder.DecodeVarint()
	if err != nil {
		return err
	}
	d.version = uint32(ver)
	if d.version > latestV8Version {
		return fmt.Errorf("unsupported V8 version %d (max supported: %d)", d.version, latestV8Version)
	}
	return nil
}

// Add ReadTag to skip padding
func (d *V8Deserializer) ReadTag() (V8SerializationTag, error) {
	for {
		peeked, err := d.decoder.PeekBytes(1)
		if err != nil {
			return 0, err
		}
		if len(peeked) == 0 {
			return 0, io.EOF
		}
		tag := V8SerializationTag(peeked[0])
		if tag == V8Padding {
			d.decoder.DecodeUint8() // consume padding
			continue
		}
		d.decoder.DecodeUint8() // consume the tag
		return tag, nil
	}
}

// ReadObject is the main recursive parsing function.
// It mimics the v8.py _ReadObject function, which handles postfix tags.
func (d *V8Deserializer) ReadObject() (any, error) {
	objectID := d.nextID
	value, err := d.ReadObjectInternal()
	if err != nil {
		return nil, err
	}
	if _, isRef := value.(*v8ReferenceWrapper); !isRef {
		d.objects[objectID] = value
	}
	peeked, err := d.decoder.PeekBytes(1)
	if err == nil && len(peeked) > 0 && V8SerializationTag(peeked[0]) == V8ArrayBufferViewTag {
		d.decoder.DecodeUint8()
		var buffer []byte
		if buf, ok := value.([]byte); ok {
			buffer = buf
		} else {
			if ref, ok := value.(*v8ReferenceWrapper); ok {
				if buf, ok := ref.Value.([]byte); ok {
					buffer = buf
				}
			}
		}
		if buffer == nil {
			return nil, fmt.Errorf("expected ArrayBuffer before ArrayBufferView tag, got %T", value)
		}
		view, err := d.ReadJSArrayBufferView(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to read ArrayBufferView: %w", err)
		}
		d.assignNextID(view)
		return view, nil
	}
	return value, nil
}

func (d *V8Deserializer) ReadJSArrayBufferView(buffer []byte) (*ArrayBufferView, error) {
	// Corresponds to _ReadJSArrayBufferView in v8.py
	_, tag, err := d.decoder.DecodeUint8() // This is the sub-type tag (e.g., 'C' for Uint8Array)
	if err != nil {
		return nil, fmt.Errorf("failed to read arraybufferview type tag: %w", err)
	}
	_, byteOffset, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read arraybufferview offset: %w", err)
	}
	_, byteLength, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read arraybufferview length: %w", err)
	}
	// v8.py checks version >= 14. Let's assume we need to read flags.
	// If the python code supports version 15, we must support flags.
	_, flags, err := d.decoder.DecodeVarint()
	if err != nil {
		// This might fail if version < 14.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			flags = 0 // Assume no flags if data ends
		} else {
			return nil, fmt.Errorf("failed to read arraybufferview flags: %w", err)
		}
	}
	// Create a new slice representing the view
	// Note: Python returns a class, we return the struct
	// We can also return the sliced bytes directly if preferred
	var viewBytes []byte
	if buffer != nil && byteOffset+byteLength <= uint64(len(buffer)) {
		viewBytes = buffer[byteOffset : byteOffset+byteLength]
	} else if buffer != nil {
		// This indicates a corrupt view, but we'll return what we can
		fmt.Fprintf(os.Stderr, "Debug: ArrayBufferView out of bounds (offset %d, length %d, buffer %d)\n", byteOffset, byteLength, len(buffer))
		viewBytes = buffer // return full buffer as fallback
	}
	return &ArrayBufferView{
		Buffer: viewBytes, // Return the actual sliced view
		Tag:    tag,
		Offset: byteOffset,
		Length: byteLength,
		Flags:  flags,
	}, nil
}

// This contains the main switch statement.
func (d *V8Deserializer) ReadObjectInternal() (any, error) {
	d.depth++
	if d.depth > 10000 { // Increased limit
		return nil, fmt.Errorf("max recursion depth exceeded")
	}
	defer func() { d.depth-- }()
	tag, err := d.ReadTag()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	switch tag {
	// no padding case, since skipped
	case V8ObjectReference:
		_, id, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read object reference ID: %w", err)
		}
		obj, ok := d.objects[uint32(id)]
		if !ok {
			return nil, fmt.Errorf("invalid object reference: ID %d not found", id)
		}
		return &v8ReferenceWrapper{ID: uint32(id), Value: obj}, nil
	case V8BeginJSObject:
		return d.ReadJSObject()
	case V8UTF8String:
		_, length, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read UTF8 string length: %w", err)
		}
		if length > maxAllocBytes {
			d.decoder.Seek(int64(length), io.SeekCurrent)
			return "<string_too_large: skipped>", nil
		}
		_, data, err := d.decoder.ReadBytes(int(length))
		if err != nil {
			return nil, fmt.Errorf("failed to read UTF8 string data: %w", err)
		}
		return string(data), nil
	case V8OneByteString:
		_, length, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read one-byte string length: %w", err)
		}
		if length > maxAllocBytes {
			d.decoder.Seek(int64(length), io.SeekCurrent)
			return "<string_too_large: skipped>", nil
		}
		_, data, err := d.decoder.ReadBytes(int(length))
		if err != nil {
			return nil, fmt.Errorf("failed to read one-byte string data: %w", err)
		}
		return string(data), nil
	case V8TwoByteString:
		_, length, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read two-byte string length: %w", err)
		}
		if length > maxAllocBytes {
			d.decoder.Seek(int64(length), io.SeekCurrent)
			return "<string_too_large: skipped>", nil
		}
		_, str, err := d.decoder.DecodeUTF16StringWithLength()
		return str, err
	case V8BeginDenseJSArray:
		return d.ReadDenseJSArray()
	case V8BeginSparseJSArray:
		return d.ReadSparseJSArray()
	case V8BeginJSMap:
		return d.ReadJSMap()
	case V8BeginJSSet:
		return d.ReadJSSet()
	case V8RegExp:
		return d.ReadJSRegExp()
	case V8ArrayBuffer:
		return d.ReadJSArrayBuffer(false, false)
	case V8ResizableArrayBuffer:
		return d.ReadJSArrayBuffer(false, true)
	case V8SharedArrayBuffer:
		return d.ReadJSArrayBuffer(true, false)
	case V8Error:
		return d.ReadJSError()
	case V8WasmModuleTransfer:
		return d.ReadWasmModuleTransfer()
	case V8WasmMemoryTransfer:
		return d.ReadWasmMemoryTransfer()
	case V8HostObject:
		return d.ReadHostObject()
	case V8SharedObject:
		if d.version >= 15 {
			return d.ReadSharedObject()
		}
		return fmt.Sprintf("<unsupported_shared_value>"), nil
	case V8TheHole:
		return nil, nil
	case V8Null:
		return nil, nil
	case V8Undefined:
		return "undefined", nil
	case V8True:
		return true, nil
	case V8False:
		return false, nil
	case V8Int32:
		_, uval, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read V8Int32: %w", err)
		}
		val := int64((uval >> 1) ^ (-(uval & 1)))
		return val, nil
	case V8Uint32:
		_, val, err := d.decoder.DecodeVarint()
		return val, err
	case V8Double:
		_, val, err := d.decoder.DecodeDouble()
		return val, err
	case V8BigIntObject:
		return d.ReadBigInt()
	case V8BigInt:
		_, bitField, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read V8BigInt bitfield: %w", err)
		}
		byteCount := bitField >> 1
		isNegative := (bitField & 0x1) == 1
		// Read the bytes. Max 8 bytes for int64/uint64.
		if byteCount > 8 {
			_, data, err := d.decoder.ReadBytes(int(byteCount))
			if err != nil {
				return nil, fmt.Errorf("failed to read V8BigInt data: %w", err)
			}
			sign := ""
			if isNegative {
				sign = "-"
			}
			// Return a string placeholder for very large numbers
			return fmt.Sprintf("<bigint_too_large:%s%x>", sign, data), nil
		}
		_, val, err := d.decoder.DecodeInt(int(byteCount)) // This returns uint64
		if err != nil {
			return nil, fmt.Errorf("failed to read V8BigInt value: %w", err)
		}
		if isNegative {
			// Convert uint64 to int64 for negative representation
			return int64(val) * -1, nil
		}
		return val, nil // Return as uint64
	case V8Date:
		_, ms, err := d.decoder.DecodeDouble()
		if err != nil {
			return nil, err
		}
		return ms, nil
	case V8TrueObject:
		return true, nil
	case V8FalseObject:
		return false, nil
	case V8NumberObject:
		_, val, err := d.decoder.DecodeDouble()
		return val, err
	case V8StringObject:
		val, err := d.ReadObject()
		if err != nil {
			return nil, err
		}
		if ref, ok := val.(*v8ReferenceWrapper); ok {
			val = ref.Value
		}
		if _, ok := val.(string); !ok {
			return nil, fmt.Errorf("V8StringObject did not contain a string, got %T", val)
		}
		return val, nil
	case V8VerifyObjectCount:
		_, _, err := d.decoder.DecodeVarint() // skip count
		if err != nil {
			return nil, err
		}
		obj, err := d.ReadObjectInternal()
		if err != nil {
			return nil, err
		}
		return obj, nil
	default:
		return fmt.Sprintf("<unsupported_v8_tag:0x%x>", tag), nil
	}
}

// ReadJSObject reads a V8 Object (map) from the stream.
func (d *V8Deserializer) ReadJSObject() (map[string]any, error) {
	jsObject := make(map[string]any)
	num, err := d.ReadJSObjectProperties(jsObject, V8EndJSObject)
	if err != nil {
		return nil, err
	}
	_, expected, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if int(expected) != num {
		fmt.Fprintf(os.Stderr, "Warning: JSObject expected %d properties, got %d\n", expected, num)
	}
	d.assignNextID(jsObject)
	return jsObject, nil
}

type BlinkDeserializer struct {
	decoder       *common.LevelDBDecoder
	version       int
	trailerOffset uint64
	trailerSize   uint32
}

func NewBlinkDeserializer(data []byte) *BlinkDeserializer {
	return &BlinkDeserializer{
		decoder: common.NewLevelDBDecoder(bytes.NewReader(data)),
	}
}

func (bd *BlinkDeserializer) ReadVersionEnvelope() error {
	_, tag, err := bd.decoder.DecodeUint8()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if BlinkSerializationTag(tag) != BlinkVersionTag {
		bd.decoder.Seek(-1, io.SeekCurrent)
		return nil
	}
	_, ver, err := bd.decoder.DecodeVarint()
	if err != nil {
		return err
	}
	bd.version = int(ver)
	if bd.version < 16 {
		return nil
	}
	if bd.version >= 21 {
		_, tag, err := bd.decoder.DecodeUint8()
		if err != nil {
			return err
		}
		if BlinkSerializationTag(tag) != BlinkTrailerOffset {
			return fmt.Errorf("expected trailer offset tag, got 0x%x", tag)
		}
		_, bd.trailerOffset, err = bd.decoder.DecodeUint64BE()
		if err != nil {
			return err
		}
		_, tsize32, err := bd.decoder.DecodeUint32BE()
		if err != nil {
			return err
		}
		bd.trailerSize = tsize32
	}
	return nil
}

func (bd *BlinkDeserializer) Deserialize() (any, error) {
	err := bd.ReadVersionEnvelope()
	if err != nil {
		return nil, err
	}
	currentPos := bd.decoder.Offset()
	endPos, err := bd.decoder.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	_, err = bd.decoder.Seek(currentPos, io.SeekStart)
	if err != nil {
		return nil, err
	}
	dataLen := endPos - currentPos
	_, data, err := bd.decoder.ReadBytes(int(dataLen))
	if err != nil {
		return nil, err
	}
	var v8bytes []byte
	if bd.trailerSize > 0 {
		v8bytes = data[0 : bd.trailerOffset-uint64(currentPos)]
	} else {
		v8bytes = data
	}
	d := NewV8Deserializer(v8bytes)
	d.delegate = bd
	err = d.ReadHeader()
	if err != nil {
		return nil, err
	}
	return d.ReadObject()
}

func (bd *BlinkDeserializer) ReadHostObject(d *V8Deserializer) (any, error) {
	_, tagByte, err := d.decoder.DecodeUint8()
	if err != nil {
		return nil, err
	}
	tag := BlinkSerializationTag(tagByte)
	switch tag {
	case BlinkBlob:
		uuid, err := d.ReadUTF8StringWithLength()
		if err != nil {
			return nil, err
		}
		typeStr, err := d.ReadUTF8StringWithLength()
		if err != nil {
			return nil, err
		}
		_, size, err := d.decoder.DecodeVarint()
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "Blob", "uuid": uuid, "mime_type": typeStr, "size": size}, nil
	case BlinkFile:
		path, err := d.ReadUTF8StringWithLength()
		if err != nil {
			return nil, err
		}
		name, err := d.ReadUTF8StringWithLength()
		if err != nil {
			name = ""
		}
		relative, err := d.ReadUTF8StringWithLength()
		if err != nil {
			relative = ""
		}
		uuid, err := d.ReadUTF8StringWithLength()
		if err != nil {
			return nil, err
		}
		typeStr, err := d.ReadUTF8StringWithLength()
		if err != nil {
			return nil, err
		}
		// Add more fields if needed
		return map[string]any{"type": "File", "path": path, "name": name, "relative": relative, "uuid": uuid, "mime_type": typeStr}, nil
	default:
		return fmt.Sprintf("<unsupported_blink_tag:0x%x>", tagByte), nil
	}
}

// Modify parseBlink slightly to handle the error return from parseBlink
func parseBlink(data []byte, version int) (any, error) {
	fmt.Fprintf(os.Stderr, "Debug: parseBlink received data (hex): %x\n", data)
	if len(data) == 0 {
		return &ObjectStoreDataValue{Version: version}, nil
	}
	bd := NewBlinkDeserializer(data)
	value, err := bd.Deserialize()
	if err != nil {
		// Return error instead of nil, allows caller to log raw value
		return nil, fmt.Errorf("failed to deserialize V8 value: %w", err)
	}
	if ref, ok := value.(*v8ReferenceWrapper); ok {
		value = ref.Value
	}
	fmt.Fprintf(os.Stderr, "Debug: parseBlink result (success): type %T\n", value) // Safer alternative
	// This helps make byte data searchable
	if view, ok := value.(*ArrayBufferView); ok {
		// Attempt to convert to string. This is good for forensics.
		// If it's not text, it'll be garbage, but searchable garbage.
		// The python tool seems to return a list of ints.
		// Returning the raw bytes (view.Buffer) is also an option.
		// Let's return the struct itself, JSON marshalling will handle it.
		return &ObjectStoreDataValue{
			Version: version,
			Value:   view,
		}, nil
	}
	return &ObjectStoreDataValue{
		Version: version,
		Value:   value,
	}, nil
}

// Modify ParseObjectStoreDataValue slightly to handle the error return from parseBlink
func ParseObjectStoreDataValue(valueBytes []byte) (any, error) {
	if len(valueBytes) == 0 {
		return nil, nil
	}
	decoder := common.NewLevelDBDecoder(bytes.NewReader(valueBytes))
	_, version, err := decoder.DecodeVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read version from value: %w", err)
	}
	peek, err := decoder.PeekBytes(3)
	if err == nil {
		if bytes.Equal(peek, []byte{byte(BlinkVersionTag), RequiresProcessingSSVPseudoVersion, ReplaceWithBlob}) {
			decoder.ReadBytes(3)
			_, blobSize, _ := decoder.DecodeVarint()
			_, blobIndex, _ := decoder.DecodeVarint()
			return &ObjectStoreDataValue{
				Version: int(version),
				Value: &BlobReference{
					Type:      "BlobReference",
					BlobSize:  blobSize,
					BlobIndex: blobIndex,
				},
			}, nil
		}
		if bytes.Equal(peek, []byte{byte(BlinkVersionTag), RequiresProcessingSSVPseudoVersion, CompressedWithSnappy}) {
			decoder.ReadBytes(3)
			remainingBytes, _ := io.ReadAll(decoder.GetReader())
			decompressed, err := snappy.Decode(nil, remainingBytes)
			if err != nil {
				return nil, fmt.Errorf("snappy decompression failed: %w", err)
			}
			// Call parseBlink and handle potential error
			result, err := parseBlink(decompressed, int(version))
			if err != nil {
				return nil, fmt.Errorf("parseBlink failed after snappy: %w, raw_decompressed_hex: %x", err, decompressed)
			}
			return result, nil
		}
	}
	remainingBytes, _ := io.ReadAll(decoder.GetReader())
	// Call parseBlink and handle potential error
	result, err := parseBlink(remainingBytes, int(version))
	if err != nil {
		return nil, fmt.Errorf("parseBlink failed: %w, raw_hex: %x", err, remainingBytes)
	}
	return result, nil
}

type v8ReferenceWrapper struct {
	ID    uint32
	Value any
}

// ReadJSObjectProperties reads properties until end tag
func (d *V8Deserializer) ReadJSObjectProperties(js map[string]any, endTag V8SerializationTag) (int, error) {
	num := 0
	for {
		peeked, err := d.decoder.PeekBytes(1)
		if err != nil {
			return num, err
		}
		if len(peeked) == 0 {
			return num, io.EOF
		}
		if V8SerializationTag(peeked[0]) == endTag {
			d.decoder.DecodeUint8()
			break
		}
		keyObj, err := d.ReadObject()
		if err != nil {
			return num, err
		}
		if ref, ok := keyObj.(*v8ReferenceWrapper); ok {
			keyObj = ref.Value
		}
		keyStr, ok := keyObj.(string)
		if !ok {
			return num, fmt.Errorf("property key not string, got %T", keyObj)
		}
		valueObj, err := d.ReadObject()
		if err != nil {
			return num, err
		}
		if ref, ok := valueObj.(*v8ReferenceWrapper); ok {
			valueObj = ref.Value
		}
		js[keyStr] = valueObj
		num++
		if num > maxProps {
			return num, fmt.Errorf("too many properties: %d", num)
		}
	}
	return num, nil
}

// ReadDenseJSArray updated to read properties
func (d *V8Deserializer) ReadDenseJSArray() (any, error) {
	_, length, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read dense array length: %w", err)
	}
	if length > maxArrayLen {
		return nil, fmt.Errorf("dense array length too large: %d", length)
	}
	arr := &JSArray{Values: make([]any, length), Properties: make(map[string]any)}
	for i := 0; i < int(length); i++ {
		peeked, err := d.decoder.PeekBytes(1)
		if err != nil {
			return nil, err
		}
		if len(peeked) == 0 {
			return nil, io.EOF
		}
		if V8SerializationTag(peeked[0]) == V8TheHole {
			d.decoder.DecodeUint8()
			arr.Values[i] = nil
			continue
		}
		val, err := d.ReadObject()
		if err != nil {
			return nil, err
		}
		if ref, ok := val.(*v8ReferenceWrapper); ok {
			val = ref.Value
		}
		arr.Values[i] = val
	}
	num, err := d.ReadJSObjectProperties(arr.Properties, V8EndDenseJSArray)
	if err != nil {
		return nil, err
	}
	_, expectedNum, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	_, expectedLength, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if int(expectedNum) != num || int(expectedLength) != int(length) {
		fmt.Fprintf(os.Stderr, "Warning: DenseJSArray expected num %d length %d, got num %d length %d\n", expectedNum, expectedLength, num, length)
	}
	d.assignNextID(arr)
	return arr, nil
}

// ReadSparseJSArray implementation
func (d *V8Deserializer) ReadSparseJSArray() (any, error) {
	_, length, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if length > maxArrayLen {
		return nil, fmt.Errorf("sparse array length too large: %d", length)
	}
	arr := &JSArray{Values: make([]any, length), Properties: make(map[string]any)}
	for i := range arr.Values {
		arr.Values[i] = nil // or "undefined"
	}
	num, err := d.ReadJSObjectProperties(arr.Properties, V8EndSparseJSArray)
	if err != nil {
		return nil, err
	}
	_, expectedNum, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	_, expectedLength, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if int(expectedNum) != num || int(expectedLength) != int(length) {
		fmt.Fprintf(os.Stderr, "Warning: SparseJSArray expected num %d length %d, got num %d length %d\n", expectedNum, expectedLength, num, length)
	}
	d.assignNextID(arr)
	return arr, nil
}

// ReadJSMap implementation
func (d *V8Deserializer) ReadJSMap() (any, error) {
	entries := make([]JSMapEntry, 0)
	count := 0
	for {
		peeked, err := d.decoder.PeekBytes(1)
		if err != nil {
			return nil, err
		}
		if V8SerializationTag(peeked[0]) == V8EndJSMap {
			d.decoder.DecodeUint8()
			break
		}
		key, err := d.ReadObject()
		if err != nil {
			return nil, err
		}
		value, err := d.ReadObject()
		if err != nil {
			return nil, err
		}
		entries = append(entries, JSMapEntry{key, value})
		count++
		if count > maxArrayLen {
			return nil, fmt.Errorf("JSMap too large: exceeded %d entries", maxArrayLen)
		}
	}
	_, expected, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if int(expected) != len(entries)*2 {
		fmt.Fprintf(os.Stderr, "Warning: JSMap expected %d, got %d\n", expected, len(entries)*2)
	}
	d.assignNextID(entries)
	return entries, nil
}

type JSMapEntry [2]any

// ReadJSSet implementation
func (d *V8Deserializer) ReadJSSet() (any, error) {
	jsSet := make([]any, 0)
	count := 0
	for {
		peeked, err := d.decoder.PeekBytes(1)
		if err != nil {
			return nil, err
		}
		if V8SerializationTag(peeked[0]) == V8EndJSSet {
			d.decoder.DecodeUint8()
			break
		}
		elem, err := d.ReadObject()
		if err != nil {
			return nil, err
		}
		if ref, ok := elem.(*v8ReferenceWrapper); ok {
			elem = ref.Value
		}
		jsSet = append(jsSet, elem)
		count++
		if count > maxArrayLen {
			return nil, fmt.Errorf("JSSet too large: exceeded %d elements", maxArrayLen)
		}
	}
	_, expected, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	if int(expected) != len(jsSet) {
		fmt.Fprintf(os.Stderr, "Warning: JSSet expected %d, got %d\n", expected, len(jsSet))
	}
	d.assignNextID(jsSet)
	return jsSet, nil
}

func (d *V8Deserializer) ReadBigInt() (any, error) {
	_, bitField, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read V8BigInt bitfield: %w", err)
	}
	byteCount := bitField >> 1
	isNegative := (bitField & 0x1) == 1
	// Add logging to debug
	fmt.Fprintf(os.Stderr, "Debug: ReadBigInt bitField=%d, byteCount=%d\n", bitField, byteCount)
	if byteCount > uint64(math.MaxInt64) {
		// Cannot safely cast to int64 for seek; treat as corrupt
		return fmt.Sprintf("<bigint_invalid_size: too large (%d)>", byteCount), nil
	}
	rem, remErr := d.decoder.Remaining()
	if remErr == nil && int64(byteCount) > rem {
		// Skip to end if larger than remaining
		d.decoder.Seek(rem, io.SeekCurrent)
		return "<bigint_corrupt: larger than remaining data>", nil
	}
	if byteCount > maxAllocBytes {
		_, err := d.decoder.Seek(int64(byteCount), io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failed to skip large BigInt: %w", err)
		}
		sign := ""
		if isNegative {
			sign = "-"
		}
		return fmt.Sprintf("<bigint_too_large: skipped (size %d)%s>", byteCount, sign), nil
	}
	if byteCount > 8 {
		_, data, err := d.decoder.ReadBytes(int(byteCount))
		if err != nil {
			return nil, fmt.Errorf("failed to read V8BigInt data: %w", err)
		}
		sign := ""
		if isNegative {
			sign = "-"
		}
		return fmt.Sprintf("<bigint_too_large:%s%x>", sign, data), nil
	}
	_, val, err := d.decoder.DecodeInt(int(byteCount))
	if err != nil {
		return nil, fmt.Errorf("failed to read V8BigInt value: %w", err)
	}
	if isNegative {
		return int64(val) * -1, nil
	}
	return val, nil
}

// ReadJSRegExp implementation
func (d *V8Deserializer) ReadJSRegExp() (any, error) {
	pattern, err := d.ReadUTF8StringWithLength() // or d.ReadString() if version aware
	if err != nil {
		return nil, err
	}
	_, flags, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	regexp := map[string]any{"pattern": pattern, "flags": flags}
	d.assignNextID(regexp)
	return regexp, nil
}

// ReadJSArrayBuffer updated
func (d *V8Deserializer) ReadJSArrayBuffer(isShared, isResizable bool) (any, error) {
	_, byteLength, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	maxByteLength := byteLength
	if isResizable {
		_, maxByteLength, err = d.decoder.DecodeVarint()
		if err != nil {
			return nil, err
		}
		if byteLength > maxByteLength {
			return "<invalid_resizable_array_buffer>", nil
		}
	}
	var data []byte
	if byteLength > 0 {
		if byteLength > maxAllocBytes {
			return nil, fmt.Errorf("array buffer too large: %d", byteLength)
		}
		_, data, err = d.decoder.ReadBytes(int(byteLength))
		if err != nil {
			return nil, err
		}
	}
	d.assignNextID(data)
	return data, nil
}

// ReadJSError implementation
func (d *V8Deserializer) ReadJSError() (any, error) {
	name, err := d.ReadObject()
	if err != nil {
		return nil, err
	}
	message, err := d.ReadObject()
	if err != nil {
		return nil, err
	}
	stack, err := d.ReadObject()
	if err != nil {
		return nil, err
	}
	code, err := d.ReadObject()
	if err != nil {
		return nil, err
	}
	var options any
	if d.version >= 13 {
		options, err = d.ReadObject()
		if err != nil {
			return nil, err
		}
	}
	errorObj := map[string]any{"name": name, "message": message, "stack": stack, "code": code, "options": options}
	d.assignNextID(errorObj)
	return errorObj, nil
}

// ReadWasmModuleTransfer placeholder
func (d *V8Deserializer) ReadWasmModuleTransfer() (any, error) {
	return "<wasm_module_transfer>", nil
}

// ReadWasmMemoryTransfer placeholder
func (d *V8Deserializer) ReadWasmMemoryTransfer() (any, error) {
	return "<wasm_memory_transfer>", nil
}

func (d *V8Deserializer) ReadHostObject() (any, error) {
	if d.delegate == nil {
		return "<host_object_no_delegate>", nil
	}
	return d.delegate.ReadHostObject(d)
}

// ReadSharedValue placeholder
func (d *V8Deserializer) ReadSharedObject() (any, error) {
	_, id, err := d.decoder.DecodeVarint()
	if err != nil {
		return nil, err
	}
	return fmt.Sprintf("<shared_value:%d>", id), nil
}

// ReadUTF8StringWithLength helper if not already
func (d *V8Deserializer) ReadUTF8StringWithLength() (string, error) {
	_, length, err := d.decoder.DecodeVarint()
	if err != nil {
		return "", err
	}
	_, data, err := d.decoder.ReadBytes(int(length))
	if err != nil {
		return "", err
	}
	return string(data), nil
}
