package chromium

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"leveldb-parser-go/leveldb/common"
)

// IndexedDBKey is an interface for all parsed IndexedDB key types.
type IndexedDBKey interface {
	ParseValue(valueBytes []byte) (any, error)
	GetKeyPrefix() *KeyPrefix
}

// BaseKey provides common fields for all key types.
type BaseKey struct {
	Prefix *KeyPrefix `json:"key_prefix"`
}

func (b *BaseKey) GetKeyPrefix() *KeyPrefix {
	return b.Prefix
}

// KeyPrefix is the header for all IndexedDB keys.
type KeyPrefix struct {
	DatabaseID    int `json:"database_id"`
	ObjectStoreID int `json:"object_store_id"`
	IndexID       int `json:"index_id"`
}

// GetKeyPrefixType determines the record type based on the IDs.
func (kp *KeyPrefix) GetKeyPrefixType() (KeyPrefixType, error) {
	if kp.DatabaseID == 0 {
		return GlobalMetadata, nil
	}
	if kp.ObjectStoreID == 0 {
		return DatabaseMetadata, nil
	}
	switch kp.IndexID {
	case 1:
		return ObjectStoreData, nil
	case 2:
		return ExistsEntry, nil
	case 3:
		return BlobEntry, nil
	}
	if kp.IndexID >= 30 {
		return IndexData, nil
	}
	return InvalidType, fmt.Errorf("unknown key prefix type (index_id=%d)", kp.IndexID)
}

// DecodeKeyPrefix parses the key prefix from a decoder.
func DecodeKeyPrefix(decoder *common.LevelDBDecoder) (*KeyPrefix, error) {
	_, rawPrefix, err := decoder.ReadBytes(1)
	if err != nil {
		return nil, fmt.Errorf("failed to read key prefix byte: %w", err)
	}

	dbIDLen := (rawPrefix[0] & 0xE0 >> 5) + 1
	objStoreIDLen := (rawPrefix[0] & 0x1C >> 2) + 1
	indexIDLen := (rawPrefix[0] & 0x03) + 1

	if dbIDLen < 1 || dbIDLen > 8 || objStoreIDLen < 1 || objStoreIDLen > 8 || indexIDLen < 1 || indexIDLen > 4 {
		return nil, errors.New("invalid id lengths in key prefix")
	}

	_, dbID, err := decoder.DecodeInt(int(dbIDLen))
	if err != nil {
		return nil, err
	}
	_, objStoreID, err := decoder.DecodeInt(int(objStoreIDLen))
	if err != nil {
		return nil, err
	}
	_, indexID, err := decoder.DecodeInt(int(indexIDLen))
	if err != nil {
		return nil, err
	}

	return &KeyPrefix{
		DatabaseID:    int(dbID),
		ObjectStoreID: int(objStoreID),
		IndexID:       int(indexID),
	}, nil
}


// ObjectStoreDataKey represents data stored in an object store.
type ObjectStoreDataKey struct {
	BaseKey
	UserKey any `json:"user_key"`
}

func (k *ObjectStoreDataKey) ParseValue(valueBytes []byte) (any, error) {
	return ParseObjectStoreDataValue(valueBytes)
}

// DatabaseNameKey maps a database name to its ID.
type DatabaseNameKey struct {
	BaseKey
	Origin       string `json:"origin"`
	DatabaseName string `json:"database_name"`
}

func (k *DatabaseNameKey) ParseValue(valueBytes []byte) (any, error) {
	// If the value is empty, it's a deletion record. There's nothing to parse.
	if len(valueBytes) == 0 {
		return nil, nil
	}
	decoder := common.NewLevelDBDecoder(bytes.NewReader(valueBytes))
	_, val, err := decoder.DecodeVarint()
	// The EOF can still happen on corrupted/truncated records, so we handle it gracefully.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, nil
	}
	return val, err
}

// GenericKey is used for metadata keys where the value is simple.
type GenericKey struct {
	BaseKey
	KeyType string `json:"key_type"`
	Details any    `json:"details,omitempty"`
}

func (k *GenericKey) ParseValue(valueBytes []byte) (any, error) {
	// For many metadata types, the value is a simple varint or string.
	// This is a simplification; a full implementation would switch on k.KeyType.
	return fmt.Sprintf("Raw value: %x", valueBytes), nil
}

// IDBKey represents a decoded component of a user key.
type IDBKey struct {
	Type  IDBKeyType `json:"type"`
	Value any        `json:"value"`
}

// decodeIDBKey recursively parses an IDBKey from the stream.
func decodeIDBKey(decoder *common.LevelDBDecoder) (IDBKey, error) {
	_, typeByte, err := decoder.DecodeUint8()
	if err != nil {
		return IDBKey{}, err
	}
	keyType := IDBKeyType(typeByte)
	var value any

	switch keyType {
	case IDBKeyNull, IDBKeyMinKey:
		value = nil
	case IDBKeyString:
		// CORRECTED: Use the new BIG ENDIAN UTF16 decoder for keys
		_, val, err := decoder.DecodeUTF16StringWithLengthBigEndian()
		if err != nil {
			return IDBKey{}, err
		}
		value = val
	case IDBKeyDate:
		_, ms, err := decoder.DecodeDouble()
		if err != nil {
			return IDBKey{}, err
		}
		value = ms / 1000.0 // Convert to seconds for easier use
	case IDBKeyNumber:
		_, val, err := decoder.DecodeDouble()
		if err != nil {
			return IDBKey{}, err
		}
		value = val
	case IDBKeyArray:
		_, length, err := decoder.DecodeVarint()
		if err != nil {
			return IDBKey{}, err
		}
		arr := make([]IDBKey, length)
		for i := 0; i < int(length); i++ {
			arr[i], err = decodeIDBKey(decoder)
			if err != nil {
				return IDBKey{}, err
			}
		}
		value = arr
	case IDBKeyBinary:
		_, val, err := decoder.DecodeBlobWithLength()
		if err != nil {
			return IDBKey{}, err
		}
		value = val
	default:
		return IDBKey{}, fmt.Errorf("unsupported IDBKeyType: %d", keyType)
	}

	return IDBKey{Type: keyType, Value: value}, nil
}

func ParseKey(keyBytes []byte) (IndexedDBKey, error) {
	if len(keyBytes) == 0 {
		return nil, errors.New("cannot parse an empty key")
	}

	decoder := common.NewLevelDBDecoder(bytes.NewReader(keyBytes))
	prefix, err := DecodeKeyPrefix(decoder)
	if err != nil {
		// If prefix fails, it's definitely not a valid IndexedDB key
		return nil, fmt.Errorf("failed to decode key prefix: %w", err)
	}

	// Check if the key ONLY contains the prefix and nothing else
	if decoder.Offset() == int64(len(keyBytes)) {
		// Treat this as a generic marker, not a specific record type
		keyType, _ := prefix.GetKeyPrefixType() // Get type for context if possible
		return &GenericKey{
			BaseKey: BaseKey{Prefix: prefix},
			KeyType: fmt.Sprintf("%v (Prefix Only)", keyType),
		}, nil
	}

	keyType, err := prefix.GetKeyPrefixType()
	if err != nil {
		// If prefix is valid but type is unknown, return error
		return nil, fmt.Errorf("unknown key prefix type: %w", err)
	}

	baseKey := BaseKey{Prefix: prefix}

	switch keyType {
	case GlobalMetadata:
		// Check if there are bytes remaining for the specific metadata type
		metaTypeByte, err := decoder.PeekBytes(1)
		if err != nil || len(metaTypeByte) == 0 {
			// Not enough data for a specific type, treat as generic
			return &GenericKey{BaseKey: baseKey, KeyType: "GlobalMetadata (Generic or Prefix Only)"}, nil
		}

		metaType := GlobalMetadataKeyType(metaTypeByte[0])
		if metaType == DatabaseName {
			// Try to parse DatabaseName details
			decoder.DecodeUint8() // Consume type byte
			_, origin, errOrigin := decoder.DecodeUTF16StringWithLength()
			_, dbName, errDbName := decoder.DecodeUTF16StringWithLength()
			if errOrigin != nil || errDbName != nil {
				return nil, fmt.Errorf("failed to decode DatabaseName key details: origin_err=%v, dbname_err=%v", errOrigin, errDbName)
			}
			return &DatabaseNameKey{BaseKey: baseKey, Origin: origin, DatabaseName: dbName}, nil
		}
		// Other global metadata types handled generically for now
		return &GenericKey{BaseKey: baseKey, KeyType: fmt.Sprintf("GlobalMetadata (Type %d)", metaType)}, nil

	case DatabaseMetadata:
		// Needs specific parsing based on metadata type byte if we want details
		return &GenericKey{BaseKey: baseKey, KeyType: "DatabaseMetadata"}, nil

	// Cases requiring user key parsing
	case ObjectStoreData, BlobEntry, ExistsEntry, IndexData:
		userKey, err := decodeIDBKey(decoder)
		// If decodeIDBKey fails FOR ANY REASON (EOF, bad type, etc.),
		// return the error directly.
		if err != nil {
			return nil, fmt.Errorf("failed to decode user key for type %v: %w", keyType, err)
		}

		// Successfully parsed user key
		switch keyType {
		case ObjectStoreData:
			return &ObjectStoreDataKey{BaseKey: baseKey, UserKey: userKey}, nil
		case ExistsEntry:
			// Fallthrough for now, treat like Generic DataEntry
			fallthrough
		case BlobEntry:
			// Fallthrough for now, treat like Generic DataEntry
			fallthrough
		case IndexData:
			// TODO: Add specific IndexData handling (sequence num, primary key) if needed
			return &GenericKey{BaseKey: baseKey, KeyType: "DataEntry", Details: userKey}, nil
		}

	default: // InvalidType or unhandled cases
		return nil, fmt.Errorf("parsing not implemented or invalid key type %v", keyType)
	}
	// Should be unreachable
	return nil, errors.New("unexpected fallthrough in ParseKey")
}
