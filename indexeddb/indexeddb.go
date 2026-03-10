// File: indexeddb/indexeddb.go
package indexeddb

import (
	"encoding/json"
	"fmt"
	"strings"

	"leveldb-parser-go/config"
	"leveldb-parser-go/indexeddb/chromium"
	"leveldb-parser-go/leveldb/db"
)

// Struct to hold the actual extracted blob data and path
type BlobData struct {
	Path  string `json:"path"`
	Data  []byte `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// IndexedDBRecord represents a fully parsed IndexedDB record.
type IndexedDBRecord struct {
	Path           string     `json:"path"`
	Offset         int64      `json:"offset"`
	Key            any        `json:"key"`
	Value          any        `json:"value,omitempty"`
	Blobs          []BlobData `json:"blobs,omitempty"`
	SequenceNumber uint64     `json:"sequence_number"`
	RecordType     byte       `json:"type"` // 0 for deletion, 1 for value
	Recovered      bool       `json:"recovered"`
	DatabaseID     int        `json:"database_id"`
	ObjectStoreID  int        `json:"object_store_id"`
}

// FolderReader processes a whole IndexedDB (LevelDB) directory.
type FolderReader struct {
	levelDBReader *db.FolderReader
	blobReader    *chromium.BlobFolderReader
}

// NewFolderReader creates a new reader for an IndexedDB folder.
func NewFolderReader(path string) (*FolderReader, error) {
	levelDBReader, err := db.NewFolderReader(path)
	if err != nil {
		return nil, err
	}

	// Detect if a .blob folder exists next to the .leveldb folder
	var blobReader *chromium.BlobFolderReader
	if strings.HasSuffix(path, ".leveldb") {
		blobDirPath := strings.Replace(path, ".leveldb", ".blob", 1)
		if reader, err := chromium.NewBlobFolderReader(blobDirPath); err == nil {
			blobReader = reader
			config.VerboseLogger.Printf("Found associated blob folder: %s", blobDirPath)
		}
	}

	return &FolderReader{
		levelDBReader: levelDBReader,
		blobReader:    blobReader, // Attach it to the struct
	}, nil
}

func (fr *FolderReader) GetRecords() ([]*IndexedDBRecord, error) {
	levelDBRecords, err := fr.levelDBReader.GetRecords()
	if err != nil {
		return nil, fmt.Errorf("could not read leveldb records: %w", err)
	}

	var indexedDBRecords []*IndexedDBRecord

	for _, rec := range levelDBRecords {
		keyData := rec.Record.GetKey()
		valueData := rec.Record.GetValue()
		recordOffset := rec.Record.GetOffset() // Store offset for logging

		parsedKey, err := chromium.ParseKey(keyData)
		if err != nil {
			config.VerboseLogger.Printf("Debug: Skipping key in %s (offset %d) due to KEY parsing error. Error: %v. Raw Key (hex): %x",
				rec.Path, recordOffset, err, keyData)
			continue
		}

		keyJSON, _ := json.Marshal(parsedKey)
		config.VerboseLogger.Printf("Debug: Parsed Key OK in %s (offset %d): %s", rec.Path, recordOffset, string(keyJSON))

		parsedValue, err := parsedKey.ParseValue(valueData)
		if err != nil {
			config.VerboseLogger.Printf("Warning: could not parse VALUE for key type %T in %s (offset %d): %v. Raw Value (hex): %x",
				parsedKey, rec.Path, recordOffset, err, valueData)
		}

		// Slice to hold any blobs we find in this specific record
		var recordBlobs []BlobData
		dbID := parsedKey.GetKeyPrefix().DatabaseID

		if objVal, ok := parsedValue.(*chromium.ObjectStoreDataValue); ok {
			config.VerboseLogger.Printf("Value type for offset %d: %T", recordOffset, objVal.Value)
			if objVal.Value != nil {

				// Recursively search the parsed value for BlobReferences and extract them
				extractBlobs(objVal.Value, dbID, fr.blobReader, &recordBlobs)

				if mapVal, ok := objVal.Value.(map[string]any); ok {
					objVal.Value = removeNulls(mapVal)
				} else if arrVal, ok := objVal.Value.([]any); ok {
					objVal.Value = removeNulls(arrVal)
				} else if jsArr, ok := objVal.Value.(*chromium.JSArray); ok {
					objVal.Value = removeNulls(jsArr)
				} else {
					config.VerboseLogger.Printf("Warning: Unsupported type for removeNulls at offset %d: %T", recordOffset, objVal.Value)
				}
			}
		} else {
			config.VerboseLogger.Printf("Warning: parsedValue not ObjectStoreDataValue at offset %d: %T", recordOffset, parsedValue)
		}

		indexedDBRecords = append(indexedDBRecords, &IndexedDBRecord{
			Path:           rec.Path,
			Offset:         recordOffset,
			Key:            parsedKey,
			Value:          parsedValue,
			Blobs:          recordBlobs, // Attach the extracted blobs to the final record!
			SequenceNumber: rec.Record.GetSequenceNumber(),
			RecordType:     rec.Record.GetType(),
			Recovered:      rec.Recovered,
			DatabaseID:     dbID,
			ObjectStoreID:  parsedKey.GetKeyPrefix().ObjectStoreID,
		})
	}

	return indexedDBRecords, nil
}

// extractBlobs recursively searches through parsed maps and arrays to find BlobReferences
func extractBlobs(v any, dbID int, blobReader *chromium.BlobFolderReader, blobs *[]BlobData) {
	if v == nil || blobReader == nil {
		return
	}

	switch val := v.(type) {
	case *chromium.BlobReference: // blob found
		path, data, err := blobReader.ReadBlob(dbID, val.BlobIndex)
		blob := BlobData{Path: path}
		if err != nil {
			blob.Error = err.Error()
		} else {
			blob.Data = data
		}
		*blobs = append(*blobs, blob)

	case []any: // Search inside arrays
		for _, item := range val {
			extractBlobs(item, dbID, blobReader, blobs)
		}

	case map[string]any: // Search inside maps/objects
		for _, item := range val {
			extractBlobs(item, dbID, blobReader, blobs)
		}

	case *chromium.JSArray: // Search inside parsed V8 JSArrays
		extractBlobs(val.Values, dbID, blobReader, blobs)
		extractBlobs(val.Properties, dbID, blobReader, blobs)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// removeNulls recursively removes nil elements from slices and maps.
func removeNulls(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []any:
		var cleaned []any
		for _, item := range val {
			if item != nil {
				cleanedItem := removeNulls(item)
				if cleanedItem != nil {
					cleaned = append(cleaned, cleanedItem)
				}
			}
		}
		if len(cleaned) == 0 {
			return nil
		}
		return cleaned
	case map[string]any:
		cleanedMap := make(map[string]any)
		for k, item := range val {
			cleanedItem := removeNulls(item)
			if cleanedItem != nil {
				cleanedMap[k] = cleanedItem
			}
		}
		if len(cleanedMap) == 0 {
			return nil
		}
		return cleanedMap
	case string:
		var jsonData any
		if err := json.Unmarshal([]byte(val), &jsonData); err == nil {
			return removeNulls(jsonData)
		}
		return val
	case *chromium.JSArray:
		values := removeNulls(val.Values)
		if values != nil {
			val.Values = values.([]any)
		} else {
			val.Values = nil
		}
		props := removeNulls(val.Properties)
		if props != nil {
			val.Properties = props.(map[string]any)
		} else {
			val.Properties = nil
		}
		if val.Values == nil && val.Properties == nil {
			return nil
		}
		return val
	default:
		return val
	}
}
