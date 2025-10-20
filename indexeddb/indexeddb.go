package indexeddb

import (
	"encoding/json"
	"fmt"

	"leveldb-parser-go/config"
	"leveldb-parser-go/indexeddb/chromium"
	"leveldb-parser-go/leveldb/db"
)

// IndexedDBRecord represents a fully parsed IndexedDB record.
type IndexedDBRecord struct {
	Path           string `json:"path"`
	Offset         int64  `json:"offset"`
	Key            any    `json:"key"`
	Value          any    `json:"value,omitempty"`
	SequenceNumber uint64 `json:"sequence_number"`
	RecordType     byte   `json:"type"` // 0 for deletion, 1 for value
	Recovered      bool   `json:"recovered"`
	DatabaseID     int    `json:"database_id"`
	ObjectStoreID  int    `json:"object_store_id"`
}

// FolderReader processes a whole IndexedDB (LevelDB) directory.
type FolderReader struct {
	levelDBReader *db.FolderReader
}

// NewFolderReader creates a new reader for an IndexedDB folder.
func NewFolderReader(path string) (*FolderReader, error) {
	levelDBReader, err := db.NewFolderReader(path)
	if err != nil {
		return nil, err
	}
	return &FolderReader{levelDBReader: levelDBReader}, nil
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

		if objVal, ok := parsedValue.(*chromium.ObjectStoreDataValue); ok {
			config.VerboseLogger.Printf("Value type for offset %d: %T", recordOffset, objVal.Value)
			if objVal.Value != nil {
				if mapVal, ok := objVal.Value.(map[string]any); ok {
					objVal.Value = removeNulls(mapVal)
				} else if arrVal, ok := objVal.Value.([]any); ok {
					objVal.Value = removeNulls(arrVal)
				} else if jsArr, ok := objVal.Value.(*chromium.JSArray); ok {
					objVal.Value = removeNulls(jsArr)
				} else {
					config.VerboseLogger.Printf("Warning: Unsupported type for removeNulls at offset %d: %T", recordOffset, objVal.Value)
					// Add handling for additional types here if needed. For example, for JSMap:
					// if mapEntry, ok := objVal.Value.([]chromium.JSMapEntry); ok {
					//   var cleaned []chromium.JSMapEntry
					//   for _, entry := range mapEntry {
					//     c0 := removeNulls(entry[0])
					//     c1 := removeNulls(entry[1])
					//     if c0 != nil || c1 != nil {
					//       cleaned = append(cleaned, chromium.JSMapEntry{c0, c1})
					//     }
					//   }
					//   if len(cleaned) == 0 {
					//     objVal.Value = nil
					//   } else {
					//     objVal.Value = cleaned
					//   }
					// }
				}
			}
		} else {
			config.VerboseLogger.Printf("Warning: parsedValue not ObjectStoreDataValue at offset %d: %T", recordOffset, parsedValue)
		}

		indexedDBRecords = append(indexedDBRecords, &IndexedDBRecord{
			Path:           rec.Path,
			Offset:         recordOffset, // Use stored offset
			Key:            parsedKey,
			Value:          parsedValue,
			SequenceNumber: rec.Record.GetSequenceNumber(),
			RecordType:     rec.Record.GetType(),
			Recovered:      rec.Recovered,
			DatabaseID:     parsedKey.GetKeyPrefix().DatabaseID,
			ObjectStoreID:  parsedKey.GetKeyPrefix().ObjectStoreID,
		})
	}

	return indexedDBRecords, nil
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
				if cleanedItem != nil { // Skip empty substructures
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
		// Check if the string is itself JSON
		var jsonData any
		if err := json.Unmarshal([]byte(val), &jsonData); err == nil {
			// It is valid JSON. Recursively clean it.
			return removeNulls(jsonData)
		}
		// It's not JSON, just a regular string. Return it as is.
		return val
	case *chromium.JSArray: // Handle custom types if needed
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
		// If both values and properties are nil after cleaning, return nil
		if val.Values == nil && val.Properties == nil {
			return nil
		}
		return val
	default:
		return val // Non-containers unchanged
	}
}
