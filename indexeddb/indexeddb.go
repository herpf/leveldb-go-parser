package indexeddb

import (
	"encoding/json"
	"fmt"
	"os"

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

		if recordOffset == 264328 {
			fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD PRE-PARSE (IndexedDB) !!!!!!!! Path: %s, Offset: %d, KeyHex: %x, ValueHex(first 64b): %x\n", rec.Path, recordOffset, keyData, valueData[:min(len(valueData), 64)])
		}

		parsedKey, err := chromium.ParseKey(keyData)
		if err != nil {
			if recordOffset == 264328 {
				fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD KEY PARSE FAILED !!!!!!!! Error: %v, Raw Key (hex): %x\n", err, keyData)
			}
			fmt.Fprintf(os.Stderr, "Debug: Skipping key in %s (offset %d) due to KEY parsing error. Error: %v. Raw Key (hex): %x\n",
				rec.Path, recordOffset, err, keyData)
			continue
		}

		keyJSON, _ := json.Marshal(parsedKey)
		fmt.Fprintf(os.Stderr, "Debug: Parsed Key OK in %s (offset %d): %s\n", rec.Path, recordOffset, string(keyJSON))
		if recordOffset == 264328 {
			fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD KEY PARSE OK !!!!!!!! Parsed Key: %s\n", string(keyJSON))
		}
		parsedValue, err := parsedKey.ParseValue(valueData)
		if err != nil {
			if recordOffset == 264328 {
				fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD VALUE PARSE FAILED !!!!!!!! Error: %v, Raw Value (hex): %x\n", err, valueData)
			}
			fmt.Fprintf(os.Stderr, "Warning: could not parse VALUE for key type %T in %s (offset %d): %v. Raw Value (hex): %x\n",
				parsedKey, rec.Path, recordOffset, err, valueData)
		}

		if recordOffset == 264328 {
			fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD VALUE PARSE OK !!!!!!!!\n")
		}
		if objVal, ok := parsedValue.(*chromium.ObjectStoreDataValue); ok {
			fmt.Fprintf(os.Stderr, "Value type for offset %d: %T\n", recordOffset, objVal.Value)
			if objVal.Value != nil {
				if mapVal, ok := objVal.Value.(map[string]any); ok {
					objVal.Value = removeNulls(mapVal)
				} else if arrVal, ok := objVal.Value.([]any); ok {
					objVal.Value = removeNulls(arrVal)
				} else if jsArr, ok := objVal.Value.(*chromium.JSArray); ok {
					objVal.Value = removeNulls(jsArr)
				} else {
					fmt.Fprintf(os.Stderr, "Warning: Unsupported type for removeNulls at offset %d: %T\n", recordOffset, objVal.Value)
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
			fmt.Fprintf(os.Stderr, "Warning: parsedValue not ObjectStoreDataValue at offset %d: %T\n", recordOffset, parsedValue)
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
