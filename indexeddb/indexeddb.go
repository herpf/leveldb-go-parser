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
			continue // *** BUG FIX: Skip record if value parse fails ***
		}

		if recordOffset == 264328 {
			fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD VALUE PARSE OK !!!!!!!!\n")
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

// At the package level in indexeddb/indexeddb.go
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
