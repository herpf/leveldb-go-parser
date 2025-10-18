package db

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"leveldb-parser-go/leveldb/common"
	"leveldb-parser-go/leveldb/ldb"
	"leveldb-parser-go/leveldb/log"
)

// LevelDBRecord wraps a record with its source path and recovery status.
type LevelDBRecord struct {
	Path      string
	Record    common.Record
	Recovered bool
}

// JSONLevelDBRecord is the JSON-friendly version for output.
type JSONLevelDBRecord struct {
	Path      string            `json:"path"`
	Record    common.JSONRecord `json:"record"`
	Recovered bool              `json:"recovered"`
}

// FolderReader processes a whole LevelDB directory.
type FolderReader struct {
	folderPath string
}

// NewFolderReader creates a new reader for a LevelDB folder.
func NewFolderReader(path string) (*FolderReader, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", path)
	}
	return &FolderReader{folderPath: path}, nil
}

// GetRecords reads all files, sorts records by key and sequence number to determine active/recovered status.
func (fr *FolderReader) GetRecords() ([]*LevelDBRecord, error) {
	unsortedRecords := make(map[string][]*LevelDBRecord)

	err := filepath.WalkDir(fr.folderPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil // Skip directories
		}

		ext := strings.ToLower(filepath.Ext(path))
		var currentRecords []common.Record
		recordSourceType := "" // To know if it came from log or ldb

		switch ext {
		case ".log":
			recordSourceType = "LOG"
			reader := log.NewFileReader(path)
			parsedKeys, parseErr := reader.GetParsedInternalKeys()
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: could not parse log file %s: %v\n", path, parseErr)
				return nil
			}
			for i := range parsedKeys {
				currentRecords = append(currentRecords, &parsedKeys[i])
			}
		case ".ldb", ".sst":
			recordSourceType = "LDB"
			reader, parseErr := ldb.NewFileReader(path)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: could not parse ldb file %s: %v\n", path, parseErr)
				return nil
			}
			keyValueRecords, parseErr := reader.GetKeyValueRecords()
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: could not parse ldb records from %s: %v\n", path, parseErr)
				return nil
			}
			for i := range keyValueRecords {
				currentRecords = append(currentRecords, &keyValueRecords[i])
			}
		default:
			return nil
		}

		// --- NEW LOGGING: Log raw keys as read ---
		fmt.Fprintf(os.Stderr, "Debug-DB: Processing %d records from %s (%s)\n", len(currentRecords), path, recordSourceType)
		for _, rec := range currentRecords {
			fmt.Fprintf(os.Stderr, "Debug-DB: Raw record from %s (Offset: %d, Type: %T, KeyHex: %x)\n",
				path, rec.GetOffset(), rec, rec.GetKey())
			// *** ADD THIS BLOCK ***
			if rec.GetOffset() == 264328 {
				fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD READ (DB) !!!!!!!! Path: %s, Offset: %d, Type: %T, KeyHex: %x\n", path, rec.GetOffset(), rec, rec.GetKey())
			}
			// *** END OF BLOCK ***
		}
		// ------------------------------------------

		// Group records by their key.
		for _, rec := range currentRecords {
			// Convert key to string for map key. Handle potential non-printable bytes.
			keyStr := string(rec.GetKey())
			levelDBRecord := &LevelDBRecord{
				Path:   path,
				Record: rec,
			}
			unsortedRecords[keyStr] = append(unsortedRecords[keyStr], levelDBRecord)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	var allRecords []*LevelDBRecord
	// Process each group of records that share the same key.
	for keyStr, recordsForKey := range unsortedRecords {

		// --- NEW LOGGING: Log keys being grouped ---
		keyJSON, _ := json.Marshal(keyStr) // Escape the key string for logging
		fmt.Fprintf(os.Stderr, "Debug-DB: Grouping %d records for key: %s\n", len(recordsForKey), string(keyJSON))
		// ------------------------------------------

		sort.Slice(recordsForKey, func(i, j int) bool {
			if recordsForKey[i].Record.GetSequenceNumber() != recordsForKey[j].Record.GetSequenceNumber() {
				return recordsForKey[i].Record.GetSequenceNumber() < recordsForKey[j].Record.GetSequenceNumber()
			}
			return recordsForKey[i].Record.GetOffset() < recordsForKey[j].Record.GetOffset()
		})

		numRecords := len(recordsForKey)
		for i, rec := range recordsForKey {
			if i == numRecords-1 {
				rec.Recovered = false // Active record
			} else {
				rec.Recovered = true // Recovered record
			}
			allRecords = append(allRecords, rec)
		}
	}

	sort.Slice(allRecords, func(i, j int) bool {
		// Ensure consistent final sort order
		if allRecords[i].Record.GetSequenceNumber() != allRecords[j].Record.GetSequenceNumber() {
			return allRecords[i].Record.GetSequenceNumber() < allRecords[j].Record.GetSequenceNumber()
		}
		if allRecords[i].Record.GetOffset() != allRecords[j].Record.GetOffset() {
			return allRecords[i].Record.GetOffset() < allRecords[j].Record.GetOffset()
		}
		// Tie-break by path if sequence and offset are identical (unlikely but possible)
		return allRecords[i].Path < allRecords[j].Path
	})

	return allRecords, nil
}
