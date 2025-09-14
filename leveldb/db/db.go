package db

import (
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
	// A map to hold all versions of a record, keyed by the record's primary key.
	unsortedRecords := make(map[string][]*LevelDBRecord)

	// Walk the directory to find all .log and .ldb files.
	err := filepath.WalkDir(fr.folderPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil // Skip directories
		}

		ext := strings.ToLower(filepath.Ext(path))
		var currentRecords []common.Record

		switch ext {
		case ".log":
			reader := log.NewFileReader(path)
			parsedKeys, parseErr := reader.GetParsedInternalKeys()
			if parseErr != nil {
				// Log errors but continue processing other files.
				fmt.Fprintf(os.Stderr, "Warning: could not parse log file %s: %v\n", path, parseErr)
				return nil
			}
			for i := range parsedKeys {
				currentRecords = append(currentRecords, &parsedKeys[i])
			}
		case ".ldb", ".sst":
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
			// Ignore other files like MANIFEST, CURRENT, LOCK, etc.
			return nil
		}

		// Group records by their key.
		for _, rec := range currentRecords {
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
	for _, recordsForKey := range unsortedRecords {
		// Sort by sequence number, then by file offset as a tie-breaker.
		sort.Slice(recordsForKey, func(i, j int) bool {
			if recordsForKey[i].Record.GetSequenceNumber() != recordsForKey[j].Record.GetSequenceNumber() {
				return recordsForKey[i].Record.GetSequenceNumber() < recordsForKey[j].Record.GetSequenceNumber()
			}
			return recordsForKey[i].Record.GetOffset() < recordsForKey[j].Record.GetOffset()
		})

		// The last record in the sorted list is the most recent (active) version.
		// All previous versions are considered "recovered".
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

	// Final sort of all records for a consistent and chronological output order.
	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].Record.GetSequenceNumber() < allRecords[j].Record.GetSequenceNumber()
	})

	return allRecords, nil
}
