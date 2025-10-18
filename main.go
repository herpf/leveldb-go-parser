package main

import (
	"encoding/json"
	"fmt"
	"io"
	"leveldb-parser-go/indexeddb"
	"leveldb-parser-go/leveldb/common"
	"leveldb-parser-go/leveldb/db"
	"leveldb-parser-go/leveldb/ldb"
	"leveldb-parser-go/leveldb/log"
	"os"
	"sort"

	"runtime/debug"

	"github.com/alecthomas/kingpin/v2"
)

var (
	app = kingpin.New("leveldb-parser-go", "A Go tool for forensic analysis of LevelDB files.")

	// DB command
	dbCmd        = app.Command("db", "Parse a LevelDB directory.")
	dbPath       = dbCmd.Arg("path", "Path to the LevelDB directory.").Required().String()
	dbFormat     = dbCmd.Flag("format", "Output format ('json' or 'jsonl').").Default("json").Enum("json", "jsonl")
	dbOutputFile = dbCmd.Flag("output-file", "Save output to a file.").Short('o').String()

	// LDB command
	ldbCmd        = app.Command("ldb", "Parse a single .ldb table file.")
	ldbPath       = ldbCmd.Arg("path", "Path to the .ldb file.").Required().String()
	ldbFormat     = ldbCmd.Flag("format", "Output format ('json' or 'jsonl').").Default("json").Enum("json", "jsonl")
	ldbOutputFile = ldbCmd.Flag("output-file", "Save output to a file.").Short('o').String()

	// LOG command
	logCmd        = app.Command("log", "Parse a single .log file.")
	logPath       = logCmd.Arg("path", "Path to the .log file.").Required().String()
	logFormat     = logCmd.Flag("format", "Output format ('json' or 'jsonl').").Default("json").Enum("json", "jsonl")
	logOutputFile = logCmd.Flag("output-file", "Save output to a file.").Short('o').String()

	indexedDbCmd        = app.Command("indexeddb", "Parse a Chromium IndexedDB directory.")
	indexedDbPath       = indexedDbCmd.Arg("path", "Path to the IndexedDB (LevelDB) directory.").Required().String()
	indexedDbFormat     = indexedDbCmd.Flag("format", "Output format ('json' or 'jsonl').").Default("json").Enum("json", "jsonl")
	indexedDbOutputFile = indexedDbCmd.Flag("output-file", "Save output to a file.").Short('o').String()
)

func main() {
	debug.SetTraceback("crash") // Enables full stack trace on panic
	// Determine which command was parsed
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case dbCmd.FullCommand():
		runDbCommand(*dbPath, *dbFormat, *dbOutputFile)
	case ldbCmd.FullCommand():
		runLdbCommand(*ldbPath, *ldbFormat, *ldbOutputFile)
	case logCmd.FullCommand():
		runLogCommand(*logPath, *logFormat, *logOutputFile)
	case indexedDbCmd.FullCommand():
		runIndexedDBCommand(*indexedDbPath, *indexedDbFormat, *indexedDbOutputFile)
	}
}

// getOutputWriter determines if the output should go to stdout or a file.
func getOutputWriter(outputFile string) (io.WriteCloser, error) {
	if outputFile != "" {
		return os.Create(outputFile)
	}
	return os.Stdout, nil
}

func runIndexedDBCommand(path, format, outputFile string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing IndexedDB directory: %s\n", path)
	reader, err := indexeddb.NewFolderReader(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating IndexedDB folder reader: %v\n", err)
		os.Exit(1)
	}

	records, err := reader.GetRecords()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting IndexedDB records: %v\n", err)
		os.Exit(1)
	}

	// Sort by sequence number for chronological order
	sort.Slice(records, func(i, j int) bool {
		seqI := records[i].SequenceNumber
		seqJ := records[j].SequenceNumber
		if seqI != seqJ {
			return seqI < seqJ
		}
		return records[i].Offset < records[j].Offset
	})

	writer, err := getOutputWriter(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	if format == "jsonl" {
		for _, rec := range records {
			line, err := json.Marshal(rec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error marshalling record to JSONL: %v\n", err)
				continue
			}
			fmt.Fprintln(writer, string(line))
		}
	} else {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(records); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		}
	}

	if outputFile != "" {
		fmt.Fprintf(os.Stderr, "✅ Output successfully saved to %s\n", outputFile)
	}
}

func runDbCommand(path, format, outputFile string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LevelDB directory: %s\n", path)
	reader, err := db.NewFolderReader(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating folder reader: %v\n", err)
		os.Exit(1)
	}

	records, err := reader.GetRecords()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Sort records for consistent output by accessing the nested Record field.
	sort.Slice(records, func(i, j int) bool {
		if records[i].Record.GetSequenceNumber() != records[j].Record.GetSequenceNumber() {
			return records[i].Record.GetSequenceNumber() < records[j].Record.GetSequenceNumber()
		}
		return records[i].Record.GetOffset() < records[j].Record.GetOffset()
	})

	writer, err := getOutputWriter(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	// Convert the slice of LevelDBRecord structs to a slice of maps for consistent marshalling.
	outputRecords := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		var recordAsMap map[string]interface{}

		// Use a type switch to manually build the map for each concrete type.
		switch v := rec.Record.(type) {
		case *common.KeyValueRecord:
			recordAsMap = map[string]interface{}{
				"offset":          v.GetOffset(),
				"key":             common.BytesToEscapedString(v.Key),
				"value":           common.BytesToEscapedString(v.Value),
				"sequence_number": v.GetSequenceNumber(),
				"record_type":     v.RecordType,
			}
		case *common.ParsedInternalKey:
			recordAsMap = map[string]interface{}{
				"offset":          v.GetOffset(),
				"key":             common.BytesToEscapedString(v.Key),
				"value":           common.BytesToEscapedString(v.Value),
				"sequence_number": v.GetSequenceNumber(),
				"record_type":     v.RecordType,
			}
		default:
			fmt.Fprintf(os.Stderr, "Warning: unknown record type %T\n", v)
			continue
		}

		recordMap := map[string]interface{}{
			"path":      rec.Path,
			"record":    recordAsMap,
			"recovered": rec.Recovered,
		}
		outputRecords = append(outputRecords, recordMap)
	}

	if format == "jsonl" {
		for _, rec := range outputRecords {
			line, err := json.Marshal(rec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error marshalling record to JSONL: %v\n", err)
				continue
			}
			fmt.Fprintln(writer, string(line))
		}
	} else {
		// For 'json' format, encode the entire slice as a pretty-printed array.
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(outputRecords); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		}
	}

	if outputFile != "" {
		fmt.Fprintf(os.Stderr, "✅ Output successfully saved to %s\n", outputFile)
	}
}

func runLdbCommand(path, format, outputFile string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LDB file: %s\n", path)
	reader, err := ldb.NewFileReader(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	records, err := reader.GetKeyValueRecords()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing LDB records: %v\n", err)
		os.Exit(1)
	}

	var genericRecords []common.Record
	for i := range records {
		genericRecords = append(genericRecords, &records[i])
	}

	writer, err := getOutputWriter(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	if format == "jsonl" {
		for _, rec := range genericRecords {
			printRecordJSONL(rec, path, writer)
		}
	} else {
		printRecordsJSON(genericRecords, path, writer)
	}

	if outputFile != "" {
		fmt.Fprintf(os.Stderr, "✅ Output successfully saved to %s\n", outputFile)
	}
}

func runLogCommand(path, format, outputFile string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LOG file: %s\n", path)
	reader := log.NewFileReader(path)
	records, err := reader.GetParsedInternalKeys()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing LOG records: %v\n", err)
		os.Exit(1)
	}

	var genericRecords []common.Record
	for i := range records {
		genericRecords = append(genericRecords, &records[i])
	}

	writer, err := getOutputWriter(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	if format == "jsonl" {
		for _, rec := range genericRecords {
			printRecordJSONL(rec, path, writer)
		}
	} else {
		printRecordsJSON(genericRecords, path, writer)
	}

	if outputFile != "" {
		fmt.Fprintf(os.Stderr, "✅ Output successfully saved to %s\n", outputFile)
	}
}

// printRecordsJSON prints a slice of records as a single, pretty-printed JSON array.
func printRecordsJSON(records []common.Record, pathForFiles string, writer io.Writer) {
	var outputRecords []map[string]interface{}
	for _, rec := range records {
		var m map[string]interface{}
		// Use a type switch to manually build the map for each concrete type.
		switch v := rec.(type) {
		case *common.KeyValueRecord:
			m = map[string]interface{}{
				"offset":          v.GetOffset(),
				"key":             common.BytesToEscapedString(v.Key),
				"value":           common.BytesToEscapedString(v.Value),
				"sequence_number": v.GetSequenceNumber(),
				"record_type":     v.RecordType,
			}
		case *common.ParsedInternalKey:
			m = map[string]interface{}{
				"offset":          v.GetOffset(),
				"key":             common.BytesToEscapedString(v.Key),
				"value":           common.BytesToEscapedString(v.Value),
				"sequence_number": v.GetSequenceNumber(),
				"record_type":     v.RecordType,
			}
		default:
			fmt.Fprintf(os.Stderr, "Warning: unknown record type in printRecordsJSON: %T\n", v)
			continue
		}

		if pathForFiles != "" {
			m["path"] = pathForFiles
		}
		outputRecords = append(outputRecords, m)
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(outputRecords); err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
	}
}

// printRecordJSONL prints a single record as a one-line JSON object.
func printRecordJSONL(rec common.Record, pathForFile string, writer io.Writer) {
	var m map[string]interface{}

	switch v := rec.(type) {
	case *common.KeyValueRecord:
		m = map[string]interface{}{
			"offset":          v.GetOffset(),
			"key":             common.BytesToEscapedString(v.Key),
			"value":           common.BytesToEscapedString(v.Value),
			"sequence_number": v.GetSequenceNumber(),
			"record_type":     v.RecordType,
		}
	case *common.ParsedInternalKey:
		m = map[string]interface{}{
			"offset":          v.GetOffset(),
			"key":             common.BytesToEscapedString(v.Key),
			"value":           common.BytesToEscapedString(v.Value),
			"sequence_number": v.GetSequenceNumber(),
			"record_type":     v.RecordType,
		}
	default:
		fmt.Fprintf(os.Stderr, "Warning: unknown record type in printRecordJSONL: %T\n", v)
		return
	}

	if pathForFile != "" {
		m["path"] = pathForFile
	}

	line, err := json.Marshal(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshalling final record to JSONL: %v\n", err)
		return
	}
	fmt.Fprintln(writer, string(line))
}
