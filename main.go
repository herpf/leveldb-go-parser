package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"leveldb-parser-go/leveldb/common"
	"leveldb-parser-go/leveldb/db"
	"leveldb-parser-go/leveldb/ldb"
	"leveldb-parser-go/leveldb/log"

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
)

func main() {
	// Determine which command was parsed
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case dbCmd.FullCommand():
		runDbCommand(*dbPath, *dbFormat, *dbOutputFile)
	case ldbCmd.FullCommand():
		runLdbCommand(*ldbPath, *ldbFormat, *ldbOutputFile)
	case logCmd.FullCommand():
		runLogCommand(*logPath, *logFormat, *logOutputFile)
	}
}

// getOutputWriter determines if the output should go to stdout or a file.
func getOutputWriter(outputFile string) (io.WriteCloser, error) {
	if outputFile != "" {
		return os.Create(outputFile)
	}
	return os.Stdout, nil
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

	// Handle JSON and JSONL output specifically for []*db.LevelDBRecord
	var outputRecords []map[string]interface{}
	for _, rec := range records {
		// Convert the inner common.Record to a JSON-friendly struct
		jsonInnerRec := common.ToJSONRecord(rec.Record)

		// Marshal/Unmarshal to get a mutable map
		jsonRecBytes, err := json.Marshal(jsonInnerRec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshalling record: %v\n", err)
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(jsonRecBytes, &m); err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling record to map: %v\n", err)
			continue
		}

		// Create a new map with the final nested structure
		finalRecord := map[string]interface{}{
			"path":      rec.Path,
			"record":    m,
			"recovered": rec.Recovered,
		}
		outputRecords = append(outputRecords, finalRecord)
	}

	if format == "jsonl" {
		for _, finalRec := range outputRecords {
			line, err := json.Marshal(finalRec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error marshalling final record to JSONL: %v\n", err)
				continue
			}
			fmt.Fprintln(writer, string(line))
		}
	} else {
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

	// Convert to common.Record interface for generic processing
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
		jsonRecBytes, err := json.Marshal(common.ToJSONRecord(rec))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshalling record: %v\n", err)
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(jsonRecBytes, &m); err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling record to map: %v\n", err)
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
	jsonRecBytes, err := json.Marshal(common.ToJSONRecord(rec))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshalling record: %v\n", err)
		return
	}
	var m map[string]interface{}
	if err := json.Unmarshal(jsonRecBytes, &m); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshalling record to map: %v\n", err)
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
