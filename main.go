package main

import (
	"encoding/json"
	"fmt"
	"os"

	"leveldb-parser-go/leveldb/common"
	"leveldb-parser-go/leveldb/db"
	"leveldb-parser-go/leveldb/ldb"
	"leveldb-parser-go/leveldb/log"

	"github.com/alecthomas/kingpin/v2"
)

var (
	app = kingpin.New("leveldb-parser-go", "A tool for digital forensic analysis of LevelDB files.")

	// DB command: process an entire directory.
	dbCmd          = app.Command("db", "Parse a directory as leveldb.")
	dbSource       = dbCmd.Arg("source", "The source leveldb directory.").Required().String()
	dbOutputFormat = dbCmd.Flag("output", "Output format.").Default("json").Enum("json", "jsonl")

	// LOG command: process a single .log file.
	logCmd          = app.Command("log", "Parse a leveldb log file.")
	logSource       = logCmd.Arg("source", "The source log file.").Required().String()
	logOutputFormat = logCmd.Flag("output", "Output format.").Default("json").Enum("json", "jsonl")

	// LDB command: process a single .ldb or .sst file.
	ldbCmd          = app.Command("ldb", "Parse a leveldb table (.ldb or .sst) file.")
	ldbSource       = ldbCmd.Arg("source", "The source ldb file.").Required().String()
	ldbOutputFormat = ldbCmd.Flag("output", "Output format.").Default("json").Enum("json", "jsonl")
)

func main() {
	app.HelpFlag.Short('h')
	// Parse the command-line arguments and run the appropriate command.
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	switch command {
	case dbCmd.FullCommand():
		runDbCommand(*dbSource, *dbOutputFormat)
	case logCmd.FullCommand():
		runLogCommand(*logSource, *logOutputFormat)
	case ldbCmd.FullCommand():
		runLdbCommand(*ldbSource, *ldbOutputFormat)
	}
}

// Handles the logic for the 'db' command.
func runDbCommand(source, outputFormat string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LevelDB directory: %s\n", source)
	reader, err := db.NewFolderReader(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	records, err := reader.GetRecords()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting records from folder: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Found %d total records.\n\n", len(records))

	// Convert each record to its JSON-friendly representation before printing.
	for _, rec := range records {
		jsonRec := db.JSONLevelDBRecord{
			Path:      rec.Path,
			Recovered: rec.Recovered,
			Record:    common.ToJSONRecord(rec.Record),
		}
		printOutput(jsonRec, outputFormat)
	}
}

// Handles the logic for the 'log' command.
func runLogCommand(source, outputFormat string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LOG file: %s\n", source)
	reader := log.NewFileReader(source)
	records, err := reader.GetParsedInternalKeys()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing LOG records: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "✅ Found %d internal key records.\n\n", len(records))
	for i := range records {
		printOutput(common.ToJSONRecord(&records[i]), outputFormat)
	}
}

// Handles the logic for the 'ldb' command.
func runLdbCommand(source, outputFormat string) {
	fmt.Fprintf(os.Stderr, "🔎 Parsing LDB file: %s\n", source)
	reader, err := ldb.NewFileReader(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	records, err := reader.GetKeyValueRecords()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing LDB records: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "✅ Found %d key-value records.\n\n", len(records))
	for i := range records {
		printOutput(common.ToJSONRecord(&records[i]), outputFormat)
	}
}

// printOutput marshals the given data to the specified JSON format and prints it.
func printOutput(data interface{}, format string) {
	var output []byte
	var err error

	switch format {
	case "json":
		// Indented JSON for human readability.
		output, err = json.MarshalIndent(data, "", "  ")
	case "jsonl":
		// JSONL (JSON Lines) for machine processing.
		output, err = json.Marshal(data)
	default:
		fmt.Fprintf(os.Stderr, "Unknown output format: %s\n", format)
		return
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling to JSON: %v\n", err)
		return
	}
	// Print the final JSON to standard output.
	fmt.Println(string(output))
}
