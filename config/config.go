package config

import (
	"io"
	"log"
	"os"
)

var VerboseLogger *log.Logger

func InitLogger(isVerbose bool) {
	var output io.Writer

	if isVerbose {
		output = os.Stderr
	} else {
		output = io.Discard
	}

	VerboseLogger = log.New(output, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
}
