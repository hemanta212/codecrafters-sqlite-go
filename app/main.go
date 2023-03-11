package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	log.SetOutput(ioutil.Discard)
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	if strings.HasPrefix(command, ".") {
		parseDotCommand(command, databaseFilePath)
	} else {
		parseSQLCommand(command, databaseFilePath)
	}
}
