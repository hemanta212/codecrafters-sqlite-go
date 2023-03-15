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
	db, err := NewDatabase(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.databaseFile.Close()

	if strings.HasPrefix(command, ".") {
		db.parseDotCommand(command)
	} else {
		db.parseSQLCommand(command)
	}
}
