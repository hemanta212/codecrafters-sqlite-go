package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/pingcap/parser"
	// "github.com/pingcap/parser/ast"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	if len(command) == 0 {
		fmt.Println("Usage: your_sqlite3.sh sample.db .dbinfo")
		return
	}
	if command[0] == '.' {
		parseDotCommands(databaseFilePath, command)
	} else {
		parseSqlCommand(databaseFilePath, command)
	}
}

func parseSqlCommand(databaseFilePath, command string) {
	commandParts := strings.Split(command, " ")
	tableName := commandParts[len(commandParts)-1]
	fmt.Println("Got tableName: ", tableName)

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	_, _ = databaseFile.Seek(100, io.SeekStart)
	sqliteSchemaRows, err := parseSqliteSchemaRows(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	pageSize := int64(getPageSize(databaseFile))
	rootPage := -1
	for _, row := range sqliteSchemaRows {
		if row.tblName == tableName {
			rootPage = row.rootPage
		}
	}
	seekAt := (rootPage - 1) * int(pageSize)

	_, _ = databaseFile.Seek(int64(seekAt), io.SeekStart) // Skip the database header
	pageHeader := parsePageHeader(databaseFile)
	fmt.Println(pageHeader.NumberOfCells)
}

func parseDotCommands(databaseFilePath, command string) {
	switch command {
	case ".dbinfo":
		sqliteSchemaRows, err := parseMainPage(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		// Uncomment this to pass the first stage
		fmt.Printf("number of tables: %v", len(sqliteSchemaRows))

	case ".tables":
		sqliteSchemaRows, err := parseMainPage(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(":: Tables")
		for _, row := range sqliteSchemaRows {
			if row.tblName == "sqlite_sequence" {
				continue
			}
			fmt.Print(string(row.tblName), " ")
		}
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
