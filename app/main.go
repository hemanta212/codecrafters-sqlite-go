package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	if strings.HasPrefix(command, ".") {
		parseDotCommand(command, databaseFilePath)
	} else {
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal("Error opening db file")
		}
		dbHeader := parseDatabaseHeader(databaseFile)
		dbPageSize := dbHeader.DbPageSize
		sqlParts := strings.Split(command, " ")
		tableName := sqlParts[len(sqlParts)-1]

		schemaTables := parseRootPageSchemaTable(databaseFile)
		rootPage := -1
		for _, schemaTable := range schemaTables {
			if schemaTable.table_name == tableName {
				rootPage = int(schemaTable.rootPage)
			}
		}
		if rootPage == -1 {
			log.Fatalf("Table %s not found", tableName)
		}
		tablePageOffset := (rootPage - 1) * int(dbPageSize)
		databaseFile.Seek(int64(tablePageOffset), io.SeekStart)

		tablePageHeader := parsePageHeader(databaseFile)
		fmt.Println(tablePageHeader.NoOfCells)
	}
}
