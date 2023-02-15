package main

import (
	"fmt"
	"log"
	"os"
)

func parseDotCommand(command, databaseFilePath string) {
	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal("Cannot open the db file", err)
		}

		dbHeader := parseDatabaseHeader(databaseFile)
		fmt.Printf("%s.%d\nDatabase Page Size: %d\n",
			dbHeader.MagicHeader, dbHeader.SQLiteVersion, dbHeader.DbPageSize)

		pageHeader := parsePageHeader(databaseFile)
		fmt.Printf("No of Tables: %d", pageHeader.NoOfCells)

	case ".tables":
		// print the tables
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal("Cannot open the db file", err)
		}
		schemaTables := parseRootPageSchemaTable(databaseFile)
		for _, schemaTable := range schemaTables {
			if schemaTable.table_name == "sqlite_sequence" {
				continue
			}
			fmt.Print(schemaTable.table_name, " ")
		}
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
