package main

import (
	"fmt"
	"os"
)

func (d *Database) parseDotCommand(command string) {
	switch command {
	case ".dbinfo":
		dbHeader := parseDatabaseHeader(d.databaseFile)
		fmt.Printf("%s.%d\ndatabase page size: %d\n",
			dbHeader.MagicHeader, dbHeader.SQLiteVersion, dbHeader.DbPageSize)

		pageHeader := parsePageHeader(d.databaseFile)
		fmt.Printf("number of tables: %d", pageHeader.NoOfCells)
	case ".tables":
		// print the tables
		schemaTables := parseRootPageSchemaTable(d.databaseFile)
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
