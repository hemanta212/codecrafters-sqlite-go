package main

import (
	"fmt"
	"os"
)

func (db *Database) parseDotCommand(command string) {
	switch command {
	case ".dbinfo":
		fmt.Printf("%s.%d\ndatabase page size: %d\n",
			db.dbHeader.MagicHeader, db.dbHeader.SQLiteVersion, db.dbHeader.DbPageSize)
		pageHeader := parsePageHeader(db.databaseFile)
		fmt.Printf("number of tables: %d", pageHeader.NoOfCells)
	case ".tables":
		// print the tables
		schemaTables := parseRootPageSchemaTable(db.databaseFile)
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
