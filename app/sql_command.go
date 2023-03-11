package main

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/parser"
	"io"
	"log"
	"os"
)

func parseSQLCommand(command, databaseFilePath string) {
	databaseFile, err := os.Open(databaseFilePath)
	defer databaseFile.Close()
	if err != nil {
		log.Fatal("Error opening db file")
	}
	stmt, err := parser.NewParser(command).Parse()
	if err != nil {
		log.Fatal(err)
	}
	_, Sok := stmt.(*parser.SelectStmt)

	if Sok {
		executeSelectStmt(stmt, databaseFile)
	} else {
		fmt.Println("Not Implememented")
	}
}

func executeSelectStmt(stmt interface{}, databaseFile io.ReadSeeker) {
	selectStmt := stmt.(*parser.SelectStmt)
	log.Println("Sql Commant: executeSelectStmt: stmt", selectStmt)
	tableName := selectStmt.TableNames[0]
	function := selectStmt.Functions[0]

	dbHeader := parseDatabaseHeader(databaseFile)
	dbPageSize := dbHeader.DbPageSize
	schemaTables := parseRootPageSchemaTable(databaseFile)
	rootPage := -1
	for _, schemaTable := range schemaTables {
		if schemaTable.table_name == tableName {
			rootPage = int(schemaTable.rootPage)
			// fmt.Println(schemaTable.sql)
		}
	}
	if rootPage == -1 {
		log.Fatalf("Table %s not found", tableName)
	}
	tablePageOffset := (rootPage - 1) * int(dbPageSize)
	databaseFile.Seek(int64(tablePageOffset), io.SeekStart)

	tablePageHeader := parsePageHeader(databaseFile)
	if function.Name == "count" {
		fmt.Println(tablePageHeader.NoOfCells)
		return
	}
	// cellPointers := make([]uint16, tablePageHeader.NoOfCells)
	// for i := range cellPointers {
	// 	cellPointers[i] = parseUInt16(databaseFile)
	// }

	// // fmt.Println(cellPointers, tablePageOffset)
	// for _, pointer := range cellPointers {
	// 	pointerOffset := tablePageOffset + int(pointer)
	// 	// fmt.Println(pointerOffset)
	// 	databaseFile.Seek(int64(pointerOffset), io.SeekStart)
	// 	parseVarint(databaseFile) // payload size
	// 	parseVarint(databaseFile) // primary key id

	// 	record := parseRecord(databaseFile, 3)
	// 	// fmt.Printf("%v\n", record)
	// }
}
