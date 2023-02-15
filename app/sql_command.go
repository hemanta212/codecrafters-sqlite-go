package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func parseSQLCommand(command, databaseFilePath string) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal("Error opening db file")
	}
	dbHeader := parseDatabaseHeader(databaseFile)
	dbPageSize := dbHeader.DbPageSize
	sqlParts := strings.Split(command, " ")
	tableName := strings.ToLower(sqlParts[len(sqlParts)-1])
	attribute := strings.ToLower(sqlParts[1])

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
	if strings.Contains(attribute, "count") {
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
