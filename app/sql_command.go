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
	log.Println("Sql Command: executeSelectStmt: stmt", selectStmt)
	tableNames := selectStmt.TableNames

	dbHeader := parseDatabaseHeader(databaseFile)
	dbPageSize := dbHeader.DbPageSize
	schemaTables := parseRootPageSchemaTable(databaseFile)
	for _, schemaTable := range schemaTables {
		for _, tableName := range tableNames {
			if tableName == schemaTable.name {
				getTableInfo(selectStmt, schemaTable, databaseFile, dbPageSize)
			}
		}
	}
}

func getTableInfo(stmt *parser.SelectStmt, tableSchemaTable SchemaTable, databaseFile io.ReadSeeker, dbPageSize uint16) {
	rootPage := int(tableSchemaTable.rootPage)
	tablePageOffset := (rootPage - 1) * int(dbPageSize)
	databaseFile.Seek(int64(tablePageOffset), io.SeekStart)

	tablePageHeader := parsePageHeader(databaseFile)
	for _, function := range stmt.Functions {
		if function.Name == "count" {
			fmt.Println(tablePageHeader.NoOfCells)
		} else {
			fmt.Printf("Not implemented Error, Select Function %q\n", function.Name)
		}
	}

	cellPointers := make([]uint16, tablePageHeader.NoOfCells)
	for i := range cellPointers {
		cellPointers[i] = parseUInt16(databaseFile)
	}

	sqlStmt, err := parser.NewParser(tableSchemaTable.sql).Parse()
	if err != nil {
		log.Fatalf("Error parsing sql for table %q %q", tableSchemaTable.table_name, tableSchemaTable.sql)
	}
	createSqlStmt := sqlStmt.(*parser.CreateStmt)
	columnNameOrder := arrangeColumnNameOrder(createSqlStmt)

	log.Printf("%+v\n", createSqlStmt)
	records := []Record{}
	recordLen := len(createSqlStmt.Function.Arguments) - 1
	for _, pointer := range cellPointers {
		pointerOffset := tablePageOffset + int(pointer)
		// fmt.Println(pointerOffset)
		databaseFile.Seek(int64(pointerOffset), io.SeekStart)
		parseVarint(databaseFile) // payload size
		// id of row
		parseVarint(databaseFile)
		parseVarint(databaseFile)

		record := parseRecord(databaseFile, recordLen)
		log.Printf("%+v\n", record)
		records = append(records, record)
	}
	columnNames := stmt.ColumnNames
	// what to print for each row eg -> 0: [name1, color1, id1],  1: [name2, color2, id2]
	resultsToPrint := map[int][]string{}
	for i, record := range records {
		for _, columnName := range columnNames {
			columnOrder, exist := columnNameOrder[columnName]
			if !exist {
				log.Fatalf("Cannot find column %q in table %q", columnName, tableSchemaTable.table_name)
			}
			value := record.values[columnOrder].(string)
			resultsToPrint[i] = append(resultsToPrint[i], value)
		}
	}
	for _, value := range resultsToPrint {
		for i, val := range value {
			if i == len(value)-1 {
				fmt.Printf("%s", val)
			} else {
				fmt.Printf("%s|", val)
			}
		}
		fmt.Println()
	}
}

func arrangeColumnNameOrder(stmt *parser.CreateStmt) map[string]int {
	result := map[string]int{}
	for i, dt := range stmt.Function.Arguments {
		// id(i=0) is already out so compensating for that the correct order will be i-1
		order := i - 1
		result[dt.Name] = order
	}
	return result
}
