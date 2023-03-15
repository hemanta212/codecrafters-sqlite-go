package main

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/parser"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type Database struct {
	databaseFile io.ReadSeekCloser
	dbHeader     DatabaseHeader
	pageHeader   PageHeader
}

func NewDatabase(databaseFilePath string) (*Database, error) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		return nil, err
	}
	dbHeader := parseDatabaseHeader(databaseFile)

	return &Database{
		databaseFile: databaseFile,
		dbHeader:     dbHeader,
		pageHeader:   PageHeader{},
	}, nil
}

func (db *Database) parseSQLCommand(command string) {
	stmt, err := parser.NewParser(command).Parse()
	if err != nil {
		log.Fatal(err)
	}
	_, Sok := stmt.(*parser.SelectStmt)

	if Sok {
		db.executeSelectStmt(stmt)
	} else {
		fmt.Println("Not Implememented")
	}
}

func (db *Database) executeSelectStmt(stmt interface{}) {
	selectStmt := stmt.(*parser.SelectStmt)
	log.Println("Sql Command: executeSelectStmt: stmt", selectStmt)
	tableNames := selectStmt.TableNames

	schemaTables := parseRootPageSchemaTable(db.databaseFile)
	for _, schemaTable := range schemaTables {
		for _, tableName := range tableNames {
			if tableName == schemaTable.name {
				db.getTableInfo(selectStmt, schemaTable)
			}
		}
	}
}

func (db *Database) getTableInfo(stmt *parser.SelectStmt, tableSchema SchemaTable) {
	// execute functions
	db.executeFunctions(stmt.Functions, tableSchema)

	// parse columnNames from db
	columns := parseColumnsFromCreateSQL(tableSchema)

	// get records
	records := db.getRecords(int(tableSchema.rootPage), columns)

	// filter records if any filters present
	records, err := db.filterRecords(records, stmt, columns)
	if err != nil {
		log.Fatalf("Error processing table %q, %s", tableSchema.table_name, err)
	}

	columnNames := stmt.ColumnNames
	// what to print for each row eg -> 0: [name1, color1, id1],  1: [name2, color2, id2]
	resultsToPrint := map[int][]string{}
	for i, record := range records {
		for _, columnName := range columnNames {
			columnOrder, exist := columns[columnName]
			if !exist {
				log.Fatalf("Cannot find column %q in table %q", columnName, tableSchema.table_name)
			}
			value := ""
			if columnOrder == -1 {
				// not in record.values since it is primary id
				value = strconv.Itoa(record.id)
			} else {
				value = record.values[columnOrder].(string)
			}
			resultsToPrint[i] = append(resultsToPrint[i], value)
		}
	}

	for i, _ := range records {
		value, ok := resultsToPrint[i]
		if !ok {
			continue
		}
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

func (db *Database) getRecords(tablePage int, columns map[string]int) []Record {
	tablePageOffset := (tablePage - 1) * int(db.dbHeader.DbPageSize)
	cellPointers := db.parseCellPointersFromPage(tablePageOffset)

	records := []Record{}
	recordLen := len(columns) - 1
	log.Println("Columns:", recordLen)

	pageType := db.pageHeader.PageType
	log.Println("PageType:", pageType)

	for _, pointer := range cellPointers {
		pointerOffset := tablePageOffset + int(pointer)
		db.databaseFile.Seek(int64(pointerOffset), io.SeekStart)

		switch pageType {
		case 13:
			id := scanCellHeader(db.pageHeader, db.databaseFile)
			record := parseRecord(db.databaseFile, recordLen)
			record.id = id
			log.Printf("%+v\n", record)
			records = append(records, record)
		case 5:
			leftChildPointer := parseUInt32(db.databaseFile)
			parseVarint(db.databaseFile) // int key
			newRecords := db.getRecords(int(leftChildPointer), columns)
			records = append(records, newRecords...)
		default:
			log.Fatal("Unknown page Type, Implement for type", pageType)
		}
	}
	return records
}

func (db *Database) executeFunctions(functions []parser.SqlFunction, schemaTable SchemaTable) {
	rootPage := int(schemaTable.rootPage)
	log.Println("SQLCmd: exeFunctions: rootPage", rootPage)
	tablePageOffset := (rootPage - 1) * int(db.dbHeader.DbPageSize)
	db.databaseFile.Seek(int64(tablePageOffset), io.SeekStart)

	db.pageHeader = parsePageHeader(db.databaseFile)
	for _, function := range functions {
		if function.Name == "count" {
			fmt.Println(db.pageHeader.NoOfCells)
		} else {
			fmt.Printf("Not implemented Error, Select Function %q\n", function.Name)
		}
	}
}

func (db *Database) filterRecords(records []Record, stmt *parser.SelectStmt, columns map[string]int) ([]Record, error) {
	if len(stmt.Filters) <= 0 {
		return records, nil
	}

	results := []Record{}
	for _, record := range records {
		for columnName, FilterValue := range stmt.Filters {
			columnOrder, exist := columns[columnName]
			if !exist {
				return nil, fmt.Errorf("Cannot find column %q", columnName)
			}
			valueRaw := record.values[columnOrder]
			if valueRaw == nil {
				continue
			}
			value := record.values[columnOrder].(string)
			if FilterValue.Value == strings.ToLower(value) {
				results = append(results, record)
			}
		}
	}
	return results, nil
}

func scanCellHeader(tablePageHeader PageHeader, databaseFile io.ReadSeeker) int {
	parseVarint(databaseFile)       // pay load
	id := parseVarint(databaseFile) // primary key int
	parseVarint(databaseFile)       // initial portion of payload
	return id
}

func (db *Database) parseCellPointersFromPage(dbPageOffset int) []uint16 {
	db.databaseFile.Seek(int64(dbPageOffset), io.SeekStart)

	db.pageHeader = parsePageHeader(db.databaseFile)

	cellPointers := make([]uint16, db.pageHeader.NoOfCells)
	for i := range cellPointers {
		cellPointers[i] = parseUInt16(db.databaseFile)
	}
	log.Println("SQLCmd: parseCellPointers: cellPointers", cellPointers)
	return cellPointers
}

func parseColumnsFromCreateSQL(tableSchema SchemaTable) map[string]int {
	sqlStmt, err := parser.NewParser(tableSchema.sql).Parse()
	if err != nil {
		log.Fatalf("Error parsing sql for table %q %q", tableSchema.table_name, tableSchema.sql)
	}

	createSqlStmt := sqlStmt.(*parser.CreateStmt)
	columnNameOrder := arrangeColumnNameOrder(createSqlStmt)
	log.Printf("%+v\n", createSqlStmt)
	return columnNameOrder
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
