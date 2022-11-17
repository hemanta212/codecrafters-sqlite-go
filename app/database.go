package main

import (
	"fmt"
	"io"
	"os"
)

type SQLiteSchemaRow struct {
	_type    string // _type since type is a reserved keyword
	name     string
	tblName  string
	rootPage int
	sql      string
}

func parseMainPage(databaseFilePath string) ([]SQLiteSchemaRow, error) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		return nil, err
	}

	headerString := parseString(databaseFile, 16)
	fmt.Println(headerString)

	_, _ = databaseFile.Seek(96, io.SeekStart) // Read the last header info about sqlite version number
	sqlite3VersionNumber := parseUInt32(databaseFile)
	fmt.Println("Sqlite3 Version Number: ", sqlite3VersionNumber)

	_, _ = databaseFile.Seek(100, io.SeekStart)
	return parseSqliteSchemaRows(databaseFile)
}

func parseSqliteSchemaRows(databaseFile *os.File) ([]SQLiteSchemaRow, error) {
	pageHeader := parsePageHeader(databaseFile)

	cellPointers := make([]uint16, pageHeader.NumberOfCells)

	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		cellPointers[i] = parseUInt16(databaseFile)
	}

	var sqliteSchemaRows []SQLiteSchemaRow

	for _, cellPointer := range cellPointers {
		_, _ = databaseFile.Seek(int64(cellPointer), io.SeekStart)
		parseVarint(databaseFile) // number of bytes in payload
		parseVarint(databaseFile) // rowid
		record := parseRecord(databaseFile, 5)

		sqliteSchemaRows = append(sqliteSchemaRows, SQLiteSchemaRow{
			_type:    string(record.values[0].([]byte)),
			name:     string(record.values[1].([]byte)),
			tblName:  string(record.values[2].([]byte)),
			rootPage: int(record.values[3].(uint8)),
			sql:      string(record.values[4].([]byte)),
		})
	}
	return sqliteSchemaRows, nil
}

func getPageSize(databaseFile *os.File) uint16 {
	// Take the page length (2-byte int at offset of 16 bytes)
	// https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
	_, _ = databaseFile.Seek(16, io.SeekStart)
	pageSize := parseUInt16(databaseFile)
	return pageSize
}
