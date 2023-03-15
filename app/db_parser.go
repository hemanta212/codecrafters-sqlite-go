package main

import (
	"io"
	"log"
)

type DatabaseHeader struct {
	MagicHeader   string
	DbPageSize    uint16
	SQLiteVersion uint32
}

type PageHeader struct {
	PageType  byte
	NoOfCells uint16
}

type SchemaTable struct {
	_type      string
	name       string
	table_name string
	rootPage   uint8
	sql        string
}

func parsePageHeader(stream io.ReadSeeker) PageHeader {
	// 8-12 bytes page header immediately after the db header
	// todo; curretly unimportant data
	pageType := parseBytes(stream, 1)[0]
	log.Println("PageType:", pageType)
	stream.Seek(2, io.SeekCurrent)
	noOfCells := parseUInt16(stream)
	// todo to parse
	stream.Seek(3, io.SeekCurrent)

	// for internal pageType page header is 12 bytes
	lastOffset := 0
	if pageType == 2 || pageType == 5 {
		lastOffset = 4
	}
	stream.Seek(int64(lastOffset), io.SeekCurrent)

	return PageHeader{
		PageType:  pageType,
		NoOfCells: noOfCells,
	}
}

func parseDatabaseHeader(databaseFile io.ReadSeeker) DatabaseHeader {
	magicHeaderString := parseString(databaseFile, 16)
	dbPageSize := parseUInt16(databaseFile)
	// todo; currently unneccesary items
	_, _ = databaseFile.Seek(96, io.SeekStart)
	sqliteVersionNo := parseUInt32(databaseFile)
	return DatabaseHeader{
		MagicHeader:   magicHeaderString[0:15],
		DbPageSize:    dbPageSize,
		SQLiteVersion: sqliteVersionNo,
	}
}

func parseSqliteSchemaTable(record Record) SchemaTable {
	return SchemaTable{
		_type:      record.values[0].(string),
		name:       record.values[1].(string),
		table_name: record.values[2].(string),
		rootPage:   record.values[3].(uint8),
		sql:        record.values[4].(string),
	}
}

func parseRootPageSchemaTable(databaseFile io.ReadSeeker) []SchemaTable {
	// skip the db header
	databaseFile.Seek(100, io.SeekStart)

	pageHeader := parsePageHeader(databaseFile)

	cellPointers := make([]uint16, pageHeader.NoOfCells)
	for i := range cellPointers {
		cellPointers[i] = parseUInt16(databaseFile)
	}
	schemas := make([]SchemaTable, pageHeader.NoOfCells)
	for i, pointer := range cellPointers {
		databaseFile.Seek(int64(pointer), io.SeekStart)
		parseVarint(databaseFile) // payload size
		parseVarint(databaseFile) // primary key id

		record := parseRecord(databaseFile, 5)
		schemaTable := parseSqliteSchemaTable(record)
		schemas[i] = schemaTable
	}
	return schemas
}
