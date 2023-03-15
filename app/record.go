package main

import (
	"io"
	"log"
)

type Record struct {
	id     int
	values []interface{}
}

func parseRecord(stream io.Reader, valuesCount int) Record {
	parseVarint(stream) // size of record header

	serialTypes := make([]int, valuesCount)
	for i := 0; i < valuesCount; i++ {
		serialTypes[i] = parseVarint(stream)
	}
	values := make([]interface{}, valuesCount)
	for i, _type := range serialTypes {
		values[i] = parseRecordValues(stream, _type)
	}
	return Record{values: values}
}

func parseRecordValues(stream io.Reader, serialType int) interface{} {
	if serialType == 0 {
		return nil
	} else if serialType == 1 {
		// 8 bit two's complement
		return parseUInt8(stream)
	} else if serialType == 2 {
		// 16 bit two's complement
		return parseUInt16(stream)
	} else if serialType == 4 {
		// 32 bit big endian two's complement
		return parseUInt32(stream)
	} else if serialType == 6 {
		// 64 bit big endian two's complement
		return parseUInt64(stream)
	} else if serialType == 9 {
		return 1
	} else if serialType >= 13 && serialType%2 == 1 {
		// text
		textLen := (serialType - 13) / 2
		return parseString(stream, textLen)
	} else if serialType >= 12 && serialType%2 == 0 {
		// text
		blobLen := (serialType - 12) / 2
		return parseBytes(stream, blobLen)
	} else {
		log.Fatalf("Record format of serialType %d decoder, MY CUSTOM not implemented", serialType)
		return -1
	}
}
