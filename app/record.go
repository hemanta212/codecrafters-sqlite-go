package main

import (
	"io"
	"log"
)

type Record struct {
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
	if serialType == 1 {
		// 8 bit two's complement
		return parseUInt8(stream)
	} else if serialType >= 13 && serialType%2 == 1 {
		// text
		textLen := (serialType - 13) / 2
		return parseString(stream, textLen)
	} else {
		log.Fatal("Record format seriaaltype decoder, MY CUSTOM not implemented")
		return -1
	}
}
