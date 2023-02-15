package main

import (
	"encoding/binary"
	"io"
	"log"
)

const (
	IS_FIRST_BIT_ZERO_MASK = 0b10000000
	LAST_SEVEN_BITS_MASK   = 0b01111111
)

func parseVarint(stream io.Reader) int {
	result := 0

	for index, scannedInt := range parseVarintAsUint8(stream) {
		var shiftBy int
		if index == 8 {
			shiftBy = 8
		} else {
			shiftBy = 7
		}
		shifted := result << shiftBy
		usableValue := parseUsableVarintValue(scannedInt, shiftBy)
		result = shifted + usableValue
	}
	return result
}

func parseVarintAsUint8(stream io.Reader) []uint8 {
	var result []uint8
	// maximum varint is of 8 bytes, minimum is of 1 byte
	for i := 0; i < 9; i++ {
		scannedUint8 := parseUInt8(stream)
		result = append(result, scannedUint8)
		if scannedUint8&IS_FIRST_BIT_ZERO_MASK == 0 {
			break
		}
	}
	return result
}

func parseUsableVarintValue(varint uint8, usableBits int) int {
	if usableBits == 8 {
		return int(varint)
	} else {
		return int(varint & LAST_SEVEN_BITS_MASK)
	}
}

func parseUInt8(stream io.Reader) uint8 {
	var result uint8
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatal("Error parsing uint8", err)
	}
	return result
}

func parseUInt16(stream io.Reader) uint16 {
	var result uint16
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatal("Error parsing uint16", err)
	}
	return result
}

func parseUInt32(stream io.Reader) uint32 {
	var result uint32
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatal("Error parsing uint32", err)
	}
	return result
}

func parseString(stream io.Reader, strlen int) string {
	result := make([]byte, strlen)
	if _, err := stream.Read(result); err != nil {
		log.Fatal("Error parsing string", err)
	}
	return string(result)
}
