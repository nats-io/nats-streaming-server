// Copyright 2016 Apcera Inc. All rights reserved.

package util

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// CrcSize is the size in bytes of the CRC value of a record
const CrcSize = crc32.Size

// crcTable is used to compute the CRC32 of various records.
// Although IEEE performs slower than Castagnoli for small records,
// it still performs at speed far greater than what the store can
// do, so not a bottleneck. For bigger records (up to 1MB), IEEE
// performs better than Castagnoli (14,266 crc/sec vs 9,700), so
// this is the one we are using. That being said, we will use the
// API that uses a table so we can easily switch to a different
// polynomial.
var crcTable = crc32.IEEETable

// ByteOrder specifies how to convert byte sequences into 16-, 32-, or 64-bit
// unsigned integers.
var ByteOrder binary.ByteOrder

func init() {
	ByteOrder = binary.LittleEndian
}

// EnsureBufBigEnough checks that given buffer is big enough to hold 'needed'
// bytes, otherwise returns a buffer of a size of at least 'needed' bytes.
func EnsureBufBigEnough(buf []byte, needed int) []byte {
	if buf == nil || needed > len(buf) {
		return make([]byte, int(float32(needed)*1.1))
	}
	return buf
}

// WriteInt writes an int (4 bytes) to the given writer using ByteOrder.
func WriteInt(w io.Writer, v int) error {
	var b [4]byte
	var bs []byte

	bs = b[:4]

	ByteOrder.PutUint32(bs, uint32(v))
	_, err := w.Write(bs)
	return err
}

// ReadInt reads an int (4 bytes) from the reader using ByteOrder.
func ReadInt(r io.Reader) (int, error) {
	var b [4]byte
	var bs []byte

	bs = b[:4]

	_, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	}
	return int(ByteOrder.Uint32(bs)), nil
}

// WriteRecord writes a record to the given io.Writer.
// The layout is as this:
// 4       bytes: number of bytes that follow
// crcSize bytes: CRC of the following bytes
// payload bytes: payload. The size is the value stored in the first 4 bytes
//                minus the crcSize bytes for the CRC.
func WriteRecord(w io.Writer, buf []byte) error {
	// Write the size of the buffer + CrcSize for the CRC
	if err := WriteInt(w, len(buf)+CrcSize); err != nil {
		return err
	}
	// Write the CRC value
	if err := WriteInt(w, int(crc32.Checksum(buf, crcTable))); err != nil {
		return err
	}
	// Write the content itself
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

// GetRecord returns the record part of the buffer, stripping out the CRC value.
// The given buffer contains the CRC value of the content in the first `crcSize`
// bytes. If the boolean `check` is true, the CRC is computed and checked against
// the value at the beginning of the buffer. If they differ, and error is returned.
// NOTE: This call is not symetric with WriteRecord since GetRecord is called after
// having read the size of the record (so the buffer does not contain the size).
func GetRecord(buf []byte, checkCRC bool) ([]byte, error) {
	crc := uint32(0)
	if checkCRC {
		// CRC is first 4 bytes
		crc = ByteOrder.Uint32(buf[0:CrcSize])
	}
	// body is the rest...
	body := buf[CrcSize:len(buf)]
	if checkCRC {
		// check CRC against what was stored
		if err := VerifyCRC(body, crcTable, crc); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// VerifyCRC computes the CRC32 of the given buffer and compares it against
// the expected crc value. If it differs, an error is returned.
func VerifyCRC(buf []byte, table *crc32.Table, crc uint32) error {
	if c := crc32.Checksum(buf, table); c != crc {
		return fmt.Errorf("corrupted data, expected crc to be 0x%08x, got 0x%08x", crc, c)
	}
	return nil
}
