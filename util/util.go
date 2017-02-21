// Copyright 2016-2017 Apcera Inc. All rights reserved.

package util

import (
	"encoding/binary"
	"io"
	"os"
)

// ByteOrder specifies how to convert byte sequences into 16-, 32-, or 64-bit
// unsigned integers.
var ByteOrder binary.ByteOrder

func init() {
	ByteOrder = binary.LittleEndian
}

// EnsureBufBigEnough checks that given buffer is big enough to hold 'needed'
// bytes, otherwise returns a buffer of a size of at least 'needed' bytes.
func EnsureBufBigEnough(buf []byte, needed int) []byte {
	if buf == nil {
		return make([]byte, needed)
	} else if needed > len(buf) {
		return make([]byte, int(float32(needed)*1.1))
	}
	return buf
}

// WriteInt writes an int (4 bytes) to the given writer using ByteOrder.
func WriteInt(w io.Writer, v int) error {
	var b [4]byte

	bs := b[:4]

	ByteOrder.PutUint32(bs, uint32(v))
	_, err := w.Write(bs)
	return err
}

// ReadInt reads an int (4 bytes) from the reader using ByteOrder.
func ReadInt(r io.Reader) (int, error) {
	var b [4]byte

	bs := b[:4]

	_, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	}
	return int(ByteOrder.Uint32(bs)), nil
}

// CloseFile closes the given file and report the possible error only
// if the given error `err` is not already set.
func CloseFile(err error, f *os.File) error {
	if lerr := f.Close(); lerr != nil && err == nil {
		err = lerr
	}
	return err
}
