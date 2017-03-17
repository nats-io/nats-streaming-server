// Copyright 2016-2017 Apcera Inc. All rights reserved.

package util

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

// ByteOrder specifies how to convert byte sequences into 16-, 32-, or 64-bit
// unsigned integers.
var ByteOrder binary.ByteOrder

func init() {
	ByteOrder = binary.LittleEndian
}

// BackoffPrint allows to print a statement but not too often.
type BackoffPrint struct {
	nextPrint    time.Time
	frequency    time.Duration
	minFrequency time.Duration
	maxFrequency time.Duration
	factor       int
}

// NewBackoffPrint creates an instance of BackoffPrint.
// The `minFrequency` indicates how frequently BackoffPrint.Ok() can return true.
// When Ok() returns true, the allowed frequency is multiplied by `factor`. The
// resulting frequency is capped by `maxFrequency`.
func NewBackoffPrint(minFrequency time.Duration, factor int, maxFrequency time.Duration) *BackoffPrint {
	return &BackoffPrint{
		frequency:    minFrequency,
		minFrequency: minFrequency,
		maxFrequency: maxFrequency,
		factor:       factor,
	}
}

// Ok returns true for the first time it is invoked after creation of the object
// or call to Reset(), or after an amount of time (based on the last success
// and the allowed frequency) has elapsed.
// When at the maximum frequency, if this call is made after a delay at least
// equal to 3x the max frequency (or in other words, 2x after what was the target
// for the next print), then the object is auto-reset.
func (bp *BackoffPrint) Ok() bool {
	if bp.nextPrint.IsZero() {
		bp.nextPrint = time.Now().Add(bp.minFrequency)
		return true
	}
	if time.Now().Before(bp.nextPrint) {
		return false
	}
	// If we are already at the max frequency and this call
	// is made after 2x the max frequency, then auto-reset.
	if bp.frequency == bp.maxFrequency &&
		time.Since(bp.nextPrint) >= 2*bp.maxFrequency {
		bp.Reset()
		return true
	}
	if bp.frequency < bp.maxFrequency {
		bp.frequency *= time.Duration(bp.factor)
		if bp.frequency > bp.maxFrequency {
			bp.frequency = bp.maxFrequency
		}
	}
	bp.nextPrint = time.Now().Add(bp.frequency)
	return true
}

// Reset the state so that next call to BackoffPrint.Ok() will return true.
func (bp *BackoffPrint) Reset() {
	bp.nextPrint = time.Time{}
	bp.frequency = bp.minFrequency
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
