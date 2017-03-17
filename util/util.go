// Copyright 2016-2017 Apcera Inc. All rights reserved.

package util

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrAlreadyLocked is used to indicate that a lock cannot be
// immediately acquired.
var ErrAlreadyLocked = errors.New("unable to acquire the lock")

// LockFile is an interface for lock files utility.
type LockFile interface {
	io.Closer
	IsClosed() bool
}

// ByteOrder specifies how to convert byte sequences into 16-, 32-, or 64-bit
// unsigned integers.
var ByteOrder binary.ByteOrder

func init() {
	ByteOrder = binary.LittleEndian
}

// BackoffTimeCheck allows to execute some code, but not too often.
type BackoffTimeCheck struct {
	nextTime     time.Time
	frequency    time.Duration
	minFrequency time.Duration
	maxFrequency time.Duration
	factor       int
}

// NewBackoffTimeCheck creates an instance of BackoffTimeCheck.
// The `minFrequency` indicates how frequently BackoffTimeCheck.Ok() can return true.
// When Ok() returns true, the allowed frequency is multiplied by `factor`. The
// resulting frequency is capped by `maxFrequency`.
func NewBackoffTimeCheck(minFrequency time.Duration, factor int, maxFrequency time.Duration) (*BackoffTimeCheck, error) {
	if minFrequency <= 0 || factor < 1 || maxFrequency < minFrequency {
		return nil, fmt.Errorf("minFrequency must be positive, factor at least 1 and maxFrequency at least equal to minFrequency, got %v - %v - %v",
			minFrequency, factor, maxFrequency)
	}
	return &BackoffTimeCheck{
		frequency:    minFrequency,
		minFrequency: minFrequency,
		maxFrequency: maxFrequency,
		factor:       factor,
	}, nil
}

// Ok returns true for the first time it is invoked after creation of the object
// or call to Reset(), or after an amount of time (based on the last success
// and the allowed frequency) has elapsed.
// When at the maximum frequency, if this call is made after a delay at least
// equal to 3x the max frequency (or in other words, 2x after what was the target
// for the next print), then the object is auto-reset.
func (bp *BackoffTimeCheck) Ok() bool {
	if bp.nextTime.IsZero() {
		bp.nextTime = time.Now().Add(bp.minFrequency)
		return true
	}
	now := time.Now()
	if now.Before(bp.nextTime) {
		return false
	}
	// If we are already at the max frequency and this call
	// is made after 2x the max frequency, then auto-reset.
	if bp.frequency == bp.maxFrequency &&
		now.Sub(bp.nextTime) >= 2*bp.maxFrequency {
		bp.Reset()
		return true
	}
	if bp.frequency < bp.maxFrequency {
		bp.frequency *= time.Duration(bp.factor)
		if bp.frequency > bp.maxFrequency {
			bp.frequency = bp.maxFrequency
		}
	}
	bp.nextTime = now.Add(bp.frequency)
	return true
}

// Reset the state so that next call to BackoffPrint.Ok() will return true.
func (bp *BackoffTimeCheck) Reset() {
	bp.nextTime = time.Time{}
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
func CloseFile(err error, f io.Closer) error {
	if lerr := f.Close(); lerr != nil && err == nil {
		err = lerr
	}
	return err
}
