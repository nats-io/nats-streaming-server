// Copyright 2016-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// ErrUnableToLockNow is used to indicate that a lock cannot be
// immediately acquired.
var ErrUnableToLockNow = errors.New("unable to acquire the lock at the moment")

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

// IsChannelNameValid returns false if any of these conditions for
// the channel name apply:
// - is empty
// - contains the `/` character
// - token separator `.` is first or last
// - there are two consecutives token separators `.`
// if wildcardsAllowed is false:
// - contains wildcards `*` or `>`
// if wildcardsAllowed is true:
// - '*' or '>' are not a token in their own
// - `>` is not the last token
func IsChannelNameValid(channel string, wildcardsAllowed bool) bool {
	if channel == "" || channel[0] == btsep {
		return false
	}
	for i := 0; i < len(channel); i++ {
		c := channel[i]
		if c == '/' {
			return false
		}
		if (c == btsep) && (i == len(channel)-1 || channel[i+1] == btsep) {
			return false
		}
		if !wildcardsAllowed {
			if c == pwc || c == fwc {
				return false
			}
		} else if c == pwc || c == fwc {
			if i > 0 && channel[i-1] != btsep {
				return false
			}
			if c == fwc && i != len(channel)-1 {
				return false
			}
			if i < len(channel)-1 && channel[i+1] != btsep {
				return false
			}
		}
	}
	return true
}

// IsChannelNameLiteral returns true if the channel name is a literal (that is,
// it does not contain any wildcard).
// The channel name is assumed to be valid.
func IsChannelNameLiteral(channel string) bool {
	for i := 0; i < len(channel); i++ {
		if channel[i] == pwc || channel[i] == fwc {
			return false
		}
	}
	return true
}

// FriendlyBytes returns a string with the given bytes int64
// represented as a size, such as 1KB, 10MB, etc...
func FriendlyBytes(bytes int64) string {
	fbytes := float64(bytes)
	base := 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	if fbytes < float64(base) {
		return fmt.Sprintf("%v B", fbytes)
	}
	exp := int(math.Log(fbytes) / math.Log(float64(base)))
	index := exp - 1
	return fmt.Sprintf("%.2f %sB", fbytes/math.Pow(float64(base), float64(exp)), pre[index])
}
