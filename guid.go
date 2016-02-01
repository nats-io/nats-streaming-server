// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	mrand "math/rand"
)

// GUID these need to be very fast to generate and truly unique. We will use 12 bytes of
// crypto generated data, and 10 bytes of sequential data that is started at random.
// Total 22 bytes :)

const (
	digits   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	base     = 36
	preLen   = 12
	maxPre   = int64(4738381338321616896) // 36^12
	seqLen   = 10
	maxSeq   = int64(3656158440062976) // 36^10
	totalLen = preLen + seqLen
)

// Global GUID
var globalGUID *guid

type guid struct {
	mu  sync.Mutex
	pre [preLen]byte
	seq int64
}

// Seed sequential random with math/random and current time and generate crypto prefix.
func init() {
	mrand.Seed(time.Now().UnixNano())
	globalGUID = &guid{seq: mrand.Int63() % maxSeq}
	globalGUID.genNewPre()
}

func newGUID() string {
	// Check first to see if we are at the maximum for the sequential data.
	// Could use atomic, but with 3xatomic (check limit, increment, load pre) doubt its worth it.
	globalGUID.mu.Lock()
	if globalGUID.seq >= maxSeq {
		globalGUID.seq = 0
		globalGUID.genNewPre()
	}
	// copy of prefix under lock.
	var b [totalLen]byte
	for i := 0; i < preLen; i++ {
		b[i] = globalGUID.pre[i]
	}
	// Increment and capture.
	globalGUID.seq++
	seq := globalGUID.seq
	// We can unlock here
	globalGUID.mu.Unlock()

	i := len(b)
	for l := seq; i > preLen; l /= base {
		i -= 1
		b[i] = digits[l%base]
	}
	return string(b[:])
}

// Generate a new prefix from crypto/rand. Assumes lock is held.
// Will panic if it gets an error from rand.Int()
func (g *guid) genNewPre() {
	n, err := rand.Int(rand.Reader, big.NewInt(maxPre))
	if err != nil {
		panic(fmt.Sprintf("Failed generating crypto random number: %v\n", err))
	}
	i := len(g.pre)
	for l := n.Int64(); l > 0; l /= base {
		i -= 1
		g.pre[i] = digits[l%base]
	}
}
