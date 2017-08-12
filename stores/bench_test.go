// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"hash/crc32"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
)

func benchCleanupDatastore(b *testing.B) {
	if err := os.RemoveAll(testFSDefaultDatastore); err != nil {
		stackFatalf(b, "Error cleaning up datastore: %v", err)
	}
}

func benchCreateDefaultFileStore(t *testing.B) (*FileStore, *RecoveredState) {
	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &limits)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	state, err := fs.Recover()
	if err != nil {
		fs.Close()
		stackFatalf(t, "Unable to restore the state: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			fs.Close()
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs, state
}

func benchStoreMsg(b *testing.B, ms MsgStore, data []byte) *pb.MsgProto {
	seq, err := ms.Store(data)
	if err != nil {
		stackFatalf(b, "Error storing message: %v", err)
	}
	m := msgStoreLookup(b, ms, seq)
	if err != nil {
		stackFatalf(b, "Error looking up message %v: %v", seq, err)
	}
	return m
}

func BenchmarkRecoverMsgs(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b)
	defer benchCleanupDatastore(b)

	s, _ := benchCreateDefaultFileStore(b)
	defer s.Close()

	cs, err := s.CreateChannel("foo")
	if err != nil {
		b.Fatalf("Error creating channel foo: %v", err)
	}
	ms := cs.Msgs

	hw := []byte("Hello World")
	count := 1000000
	for i := 0; i < count; i++ {
		benchStoreMsg(b, ms, hw)
	}
	s.Close()

	// Measure recovery
	b.N = count
	b.StartTimer()
	s, state := benchCreateDefaultFileStore(b)
	b.StopTimer()

	defer s.Close()
	cs = getRecoveredChannel(b, state, "foo")
	n, _ := msgStoreState(b, cs.Msgs)
	if n != count {
		b.Fatalf("Expected %v messages, got %v", count, n)
	}
}

func BenchmarkRecoverSubs(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b)
	defer benchCleanupDatastore(b)

	s, _ := benchCreateDefaultFileStore(b)
	defer s.Close()

	cs, err := s.CreateChannel("foo")
	if err != nil {
		b.Fatalf("Error creating channel foo: %v", err)
	}
	ss := cs.Subs
	numSubs := 5
	for i := 0; i < numSubs; i++ {
		sub := &spb.SubState{
			AckInbox: "ackInbox",
			ClientID: "me",
			ID:       uint64(i + 1),
			Inbox:    "inbox",
		}
		ss.CreateSub(sub)
	}

	count := 200000
	var wg sync.WaitGroup
	wg.Add(numSubs)
	for i := 0; i < numSubs; i++ {
		go func(subID uint64) {
			defer wg.Done()
			for j := 0; j < count; j++ {
				seqno := uint64(j + 1)
				ss.AddSeqPending(subID, seqno)
				ss.AckSeqPending(subID, seqno)
			}
		}(uint64(i + 1))
	}
	wg.Wait()
	s.Close()

	// Measure recovery
	b.N = count * numSubs
	limits := testDefaultStoreLimits
	b.StartTimer()
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, &limits)
	b.StopTimer()
	if err != nil {
		b.Fatalf("Unable to create a FileStore instance: %v", err)
	}
	defer s.Close()
	state, err := s.Recover()
	if err != nil {
		b.Fatalf("Unable to restore the state: %v", err)
	}
	getRecoveredChannel(b, state, "foo")
	recoveredSubs := getRecoveredSubs(b, state, "foo", numSubs)
	for _, sub := range recoveredSubs {
		if len(sub.Pending) != 0 {
			b.Fatalf("Non pending message should have been recovered, got %v", len(sub.Pending))
		}
	}
}

func benchCRCWithPoly(b *testing.B, arraySize int, poly uint32) {
	b.StopTimer()
	array := make([]byte, arraySize)
	for i := 0; i < arraySize; i++ {
		array[i] = byte(rand.Intn(255))
	}
	table := crc32.MakeTable(poly)
	crc := crc32.Checksum(array, table)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if c := crc32.Checksum(array, table); c != crc {
			stackFatalf(b, "Expected checksum %v, got %v", crc, c)
		}
	}
	b.StopTimer()
}

func BenchmarkCRCIEEE_64(b *testing.B) {
	benchCRCWithPoly(b, 64, crc32.IEEE)
}

func BenchmarkCRCIEEE_512(b *testing.B) {
	benchCRCWithPoly(b, 512, crc32.IEEE)
}

func BenchmarkCRCIEEE_4096(b *testing.B) {
	benchCRCWithPoly(b, 4096, crc32.IEEE)
}

func BenchmarkCRCIEEE_1M(b *testing.B) {
	benchCRCWithPoly(b, 1024*1024, crc32.IEEE)
}

func BenchmarkCRCCastagnoli_64(b *testing.B) {
	benchCRCWithPoly(b, 64, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_512(b *testing.B) {
	benchCRCWithPoly(b, 512, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_4096(b *testing.B) {
	benchCRCWithPoly(b, 4096, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_1M(b *testing.B) {
	benchCRCWithPoly(b, 1024*1024, crc32.Castagnoli)
}
