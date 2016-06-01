// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"github.com/nats-io/go-stan/pb"
	"github.com/nats-io/stan-server/spb"
	"os"
	"sync"
	"testing"
)

func benchCleanupDatastore(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		stackFatalf(b, "Error cleaning up datastore: %v", err)
	}
}

func benchCreateDefaultFileStore(t *testing.B) *FileStore {
	fs, state, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs
}

func benchStoreMsg(b *testing.B, ms MsgStore, data []byte) *pb.MsgProto {
	m, err := ms.Store("", data)
	if err != nil {
		stackFatalf(b, "Error storing message: %v", err)
	}
	return m
}

func BenchmarkRecoverMsgs(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchCreateDefaultFileStore(b)
	defer s.Close()

	cs, _, err := s.CreateChannel("foo", nil)
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
	s = benchCreateDefaultFileStore(b)
	b.StopTimer()

	defer s.Close()
	cs = s.LookupChannel("foo")
	if cs == nil {
		b.Fatal("Channel foo should exist")
	}
	n, _, err := cs.Msgs.State()
	if err != nil {
		b.Fatalf("Unexpected error getting state: %v", err)
	}
	if n != count {
		b.Fatalf("Expected %v messages, got %v", count, n)
	}
}

func BenchmarkRecoverSubs(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchCreateDefaultFileStore(b)
	defer s.Close()

	cs, _, err := s.CreateChannel("foo", nil)
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
	b.StartTimer()
	s, state, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	b.StopTimer()
	if err != nil {
		b.Fatalf("Unable to create a FileStore instance: %v", err)
	}
	defer s.Close()
	if state == nil {
		b.Fatal("State should have been recovered")
	}
	recoveredSubs := state.Subs["foo"]
	if len(recoveredSubs) != numSubs {
		b.Fatalf("Should have recovered %v subs, got %v", numSubs, len(recoveredSubs))
	}
	for _, sub := range recoveredSubs {
		if len(sub.Pending) != 0 {
			b.Fatalf("Non pending message should have been recovered, got %v", len(sub.Pending))
		}
	}
}
