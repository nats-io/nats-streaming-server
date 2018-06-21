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

package stores

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
)

var (
	benchMsgsCount       = 100000
	benchChannelsCount   = 1000
	benchSubsCount       = 5
	benchInitRecoverMsgs = true
	benchInitRecoverSubs = true
)

func getUnlimitedStore() *StoreLimits {
	limits := testDefaultStoreLimits
	limits.MaxChannels = 0
	limits.MaxSubscriptions = 0
	limits.MaxMsgs = 0
	limits.MaxBytes = 0
	limits.MaxAge = time.Duration(0)
	return &limits
}

func setToUnlimited(b tLogger, s Store) {
	limits := getUnlimitedStore()
	if err := s.SetLimits(limits); err != nil {
		stackFatalf(b, "Error setting limits: %v", err)
	}
}

func BenchmarkStore____StoreMsgs(b *testing.B) {
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			b.StopTimer()

			defer endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")

			// Use message with 128 bytes payload
			msg := make([]byte, 128)
			for i := 0; i < len(msg); i++ {
				msg[i] = byte(rand.Intn(26) + 'A')
			}

			b.StartTimer()
			for i := 0; i < b.N; i++ {
				storeMsg(b, cs, "foo", uint64(i+1), msg)
			}
			cs.Msgs.Flush()
			b.StopTimer()
		})
	}
}

func BenchmarkStore___LookupMsgs(b *testing.B) {
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")

			// Use message with 128 bytes payload
			msg := make([]byte, 128)
			for i := 0; i < len(msg); i++ {
				msg[i] = byte(rand.Intn(26) + 'A')
			}

			for i := 0; i < b.N; i++ {
				storeMsg(b, cs, "foo", uint64(i+1), msg)
			}
			cs.Msgs.Flush()

			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if _, err := cs.Msgs.Lookup(uint64(i + 1)); err != nil {
					b.Fatalf("Error looking message: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkStore___CreateSubs(b *testing.B) {
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			b.StopTimer()

			defer endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")

			sub := &spb.SubState{
				AckInbox: "ackInbox",
				ClientID: "me",
				Inbox:    "inbox",
			}

			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := cs.Subs.CreateSub(sub); err != nil {
					b.Fatalf("Error creating subscription: %v", err)
				}
			}
			cs.Subs.Flush()
			b.StopTimer()
		})
	}
}

func BenchmarkStore___SeqAckSubs(b *testing.B) {
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			b.StopTimer()

			defer endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")

			sub := &spb.SubState{
				AckInbox: "ackInbox",
				ClientID: "me",
				Inbox:    "inbox",
			}
			if err := cs.Subs.CreateSub(sub); err != nil {
				b.Fatalf("Error creating subscription: %v", err)
			}

			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := cs.Subs.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
					b.Fatalf("Error adding seq: %v", err)
				}
			}
			for i := 0; i < b.N; i++ {
				if err := cs.Subs.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
					b.Fatalf("Error adding seq: %v", err)
				}
			}
			cs.Subs.Flush()
			b.StopTimer()

			// Make sure they are all gone (for stores supporting recovery)
			if !st.recoverable {
				return
			}
			// Make sure that they are all gone.
			s.Close()
			s, state := testReOpenStore(b, st, getUnlimitedStore())
			defer s.Close()
			rc := state.Channels["foo"]
			if rc == nil {
				b.Fatal("Channel foo should have been recovered")
			}
			if len(rc.Subscriptions) != 1 {
				b.Fatalf("One sub should have been recovered, got %v", len(rc.Subscriptions))
			}
			rs := rc.Subscriptions[0]
			if len(rs.Pending) != 0 {
				b.Fatalf("Should not be any pending message, got %v", len(rs.Pending))
			}
		})
	}
}

func BenchmarkStore___DeleteSubs(b *testing.B) {
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			b.StopTimer()

			defer endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")

			subs := make([]*spb.SubState, 0, b.N)
			for i := 0; i < b.N; i++ {
				sub := &spb.SubState{
					AckInbox: "ackInbox",
					ClientID: "me",
					Inbox:    "inbox",
				}
				if err := cs.Subs.CreateSub(sub); err != nil {
					b.Fatalf("Error creating subscription: %v", err)
				}
				subs = append(subs, sub)
			}
			cs.Subs.Flush()

			b.StartTimer()
			for _, sub := range subs {
				if err := cs.Subs.DeleteSub(sub.ID); err != nil {
					b.Fatalf("Error creating subscription: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkStore__RecoverMsgs(b *testing.B) {
	msgsCount := benchMsgsCount / benchChannelsCount
	if benchInitRecoverMsgs {
		for _, st := range testStores {
			if !st.recoverable {
				continue
			}
			// Cleanup possibly existing stores
			endTest(b, st)
			var s Store
			if st.name == TypeFile {
				s = createDefaultFileStore(b, FileDescriptorsLimit(100))
			} else {
				s = startTest(b, st)
			}
			defer s.Close()

			setToUnlimited(b, s)

			for i := 0; i < benchChannelsCount; i++ {
				name := fmt.Sprintf("foo_%d", (i + 1))
				cs := storeCreateChannel(b, s, name)
				hw := []byte("Hello World")
				for i := 0; i < msgsCount; i++ {
					storeMsg(b, cs, name, uint64(i+1), hw)
				}
				cs.Msgs.Flush()
			}
			s.Close()
		}
		benchInitRecoverMsgs = false
	}
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			if !st.recoverable {
				b.SkipNow()
			}
			b.StopTimer()

			// Measure recovery
			b.StartTimer()
			s, state := testReOpenStore(b, st, getUnlimitedStore())
			b.StopTimer()
			defer s.Close()

			if len(state.Channels) != benchChannelsCount {
				b.Fatalf("Expected %v channels, got %v", benchChannelsCount, len(state.Channels))
			}
			for rcName, rc := range state.Channels {
				n, _ := msgStoreState(b, rc.Channel.Msgs)
				if n != msgsCount {
					b.Fatalf("Expected %v messages for channel %q, got %v", msgsCount, rcName, n)
				}
			}
		})
	}
}

func BenchmarkStore__RecoverSubs(b *testing.B) {
	if benchInitRecoverSubs {
		for _, st := range testStores {
			if !st.recoverable {
				continue
			}
			// Cleanup possibly existing stores
			endTest(b, st)
			s := startTest(b, st)
			defer s.Close()

			setToUnlimited(b, s)

			cs := storeCreateChannel(b, s, "foo")
			ss := cs.Subs
			subs := []*spb.SubState{}
			for i := 0; i < benchSubsCount; i++ {
				sub := &spb.SubState{
					AckInbox: "ackInbox",
					ClientID: "me",
					Inbox:    "inbox",
				}
				if err := ss.CreateSub(sub); err != nil {
					b.Fatalf("Error creating subscription: %v", err)
				}
				subs = append(subs, sub)
			}

			count := benchMsgsCount / benchSubsCount
			var wg sync.WaitGroup
			wg.Add(benchSubsCount)
			errCh := make(chan error, 1)
			pushError := func(e error) {
				select {
				case errCh <- e:
				default:
				}
			}
			for _, sub := range subs {
				sub := sub
				go func(sub *spb.SubState) {
					defer wg.Done()
					for j := 0; j < count; j++ {
						seqno := uint64(j + 1)
						if err := ss.AddSeqPending(sub.ID, seqno); err != nil {
							pushError(err)
							return
						}
					}
				}(sub)
			}
			wg.Wait()
			select {
			case e := <-errCh:
				b.Fatalf("Error pesisting msgseq or ack: %v", e)
			default:
			}
			s.Close()
		}
		benchInitRecoverSubs = false
	}
	for _, st := range testStores {
		b.Run(st.name, func(b *testing.B) {
			if !st.recoverable {
				b.SkipNow()
			}
			b.StopTimer()

			// Measure recovery
			b.StartTimer()
			s, state := testReOpenStore(b, st, getUnlimitedStore())
			b.StopTimer()
			defer s.Close()

			msgsPerSub := benchMsgsCount / benchSubsCount

			getRecoveredChannel(b, state, "foo")
			recoveredSubs := getRecoveredSubs(b, state, "foo", benchSubsCount)
			for _, sub := range recoveredSubs {
				if len(sub.Pending) != msgsPerSub {
					b.Fatalf("Expected %v messages per sub, got %v", msgsPerSub, len(sub.Pending))
				}
			}
		})
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
