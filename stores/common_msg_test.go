// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
)

func TestCSBasicMsgStore(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			ms := cs.Msgs

			// No message is stored, verify expected values.
			if m := msgStoreFirstMsg(t, ms); m != nil {
				t.Fatalf("Unexpected first message: %v vs %v", m, nil)
			}

			if m := msgStoreLastMsg(t, ms); m != nil {
				t.Fatalf("Unexpected first message: %v vs %v", m, nil)
			}

			if seq := msgStoreFirstSequence(t, ms); seq != 0 {
				t.Fatalf("Unexpected first sequence: %v vs %v", seq, 0)
			}

			if seq := msgStoreLastSequence(t, ms); seq != 0 {
				t.Fatalf("Unexpected first sequence: %v vs %v", seq, 0)
			}

			if s1, s2 := msgStoreFirstAndLastSequence(t, ms); s1 != 0 || s2 != 0 {
				t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
			}

			payload1 := []byte("m1")
			m1 := storeMsg(t, cs, "foo", payload1)

			payload2 := []byte("m2")
			m2 := storeMsg(t, cs, "foo", payload2)

			if string(payload1) != string(m1.Data) {
				t.Fatalf("Unexpected payload: %v", string(m1.Data))
			}
			if string(payload2) != string(m2.Data) {
				t.Fatalf("Unexpected payload: %v", string(m1.Data))
			}

			firstMsg := msgStoreFirstMsg(t, ms)
			if !reflect.DeepEqual(firstMsg, m1) {
				t.Fatalf("Unexpected first message: %v vs %v", firstMsg, m1)
			}

			lastMsg := msgStoreLastMsg(t, ms)
			if !reflect.DeepEqual(lastMsg, m2) {
				t.Fatalf("Unexpected last message: %v vs %v", lastMsg, m2)
			}

			if seq := msgStoreFirstSequence(t, ms); seq != m1.Sequence {
				t.Fatalf("Unexpected first sequence: %v vs %v", seq, m1.Sequence)
			}

			if seq := msgStoreLastSequence(t, ms); seq != m2.Sequence {
				t.Fatalf("Unexpected first sequence: %v vs %v", seq, m2.Sequence)
			}

			if s1, s2 := msgStoreFirstAndLastSequence(t, ms); s1 != m1.Sequence || s2 != m2.Sequence {
				t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
			}

			lm1 := msgStoreLookup(t, ms, m1.Sequence)
			if !reflect.DeepEqual(lm1, m1) {
				t.Fatalf("Unexpected lookup result: %v instead of %v", lm1, m1)
			}

			lm2 := msgStoreLookup(t, ms, m2.Sequence)
			if !reflect.DeepEqual(lm2, m2) {
				t.Fatalf("Unexpected lookup result: %v instead of %v", lm2, m2)
			}

			count, bytes, err := ms.State()
			if err != nil {
				t.Fatalf("Unexpected error getting state: %v", err)
			}
			expectedBytes := uint64(m1.Size() + m2.Size())
			if _, ok := s.(*FileStore); ok {
				// FileStore counts more toward the number of bytes
				expectedBytes += 2 * (msgRecordOverhead)
			}
			if count != 2 || bytes != expectedBytes {
				t.Fatalf("Unexpected counts: %v, %v vs %v, %v", count, bytes, 2, expectedBytes)
			}

			// Store one more mesasge to check that LastMsg is correctly updated
			m3 := storeMsg(t, cs, "foo", []byte("last"))
			lastMsg = msgStoreLastMsg(t, ms)
			if !reflect.DeepEqual(lastMsg, m3) {
				t.Fatalf("Expected last message to be %v, got %v", m3, lastMsg)
			}
		})
	}
}

func TestCSMsgsState(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			payload := []byte("hello")

			cs1 := storeCreateChannel(t, s, "foo")
			cs2 := storeCreateChannel(t, s, "bar")

			m1 := storeMsg(t, cs1, "foo", payload)
			m2 := storeMsg(t, cs2, "bar", payload)

			_, isFileStore := s.(*FileStore)

			count, bytes := msgStoreState(t, cs1.Msgs)
			expectedBytes := uint64(m1.Size())
			if isFileStore {
				expectedBytes += msgRecordOverhead
			}
			if count != 1 || bytes != expectedBytes {
				t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
			}

			count, bytes = msgStoreState(t, cs2.Msgs)
			expectedBytes = uint64(m2.Size())
			if isFileStore {
				expectedBytes += msgRecordOverhead
			}
			if count != 1 || bytes != expectedBytes {
				t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
			}
		})
	}
}

func TestCSMaxMsgs(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			payload := []byte("hello")

			_, isFileStore := s.(*FileStore)

			limitCount := 0
			stopBytes := uint64(500)
			expectedBytes := uint64(0)
			for i := 0; ; i++ {
				seq := uint64(i + 1)
				m := pb.MsgProto{Data: payload, Subject: "foo", Sequence: seq, Timestamp: time.Now().UnixNano()}
				expectedBytes += uint64(m.Size())
				if isFileStore {
					expectedBytes += msgRecordOverhead
				}
				limitCount++
				if expectedBytes >= stopBytes {
					break
				}
			}

			limits := testDefaultStoreLimits
			limits.MaxMsgs = limitCount
			limits.MaxBytes = int64(expectedBytes)
			if err := s.SetLimits(&limits); err != nil {
				t.Fatalf("Unexpected error setting limits: %v", err)
			}

			totalSent := limitCount + 60
			firstSeqAfterLimitReached := uint64(totalSent - limitCount + 1)

			cs := storeCreateChannel(t, s, "foo")

			for i := 0; i < totalSent; i++ {
				storeMsg(t, cs, "foo", payload)
			}

			count, bytes := msgStoreState(t, cs.Msgs)
			if count != limitCount || bytes != expectedBytes {
				t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, limitCount, bytes, expectedBytes)
			}

			// Check that older messages are no longer avail.
			if msgStoreLookup(t, cs.Msgs, 1) != nil ||
				msgStoreLookup(t, cs.Msgs, uint64(firstSeqAfterLimitReached-1)) != nil {
				t.Fatal("Older messages still available")
			}

			firstMsg := msgStoreFirstMsg(t, cs.Msgs)
			firstSeq := msgStoreFirstSequence(t, cs.Msgs)
			lastMsg := msgStoreLastMsg(t, cs.Msgs)
			lastSeq := msgStoreLastSequence(t, cs.Msgs)

			if firstMsg == nil || firstMsg.Sequence != firstSeq || firstSeq != firstSeqAfterLimitReached {
				t.Fatalf("Incorrect first message: msg=%v seq=%v", firstMsg, firstSeq)
			}
			if lastMsg == nil || lastMsg.Sequence != lastSeq || lastSeq != uint64(totalSent) {
				t.Fatalf("Incorrect last message: msg=%v seq=%v", firstMsg, firstSeq)
			}

			// Store a message with a payload larger than the limit.
			// Make sure that the message is stored, but all others should
			// be removed.
			bigMsg := make([]byte, limits.MaxBytes+100)
			m := storeMsg(t, cs, "foo", bigMsg)
			expectedBytes = uint64(m.Size())
			if isFileStore {
				expectedBytes += msgRecordOverhead
			}

			count, bytes = msgStoreState(t, cs.Msgs)
			if count != 1 || bytes != expectedBytes {
				t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
			}

			// Test that we check only on non-zero limits
			expectedCount := 10
			expectedBytes = uint64(0)
			channelName := "maxcount"
			for i := 0; i < expectedCount; i++ {
				seq := uint64(i + 1)
				m := pb.MsgProto{Data: payload, Subject: channelName, Sequence: seq, Timestamp: time.Now().UnixNano()}
				expectedBytes += uint64(m.Size())
				if isFileStore {
					expectedBytes += msgRecordOverhead
				}
			}
			limits.MaxMsgs = expectedCount
			limits.MaxBytes = 0
			if err := s.SetLimits(&limits); err != nil {
				t.Fatalf("Unexpected error setting limits: %v", err)
			}
			cs = storeCreateChannel(t, s, channelName)
			for i := 0; i < expectedCount+10; i++ {
				storeMsg(t, cs, channelName, payload)
			}
			n, b := msgStoreState(t, cs.Msgs)
			if n != expectedCount {
				t.Fatalf("Expected %v messages, got %v", expectedCount, n)
			}
			if b != expectedBytes {
				t.Fatalf("Expected %v bytes, got %v", expectedBytes, b)
			}

			expectedCount = 0
			expectedBytes = uint64(0)
			channelName = "maxbytes"
			for i := 0; ; i++ {
				seq := uint64(i + 1)
				m := pb.MsgProto{Data: payload, Subject: channelName, Sequence: seq, Timestamp: time.Now().UnixNano()}
				expectedBytes += uint64(m.Size())
				if isFileStore {
					expectedBytes += msgRecordOverhead
				}
				expectedCount++
				if expectedBytes >= 1000 {
					break
				}
			}
			limits.MaxMsgs = 0
			limits.MaxBytes = int64(expectedBytes)
			if err := s.SetLimits(&limits); err != nil {
				t.Fatalf("Unexpected error setting limits: %v", err)
			}
			cs = storeCreateChannel(t, s, channelName)
			for i := 0; i < expectedCount+10; i++ {
				storeMsg(t, cs, channelName, payload)
			}
			n, b = msgStoreState(t, cs.Msgs)
			if n != expectedCount {
				t.Fatalf("Expected %d messages, got %v", expectedCount, n)
			}
			if b != expectedBytes {
				t.Fatalf("Expected %v bytes, got %v", expectedBytes, b)
			}
		})
	}
}

func TestCSMaxAge(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			sl := testDefaultStoreLimits
			sl.MaxAge = 100 * time.Millisecond
			s.SetLimits(&sl)

			cs := storeCreateChannel(t, s, "foo")
			msg := []byte("hello")
			for i := 0; i < 10; i++ {
				storeMsg(t, cs, "foo", msg)
			}
			// Wait a bit
			time.Sleep(60 * time.Millisecond)
			// Send more
			for i := 0; i < 5; i++ {
				storeMsg(t, cs, "foo", msg)
			}
			// Wait a bit
			time.Sleep(60 * time.Millisecond)
			// We should have the first 10 expired and 5 left.
			expectedFirst := uint64(11)
			expectedLast := uint64(15)
			first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
			if first != expectedFirst || last != expectedLast {
				t.Fatalf("Expected first/last to be %v/%v, got %v/%v",
					expectedFirst, expectedLast, first, last)
			}
			// Wait more and all should be gone.
			time.Sleep(100 * time.Millisecond)
			if n, _ := msgStoreState(t, cs.Msgs); n != 0 {
				t.Fatalf("All messages should have expired, got %v", n)
			}

			// We are going to set a limit of MaxMsgs to 1 on top
			// of the expiration and make sure that expiration works
			// ok if first message that was supposed to expire is
			// gone by the time it should have expired.
			sl.MaxMsgs = 1
			s.SetLimits(&sl)

			cs = storeCreateChannel(t, s, "bar")
			storeMsg(t, cs, "bar", msg)
			// Wait a bit
			time.Sleep(60 * time.Millisecond)
			// Send another message that should replace the first one
			m2 := storeMsg(t, cs, "bar", msg)
			// Wait more so that max age of initial message is passed
			time.Sleep(60 * time.Millisecond)
			// Ensure there is still 1 message...
			if n, _ := msgStoreState(t, cs.Msgs); n != 1 {
				t.Fatalf("There should be 1 message, got %v", n)
			}
			// ...which should be m2: this should not fail
			msgStoreLookup(t, cs.Msgs, m2.Sequence)
			// Again, wait more and second message should not be gone
			time.Sleep(100 * time.Millisecond)
			if n, _ := msgStoreState(t, cs.Msgs); n != 0 {
				t.Fatalf("All messages should have expired, got %v", n)
			}

			if st.name == TypeMemory {
				// Verify timer is set
				isSet := func() bool {
					var timerSet bool
					if st.name == TypeMemory {
						ms := cs.Msgs.(*MemoryMsgStore)
						ms.RLock()
						timerSet = ms.ageTimer != nil
						ms.RUnlock()
					}
					return timerSet
				}
				if isSet() {
					t.Fatal("Timer should not be set")
				}
				// Store a message
				storeMsg(t, cs, "bar", []byte("msg"))
				// Now timer should have been set again
				if !isSet() {
					t.Fatal("Timer should have been set")
				}
			}
		})
	}
}

func TestCSGetSeqFromStartTime(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			limits := testDefaultStoreLimits
			// On windows, the 1ms between each send may actually be more
			// so we need a bigger expiration value.
			if runtime.GOOS == "windows" {
				limits.MaxAge = 1500 * time.Millisecond
			} else {
				limits.MaxAge = 500 * time.Millisecond
			}
			s.SetLimits(&limits)
			// Force creation of channel without storing anything yet
			cs := storeCreateChannel(t, s, "foo")
			// Check before storing anything
			seq := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano())
			if seq != 0 {
				t.Fatalf("Invalid start sequence. Expected %v got %v", 0, seq)
			}

			count := 100
			msgs := make([]*pb.MsgProto, 0, count)
			payload := []byte("hello")
			for i := 0; i < count; i++ {
				m := storeMsg(t, cs, "foo", payload)
				msgs = append(msgs, m)
				time.Sleep(1 * time.Millisecond)
			}

			startMsg := msgs[count/2]
			seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, startMsg.Timestamp)
			if seq != startMsg.Sequence {
				t.Fatalf("Invalid start sequence. Expected %v got %v", startMsg.Sequence, seq)
			}
			seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, msgs[0].Timestamp-int64(time.Second))
			if seq != msgs[0].Sequence {
				t.Fatalf("Expected seq to be %v, got %v", msgs[0].Sequence, seq)
			}
			seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, msgs[count-1].Timestamp+int64(time.Second))
			if seq != msgs[count-1].Sequence+1 {
				t.Fatalf("Expected seq to be %v, got %v", msgs[count-1].Sequence+1, seq)
			}
			// Wait for all messages to expire
			deadline := time.Now().Add(2 * time.Second)
			var n int
			for time.Now().Before(deadline) {
				n, _ = msgStoreState(t, cs.Msgs)
				if n == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if n > 0 {
				stackFatalf(t, "Messages should have all expired by now")
			}
			// Now these calls should all return the lastSeq + 1
			seq1 := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano()-int64(time.Hour))
			seq2 := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano()+int64(time.Hour))
			if seq1 != seq2 || seq1 != msgs[count-1].Sequence+1 {
				t.Fatalf("After expiration, returned sequence should be: %v, got %v %v", msgs[count-1].Sequence+1, seq1, seq2)
			}

			if st.recoverable {
				// Restart the store, make sure we can get the expected sequence
				times := []int64{
					time.Now().UnixNano() - int64(time.Hour),
					time.Now().UnixNano() + int64(time.Hour),
				}
				expectedSeqs := []uint64{101, 101}

				for i := 0; i < len(times); i++ {
					s.Close()
					s, state := testReOpenStore(t, st, &limits)
					defer s.Close()

					cs := getRecoveredChannel(t, state, "foo")
					seq := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, times[i])
					if seq != expectedSeqs[i] {
						t.Fatalf("Expected seq to be %v, got %v", expectedSeqs[i], seq)
					}
				}
			}
		})
	}
}

func TestCSFirstAndLastMsg(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			limit := testDefaultStoreLimits
			limit.MaxAge = 100 * time.Millisecond
			if err := s.SetLimits(&limit); err != nil {
				t.Fatalf("Error setting limits: %v", err)
			}

			msg := []byte("msg")
			cs := storeCreateChannel(t, s, "foo")
			storeMsg(t, cs, "foo", msg)
			storeMsg(t, cs, "foo", msg)

			if m := msgStoreFirstMsg(t, cs.Msgs); m.Sequence != 1 {
				t.Fatalf("Unexpected first message: %v", m)
			}
			if m := msgStoreLastMsg(t, cs.Msgs); m.Sequence != 2 {
				t.Fatalf("Unexpected last message: %v", m)
			}
			// Wait for all messages to expire
			timeout := time.Now().Add(3 * time.Second)
			ok := false
			for time.Now().Before(timeout) {
				if n, _ := msgStoreState(t, cs.Msgs); n == 0 {
					ok = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !ok {
				t.Fatal("Timed-out waiting for messages to expire")
			}

			var firstMsg, lastMsg *pb.MsgProto

			// By-pass the FirstMsg() and LastMsg() API to make sure that
			// we don't update based on lookup
			getInternalFirstAndLastMsg := func() {
				switch st.name {
				case TypeMemory:
					ms := cs.Msgs.(*MemoryMsgStore)
					ms.RLock()
					firstMsg = ms.msgs[ms.first]
					lastMsg = ms.msgs[ms.last]
					ms.RUnlock()
				case TypeFile:
					ms := cs.Msgs.(*FileMsgStore)
					ms.RLock()
					firstMsg = ms.firstMsg
					lastMsg = ms.lastMsg
					ms.RUnlock()
				case TypeSQL:
					// Not applicable since this store does not store
					// the first and last message.
					firstMsg = msgStoreFirstMsg(t, cs.Msgs)
					lastMsg = msgStoreLastMsg(t, cs.Msgs)
				default:
					stackFatalf(t, "Fix test for store type: %v", st.name)
				}
			}
			getInternalFirstAndLastMsg()

			if firstMsg != nil {
				t.Fatalf("Unexpected first message: %v", firstMsg)
			}
			if lastMsg != nil {
				t.Fatalf("Unexpected last message: %v", lastMsg)
			}
			// Store two new messages and check first/last updated correctly
			storeMsg(t, cs, "foo", msg)
			storeMsg(t, cs, "foo", msg)

			getInternalFirstAndLastMsg()
			if firstMsg == nil || firstMsg.Sequence != 3 {
				t.Fatalf("Unexpected first message: %v", firstMsg)
			}
			if lastMsg == nil || lastMsg.Sequence != 4 {
				t.Fatalf("Unexpected last message: %v", lastMsg)
			}

		})
	}
}

func TestCSLimitsOnRecovery(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			if !st.recoverable {
				return
			}
			t.Parallel()
			defer endTest(t, st)

			storeMsgs := func(cs *Channel, count, size int) {
				msg := make([]byte, size)
				for i := 0; i < count; i++ {
					storeMsg(t, cs, "foo", msg)
				}
			}

			// First run store with no limit.
			s := startTest(t, st)
			defer s.Close()
			cs := storeCreateChannel(t, s, "foo")
			storeMsgs(cs, 1, 10)
			s.Close()

			// Add limit of MaxAge
			limits := testDefaultStoreLimits
			limits.MaxAge = 15 * time.Millisecond
			// Wait more than max age
			time.Sleep(30 * time.Millisecond)
			// Reopen store
			s, state := testReOpenStore(t, st, &limits)
			cs = state.Channels["foo"].Channel
			// Message should be gone.
			if n, _ := msgStoreState(t, cs.Msgs); n != 0 {
				t.Fatalf("Expected no message, got %v", n)
			}
			s.Close()

			// Remove MaxAge
			limits.MaxAge = time.Duration(0)
			s, state = testReOpenStore(t, st, &limits)
			cs = state.Channels["foo"].Channel
			// Store 3 messages. We will set the limit to 2 on restart.
			storeMsgs(cs, 3, 10)
			s.Close()
			limits.MaxMsgs = 2
			s, state = testReOpenStore(t, st, &limits)
			cs = state.Channels["foo"].Channel
			if n, _ := msgStoreState(t, cs.Msgs); n != limits.MaxMsgs {
				t.Fatalf("Expected %d messages, got %v", limits.MaxMsgs, n)
			}
			s.Close()

			// Remove MaxMsgs
			limits.MaxMsgs = 0
			s, state = testReOpenStore(t, st, &limits)
			cs = state.Channels["foo"].Channel
			// Send more about 1000 bytes, we will set the limit to 500
			storeMsgs(cs, 10, 100)
			s.Close()
			limits.MaxBytes = 500
			s, state = testReOpenStore(t, st, &limits)
			cs = state.Channels["foo"].Channel
			if _, n := msgStoreState(t, cs.Msgs); n > uint64(limits.MaxBytes) {
				t.Fatalf("Expected bytes less than %v, got %v", limits.MaxBytes, n)
			}
			s.Close()
		})
	}
}
