// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/util"
)

func createDefaultMemStore(t *testing.T) *MemoryStore {
	ms, err := NewMemoryStore(testLogger, &testDefaultStoreLimits)
	if err != nil {
		stackFatalf(t, "Unexpected error: %v", err)
	}
	return ms
}

func TestMSUseDefaultLimits(t *testing.T) {
	ms, err := NewMemoryStore(testLogger, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()
	if !reflect.DeepEqual(*ms.limits, DefaultStoreLimits) {
		t.Fatalf("Default limits are not used: %v\n", *ms.limits)
	}
}

func TestMSMaxAge(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	cs := testMaxAge(t, ms)

	// Store a message
	storeMsg(t, cs, "foo", []byte("msg"))
	// Verify timer is set
	ms.RLock()
	timerSet := cs.Msgs.(*MemoryMsgStore).ageTimer != nil
	ms.RUnlock()
	if !timerSet {
		t.Fatal("Timer should have been set")
	}
}

func TestMSGetSeqFromTimestamp(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testGetSeqFromStartTime(t, ms)
}

func TestMSClientAPIs(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testClientAPIs(t, ms)
}

func TestMSFlush(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testFlush(t, ms)
}

func TestMSIncrementalTimestamp(t *testing.T) {
	// This test need to run without race and may take some time, so
	// excluding from Travis. Check presence of a known TRAVIS env
	// variable to detect that we run on Travis so we can skip this
	// test.
	if util.RaceEnabled || os.Getenv("TRAVIS_GO_VERSION") != "" {
		t.SkipNow()
	}
	s := createDefaultMemStore(t)
	defer s.Close()

	limits := DefaultStoreLimits
	limits.MaxMsgs = 2
	s.SetLimits(&limits)

	cs := storeCreateChannel(t, s, "foo")
	ms := cs.Msgs

	msg := []byte("msg")

	total := 8000000
	for i := 0; i < total; i++ {
		seq1, err1 := ms.Store(msg)
		seq2, err2 := ms.Store(msg)
		if err1 != nil || err2 != nil {
			t.Fatalf("Unexpected error on store: %v %v", err1, err2)
		}
		m1 := msgStoreLookup(t, ms, seq1)
		m2 := msgStoreLookup(t, ms, seq2)
		if m2.Timestamp < m1.Timestamp {
			t.Fatalf("Timestamp of msg %v is smaller than previous one. Diff is %vms",
				m2.Sequence, m1.Timestamp-m2.Timestamp)
		}
	}
}

func TestMSFirstAndLastMsg(t *testing.T) {
	limit := testDefaultStoreLimits
	limit.MaxAge = 100 * time.Millisecond
	s, err := NewMemoryStore(testLogger, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer s.Close()

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
	ms := cs.Msgs.(*MemoryMsgStore)
	// By-pass the FirstMsg() and LastMsg() API to make sure that
	// we don't update based on lookup
	ms.RLock()
	firstMsg := ms.msgs[ms.first]
	lastMsg := ms.msgs[ms.last]
	ms.RUnlock()
	if firstMsg != nil {
		t.Fatalf("Unexpected first message: %v", firstMsg)
	}
	if lastMsg != nil {
		t.Fatalf("Unexpected last message: %v", lastMsg)
	}
	// Store two new messages and check first/last updated correctly
	storeMsg(t, cs, "foo", msg)
	storeMsg(t, cs, "foo", msg)
	ms.RLock()
	firstMsg = ms.msgs[ms.first]
	lastMsg = ms.msgs[ms.last]
	ms.RUnlock()
	if firstMsg == nil || firstMsg.Sequence != 3 {
		t.Fatalf("Unexpected first message: %v", firstMsg)
	}
	if lastMsg == nil || lastMsg.Sequence != 4 {
		t.Fatalf("Unexpected last message: %v", lastMsg)
	}
}

func TestMSGetExclusiveLock(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()
	// GetExclusiveLock is not supported
	locked, err := ms.GetExclusiveLock()
	if err == nil {
		t.Fatal("Should have failed, it did not")
	}
	if locked {
		t.Fatal("Should not be locked")
	}
}

func TestMSRecover(t *testing.T) {
	ms, err := NewMemoryStore(testLogger, nil)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer ms.Close()
	state, err := ms.Recover()
	if err != nil {
		t.Fatalf("Recover should not return an error, got %v", err)
	}
	if state != nil {
		t.Fatalf("State should be nil, got %v", state)
	}
}

func TestMSNegativeLimits(t *testing.T) {
	limits := DefaultStoreLimits
	limits.MaxMsgs = -1000
	if ms, err := NewMemoryStore(testLogger, &limits); ms != nil || err == nil {
		if ms != nil {
			ms.Close()
		}
		t.Fatal("Should have failed to create store with a negative limit")
	}
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testNegativeLimit(t, ms)
}
