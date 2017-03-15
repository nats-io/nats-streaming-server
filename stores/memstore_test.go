// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"os"
	"reflect"
	"testing"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

func createDefaultMemStore(t *testing.T) *MemoryStore {
	ms, err := NewMemoryStore(&testDefaultStoreLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return ms
}

func TestMSBasicCreate(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testBasicCreate(t, ms, TypeMemory)
}

func TestMSInit(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	info := spb.ServerInfo{
		ClusterID:   "id",
		Discovery:   "discovery",
		Publish:     "publish",
		Subscribe:   "subscribe",
		Unsubscribe: "unsubscribe",
		Close:       "close",
	}
	// Should not fail
	if err := ms.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
	info.ClusterID = "newId"
	// Should not fail
	if err := ms.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
}

func TestMSUseDefaultLimits(t *testing.T) {
	ms, err := NewMemoryStore(nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()
	if !reflect.DeepEqual(ms.limits, DefaultStoreLimits) {
		t.Fatalf("Default limits are not used: %v\n", ms.limits)
	}
}

func TestMSNothingRecoveredOnFreshStart(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testNothingRecoveredOnFreshStart(t, ms)
}

func TestMSNewChannel(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testNewChannel(t, ms)
}

func TestMSCloseIdempotent(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testCloseIdempotent(t, ms)
}

func TestMSBasicMsgStore(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testBasicMsgStore(t, ms)
}

func TestMSMsgsState(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testMsgsState(t, ms)
}

func TestMSMaxMsgs(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testMaxMsgs(t, ms)
}

func TestMSMaxChannels(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	limitCount := 2

	limits := testDefaultStoreLimits
	limits.MaxChannels = limitCount

	if err := ms.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	testMaxChannels(t, ms, limitCount)

	// Set the limit to 0
	limits.MaxChannels = 0
	if err := ms.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	// Now try to test the limit against
	// any value, it should not fail
	testMaxChannels(t, ms, 0)
}

func TestMSMaxSubs(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	limitCount := 2

	limits := testDefaultStoreLimits
	limits.MaxSubscriptions = limitCount

	if err := ms.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	testMaxSubs(t, ms, "foo", limitCount)

	// Set the limit to 0
	limits.MaxSubscriptions = 0
	if err := ms.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	// Now try to test the limit against
	// any value, it should not fail
	testMaxSubs(t, ms, "bar", 0)
}

func TestMSMaxAge(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testMaxAge(t, ms)

	// Store a message
	storeMsg(t, ms, "foo", []byte("msg"))
	// Verify timer is set
	ms.RLock()
	cs := ms.LookupChannel("foo")
	timerSet := cs.Msgs.(*MemoryMsgStore).ageTimer != nil
	ms.RUnlock()
	if !timerSet {
		t.Fatal("Timer should have been set")
	}
}

func TestMSBasicSubStore(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testBasicSubStore(t, ms)
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

func TestMSPerChannelLimits(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testPerChannelLimits(t, ms)
}

func TestMSIncrementalTimestamp(t *testing.T) {
	// This test need to run without race and may take some time, so
	// excluding from Travis. Check presence of a known TRAVIS env
	// variable to detect that we run on Travis so we can skip this
	// test.
	if util.RaceEnabled || os.Getenv("TRAVIS_GO_VERSION") != "" {
		t.SkipNow()
	}
	ms := createDefaultMemStore(t)
	defer ms.Close()

	testIncrementalTimestamp(t, ms)
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
	ms, err := NewMemoryStore(nil)
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
