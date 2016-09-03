// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"github.com/nats-io/nats-streaming-server/spb"
	"reflect"
	"testing"
)

func createDefaultMemStore(t *testing.T) *MemoryStore {
	ms, err := NewMemoryStore(&testDefaultChannelLimits)
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
	if !reflect.DeepEqual(ms.limits, DefaultChannelLimits) {
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

	limits := testDefaultChannelLimits
	limits.MaxChannels = limitCount

	ms.SetChannelLimits(limits)

	testMaxChannels(t, ms, limitCount)

	// Set the limit to 0
	limits.MaxChannels = 0
	ms.SetChannelLimits(limits)
	// Now try to test the limit against
	// any value, it should not fail
	testMaxChannels(t, ms, 0)
}

func TestMSMaxSubs(t *testing.T) {
	ms := createDefaultMemStore(t)
	defer ms.Close()

	limitCount := 2

	limits := testDefaultChannelLimits
	limits.MaxSubs = limitCount

	ms.SetChannelLimits(limits)

	testMaxSubs(t, ms, "foo", limitCount)

	// Set the limit to 0
	limits.MaxSubs = 0
	ms.SetChannelLimits(limits)
	// Now try to test the limit against
	// any value, it should not fail
	testMaxSubs(t, ms, "bar", 0)
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
