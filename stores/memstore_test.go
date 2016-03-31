package stores

import (
	"testing"
)

func TestMSBasicCreate(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testBasicCreate(t, ms, "MEMORY")
}

func TestMSNothingRecoveredOnFreshStart(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testNothingRecoveredOnFreshStart(t, ms)
}

func TestMSNewChannel(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testNewChannel(t, ms)
}

func TestMSCloseIdempotent(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testCloseIdempotent(t, ms)
}

func TestMSBasicMsgStore(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testBasicMsgStore(t, ms)
}

func TestMSMsgsState(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testMsgsState(t, ms)
}

func TestMSMaxMsgs(t *testing.T) {

	limitCount := 100

	limits := DefaultChannelLimits
	limits.MaxNumMsgs = limitCount

	ms, err := NewMemoryStore(limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testMaxMsgs(t, ms, limitCount)
}

func TestMSMaxChannels(t *testing.T) {
	limitCount := 2

	limits := DefaultChannelLimits
	limits.MaxChannels = limitCount

	ms, err := NewMemoryStore(limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testMaxChannels(t, ms, limitCount)
}

func TestMSMaxSubs(t *testing.T) {
	limitCount := 2

	limits := DefaultChannelLimits
	limits.MaxSubs = limitCount

	ms, err := NewMemoryStore(limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testMaxSubs(t, ms, limitCount)
}

func TestMSBasicSubStore(t *testing.T) {
	ms, err := NewMemoryStore(DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testBasicSubStore(t, ms)
}
