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

func TestMSLimits(t *testing.T) {

	limitCount := 100

	limits := DefaultChannelLimits
	limits.MaxNumMsgs = limitCount

	ms, err := NewMemoryStore(limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Close()

	testLimits(t, ms, limitCount)
}
