// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"reflect"
	"testing"
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

func TestMSNegativeLimitsOnCreate(t *testing.T) {
	limits := DefaultStoreLimits
	limits.MaxMsgs = -1000
	if ms, err := NewMemoryStore(testLogger, &limits); ms != nil || err == nil {
		if ms != nil {
			ms.Close()
		}
		t.Fatal("Should have failed to create store with a negative limit")
	}
}
