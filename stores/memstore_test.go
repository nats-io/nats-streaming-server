// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"os"
	"reflect"
	"testing"

	"github.com/nats-io/nats-streaming-server/util"
)

func createDefaultMemStore(t tLogger) *MemoryStore {
	limits := testDefaultStoreLimits
	ms, err := NewMemoryStore(testLogger, &limits)
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
