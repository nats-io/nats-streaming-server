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
	"reflect"
	"testing"
	"time"
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

func TestMSMsgStoreEmpty(t *testing.T) {
	s := createDefaultMemStore(t)
	defer s.Close()

	limits := StoreLimits{}
	limits.MaxAge = 250 * time.Millisecond
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", err)
	}

	cs := storeCreateChannel(t, s, "foo")

	// Send some messages
	for i := 0; i < 3; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("hello"))
	}
	// Then empty the message store
	if err := cs.Msgs.Empty(); err != nil {
		t.Fatalf("Error on Empty(): %v", err)
	}

	ms := cs.Msgs.(*MemoryMsgStore)
	ms.RLock()
	if ms.ageTimer != nil {
		ms.RUnlock()
		t.Fatal("AgeTimer not nil")
	}
	if ms.first != 0 || ms.last != 0 {
		ms.RUnlock()
		t.Fatalf("First and/or Last not reset")
	}
	ms.RUnlock()
}
