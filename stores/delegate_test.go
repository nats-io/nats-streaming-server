// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"testing"
)

func TestDelegateStore(t *testing.T) {
	// First create a MemStore
	ms := createDefaultMemStore(t)
	defer ms.Close()

	// Now a delegate store and use this mem store.
	ds := &DelegateStore{S: ms}
	// Call all APIs

	if locked, err := ds.GetExclusiveLock(); locked || err.Error() != "not supported" {
		t.Fatalf("GetExclusiveLock returned: %v, %v", locked, err)
	}
	if err := ds.Init(nil); err != nil {
		t.Fatalf("Init returned: %v", err)
	}
	if ds.Name() != TypeMemory {
		t.Fatalf("Expected memory store, got %v", ds.Name())
	}
	if state, err := ds.Recover(); state != nil || err != nil {
		t.Fatalf("Recover returned: %v - %v", state, err)
	}
	limits := DefaultStoreLimits
	if err := ds.SetLimits(&limits); err != nil {
		t.Fatalf("Error on SetLimits: %v", err)
	}
	if cs, isNew, err := ds.CreateChannel("foo", nil); cs == nil || !isNew || err != nil {
		t.Fatalf("CreateChannel returned: %v - %v - %v", cs, isNew, err)
	}
	if cs := ds.LookupChannel("foo"); cs == nil {
		t.Fatal("LookupChannel should have returned a channel store")
	}
	if !ds.HasChannel() {
		t.Fatal("HasChannel should have returned true")
	}
	if n, s, err := ds.MsgsState("foo"); n != 0 || s != 0 || err != nil {
		t.Fatalf("MsgState returned: %v - %v - %v", n, s, err)
	}
	if c, isNew, err := ds.AddClient("me", "inbox", nil); c == nil || !isNew || err != nil {
		t.Fatalf("AddClient returned: %v - %v - %v", c, isNew, err)
	}
	if c := ds.GetClient("me"); c == nil || c.ID != "me" {
		t.Fatalf("GetClient returned: %v", c)
	}
	if m := ds.GetClients(); m == nil || len(m) != 1 || m["me"] == nil {
		t.Fatalf("GetClients returned: %v", m)
	}
	if c := ds.GetClientsCount(); c != 1 {
		t.Fatalf("GetClientsCount returned: %v", c)
	}
	if c := ds.DeleteClient("me"); c == nil || c.ID != "me" {
		t.Fatalf("DeleteClient returned: %v", c)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close returned: %v", err)
	}
}
