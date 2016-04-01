// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"os"
	"testing"
)

const (
	defaultDataStore = "../data"
)

func cleanupDatastore(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("Error cleanup datastore: %v", err)
	}
}

func createDefaultFileStore(t *testing.T) *FileStore {
	fs, err := NewFileStore(defaultDataStore)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fs.SetChannelLimits(testDefaultChannelLimits)
	return fs
}

func TestFSBasicCreate(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicCreate(t, fs, "FILESTORE")
}

func TestFSNothingRecoveredOnFreshStart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testNothingRecoveredOnFreshStart(t, fs)
}

func TestFSNewChannel(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testNewChannel(t, fs)
}

func TestFSCloseIdempotent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testCloseIdempotent(t, fs)
}

func TestFSBasicMsgStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicMsgStore(t, fs)
}

func TestFSBasicRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fooRecovered := false
	barRecovered := false

	fs := createDefaultFileStore(t)
	defer fs.Close()

	if fs.LookupChannel("foo") != nil {
		fooRecovered = true
	}
	if fs.LookupChannel("bar") != nil {
		barRecovered = true
	}

	// Nothing should be recovered
	if fooRecovered || barRecovered {
		t.Fatalf("Unexpected recovery: foo=%v bar=%v", fooRecovered, barRecovered)
	}

	storeMsg(t, fs, "foo", []byte("foomsg"))
	storeMsg(t, fs, "bar", []byte("barmsg"))

	sub1 := storeSub(t, fs, "foo")
	sub2 := storeSub(t, fs, "bar")

	storeSubPending(t, fs, "foo", sub1, 1, 2, 3)
	storeSubAck(t, fs, "foo", sub1, 1, 3)

	storeSubPending(t, fs, "bar", sub2, 1, 2, 3, 4)
	storeSubAck(t, fs, "bar", sub1, 4)

	fs.Close()

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Check that subscriptions are restored
	if channels := fs.GetChannels(); channels == nil || len(channels) != 2 {
		for _, c := range channels {
			recoveredSubs := c.Subs.GetRecoveredState()
			if recoveredSubs == nil || len(recoveredSubs) != 2 {
				t.Fatal("Should have recovered 2 subscriptions")
			}
			for subID, recSub := range recoveredSubs {
				if subID != sub1 || subID != sub2 {
					t.Fatalf("Recovered unknown subscription: %v", subID)
				} else {
					for s, _ := range recSub.Seqnos {
						if subID == sub1 {
							if s != 2 {
								t.Fatalf("Unexpected recovered pending seqno for sub1: %v", s)
							}
						} else {
							if s < 1 || s > 3 {
								t.Fatalf("Unexpected recovered pending seqno for sub2: %v", s)
							}
						}
					}
				}
			}
			c.Subs.ClearRecoverdState()
		}
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	m := cs.Msgs.FirstMsg()
	if m == nil || string(m.Data) != "foomsg" {
		t.Fatalf("Unexpected message for foo channel: %v", m)
	}

	cs = fs.LookupChannel("bar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	m = cs.Msgs.FirstMsg()
	if m == nil || string(m.Data) != "barmsg" {
		t.Fatalf("Unexpected message for bar channel: %v", m)
	}

	cs = fs.LookupChannel("baz")
	if cs != nil {
		t.Fatal("Expected to get nil channel for baz, got something instead")
	}
}

func TestFSMsgsState(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testMsgsState(t, fs)
}

func TestFSMaxMsgs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limitCount := 100

	limits := testDefaultChannelLimits
	limits.MaxNumMsgs = limitCount

	fs.SetChannelLimits(limits)

	testMaxMsgs(t, fs, limitCount)
}

func TestFSMaxChannels(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limitCount := 2

	limits := testDefaultChannelLimits
	limits.MaxChannels = limitCount

	fs.SetChannelLimits(limits)

	testMaxChannels(t, fs, limitCount)
}

func TestFSMaxSubs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limitCount := 2

	limits := testDefaultChannelLimits
	limits.MaxSubs = limitCount

	fs.SetChannelLimits(limits)

	testMaxSubs(t, fs, limitCount)
}

func TestFSBasicSubStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicSubStore(t, fs)
}
