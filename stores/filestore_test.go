// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"os"
	"testing"

	"github.com/nats-io/stan-server/spb"
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
	fs, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
			if len(recoveredSubs) != 2 {
				t.Fatalf("Should have recovered 2 subscriptions, got %v", len(recoveredSubs))
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

func TestFSRecoveryLimitsNotApplied(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store some messages in various channels
	chanCount := 10
	msgCount := 50
	subsCount := 5
	payload := []byte("hello")
	expectedMsgCount := chanCount * msgCount
	expectedMsgBytes := uint64(expectedMsgCount * len(payload))
	for c := 0; c < chanCount; c++ {
		channelName := fmt.Sprintf("channel.%d", (c + 1))

		// Create a several subscriptions per channel.
		for s := 0; s < subsCount; s++ {
			storeSub(t, fs, channelName)
		}

		for m := 0; m < msgCount; m++ {
			storeMsg(t, fs, channelName, payload)
		}
	}

	// Close the store
	fs.Close()

	// Now re-open with limits below all the above counts
	limit := testDefaultChannelLimits
	limit.MaxChannels = 1
	limit.MaxNumMsgs = 1
	limit.MaxSubs = 1
	fs, err := NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	// Make sure that all our channels are recovered.
	channels := fs.GetChannels()
	if channels == nil {
		t.Fatal("Channels should have been recovered")
	}
	if len(channels) != chanCount {
		t.Fatalf("Unexpected count of recovered channels: %v vs %v", len(channels), chanCount)
	}
	// Make sure that all our subscriptions are recovered.
	for _, c := range channels {
		recoveredSubs := c.Subs.GetRecoveredState()
		if len(recoveredSubs) != subsCount {
			t.Fatalf("Unexpected count of recovered subs: %v vs %v", len(recoveredSubs), subsCount)
		}
		c.Subs.ClearRecoverdState()
	}
	// Make sure that all messages are recovered
	recMsg, recBytes, err := fs.MsgsState(AllChannels)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != expectedMsgCount {
		t.Fatalf("Unexpected count of recovered msgs: %v vs %v", recMsg, expectedMsgCount)
	}
	if recBytes != expectedMsgBytes {
		t.Fatalf("Unexpected count of recovered bytes: %v vs %v", recMsg, expectedMsgBytes)
	}

	// Now check that any new addition would be rejected
	if _, _, err := fs.LookupOrCreateChannel("new.channel"); err == nil {
		t.Fatal("Expected trying to create a new channel to fail")
	}
	channelOne := fs.LookupChannel("channel.1")
	if channelOne == nil {
		t.Fatal("Expected channel.1 to exist")
	}
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := channelOne.Subs.CreateSub(sub); err == nil {
		t.Fatal("Expected trying to create a new subscription to fail")
	}

	// TODO: Check for messages. Need to resolve how we enforce limits.
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

func TestFSSubStoreGetRecoveredNotNil(t *testing.T) {
	fs := createDefaultFileStore(t)
	defer fs.Close()

	testSubStoreGetRecoveredNotNil(t, fs)
}

func TestFSBasicSubStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicSubStore(t, fs)
}

func TestFSRecoverSubUpdatesForDeleteSubOK(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store one sub for which we are going to store updates
	// and then delete
	sub1 := storeSub(t, fs, "foo")
	// This one will stay and should be recovered
	sub2 := storeSub(t, fs, "foo")

	// Add several pending seq for sub1
	storeSubPending(t, fs, "foo", sub1, 1, 2, 3)

	// Delete sub
	storeSubDelete(t, fs, "foo", sub1)

	// Add more updates
	storeSubPending(t, fs, "foo", sub1, 4, 5)
	storeSubAck(t, fs, "foo", sub1, 1)

	// Delete unexisting subs
	storeSubDelete(t, fs, "foo", sub2+1, sub2+2, sub2+3)

	// Close the store
	fs.Close()

	// Recovers now, should not have any error
	limits := testDefaultChannelLimits
	limits.MaxSubs = 1
	fs, err := NewFileStore(defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if !fs.HasChannel() {
		t.Fatal("Channel foo should have been recovered")
	}

	// Only sub2 should be recovered
	cs := fs.GetChannels()["foo"]
	recoveredSubs := cs.Subs.GetRecoveredState()
	if len(recoveredSubs) != 1 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}
	// Make sure the subs count was not messed-up by the fact
	// that the store recovered delete requests for un-recovered
	// subscriptions.
	// Since we have set the limit of subs to 1, and we have
	// recovered one, we should fail creating a new one.
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := cs.Subs.CreateSub(sub); err == nil || err != ErrTooManySubs {
		t.Fatalf("Should have failed creating a sub, got %v", err)
	}
}
