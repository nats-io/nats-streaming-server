// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan-server/util"
)

const (
	// CAUTION! Tests will remove the directory and all its content,
	// so pick a directory where there is nothing.
	defaultDataStore = "../data"
)

func cleanupDatastore(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("Error cleanup datastore: %v", err)
	}
}

func createDefaultFileStore(t *testing.T) *FileStore {
	fs, _, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
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

	testBasicCreate(t, fs, TypeFile)
}

func TestFSUseDefaultLimits(t *testing.T) {
	fs, _, err := NewFileStore(defaultDataStore, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()
	if !reflect.DeepEqual(fs.limits, DefaultChannelLimits) {
		t.Fatalf("Default limits are not used: %v\n", fs.limits)
	}
}

func TestFSUnsupportedFileVersion(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()
	storeMsg(t, fs, "foo", []byte("test"))
	storeSub(t, fs, "foo")

	// Close store
	fs.Close()

	// Overwrite the file version of a message store to an unsupported version
	writeVersion(t, filepath.Join(defaultDataStore, "foo", "msgs.1.dat"), fileVersion+1)

	var err error

	// Recover store
	fs, _, err = NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err == nil {
		fs.Close()
		t.Fatal("Expected an error")
	}
	fileVerStr := fmt.Sprintf("%d", (fileVersion + 1))
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}

	// Restore the correct version.
	writeVersion(t, filepath.Join(defaultDataStore, "foo", "msgs.1.dat"), fileVersion)

	// Overwrite the file version of the subscriptions store to an unsupported version
	writeVersion(t, filepath.Join(defaultDataStore, "foo", "subs.dat"), fileVersion+1)

	// Recover store
	fs, _, err = NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err == nil {
		fs.Close()
		t.Fatal("Expected an error")
	}
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}
}

func writeVersion(t *testing.T, fileName string, version int) {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := util.WriteInt(file, version); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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

	foo1 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo2 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo3 := storeMsg(t, fs, "foo", []byte("foomsg"))

	bar1 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar2 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar3 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar4 := storeMsg(t, fs, "bar", []byte("barmsg"))

	sub1 := storeSub(t, fs, "foo")
	sub2 := storeSub(t, fs, "bar")

	storeSubPending(t, fs, "foo", sub1, foo1.Sequence, foo2.Sequence, foo3.Sequence)
	storeSubAck(t, fs, "foo", sub1, foo1.Sequence, foo3.Sequence)

	storeSubPending(t, fs, "bar", sub2, bar1.Sequence, bar2.Sequence, bar3.Sequence, bar4.Sequence)
	storeSubAck(t, fs, "bar", sub2, bar4.Sequence)

	fs.Close()

	fs, state, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	// Check that subscriptions are restored
	for channel, recoveredSubs := range state {
		if len(recoveredSubs) != 1 {
			t.Fatalf("Incorrect size of recovered subs. Expected 1, got %v ", len(recoveredSubs))
		}
		recSub := recoveredSubs[0]
		subID := recSub.Sub.ID

		switch channel {
		case "foo":
			if subID != sub1 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub1, subID)
			}
			for _, m := range recSub.Pending {
				if m.Sequence != foo2.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub1: %v", m.Sequence)
				}
			}
			break
		case "bar":
			if subID != sub2 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub2, subID)
			}
			for _, m := range recSub.Pending {
				if m.Sequence != bar1.Sequence && m.Sequence != bar2.Sequence && m.Sequence != bar3.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub2: %v", m.Sequence)
				}
			}
			break
		default:
			t.Fatalf("Recovered unknown channel: %v", channel)
		}
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	// In message store, the first message should still be foo1,
	// regardless of what has been consumed.
	m := cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != foo1.Sequence {
		t.Fatalf("Unexpected message for foo channel: %v", m)
	}
	// Check all recovered messages are marked as Redelivered
	checkRedelivered := func(ms MsgStore) bool {
		start, end := ms.FirstAndLastSequence()
		for i := start; i <= end; i++ {
			if m := ms.Lookup(i); m != nil && !m.Redelivered {
				return false
			}
		}
		return true
	}
	if !checkRedelivered(cs.Msgs) {
		t.Fatalf("Not all messages have been marked as redelivered")
	}

	cs = fs.LookupChannel("bar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	// In message store, the first message should still be bar1,
	// regardless of what has been consumed.
	m = cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != bar1.Sequence {
		t.Fatalf("Unexpected message for bar channel: %v", m)
	}
	if !checkRedelivered(cs.Msgs) {
		t.Fatalf("Not all messages have been marked as redelivered")
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
	fs, state, err := NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	// Make sure that all our channels are recovered.
	if state == nil || len(state) != chanCount {
		t.Fatalf("Unexpected count of recovered channels. Expected %v, got %v", chanCount, len(state))
	}
	// Make sure that all our subscriptions are recovered.
	for _, recoveredSubs := range state {
		if len(recoveredSubs) != subsCount {
			t.Fatalf("Unexpected count of recovered subs. Expected %v, got %v", subsCount, len(recoveredSubs))
		}
	}
	// Make sure that all messages are recovered
	recMsg, recBytes, err := fs.MsgsState(AllChannels)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != expectedMsgCount {
		t.Fatalf("Unexpected count of recovered msgs. Expected %v, got %v", expectedMsgCount, recMsg)
	}
	if recBytes != expectedMsgBytes {
		t.Fatalf("Unexpected count of recovered bytes: Expected %v, got %v", expectedMsgBytes, recBytes)
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
	fs, state, err := NewFileStore(defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if !fs.HasChannel() || state == nil || len(state) != 1 || state["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// Only sub2 should be recovered
	recoveredSubs := state["foo"]
	if len(recoveredSubs) != 1 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}
	// Make sure the subs count was not messed-up by the fact
	// that the store recovered delete requests for un-recovered
	// subscriptions.
	// Since we have set the limit of subs to 1, and we have
	// recovered one, we should fail creating a new one.
	cs := fs.LookupChannel("foo")
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

func TestFSNoSubIdCollisionAfterRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store a subscription.
	sub1 := storeSub(t, fs, "foo")

	// Close the store
	fs.Close()

	// Recovers now
	fs, state, err := NewFileStore(defaultDataStore, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if !fs.HasChannel() || state == nil || len(state) != 1 || state["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 should be recovered
	recoveredSubs := state["foo"]
	if len(recoveredSubs) != 1 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}

	// Store new subscription
	sub2 := storeSub(t, fs, "foo")

	if sub2 <= sub1 {
		t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", sub1+1, sub2)
	}

	// Store a delete subscription with higher ID and make sure
	// we use something higher on restart
	delSub := uint64(sub1 + 10)
	storeSubDelete(t, fs, "foo", delSub)

	// Close the store
	fs.Close()

	// Recovers now
	fs, state, err = NewFileStore(defaultDataStore, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if !fs.HasChannel() || state == nil || len(state) != 1 || state["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 & sub2 should be recovered
	recoveredSubs = state["foo"]
	if len(recoveredSubs) != 2 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}

	// Store new subscription
	sub3 := storeSub(t, fs, "foo")

	if sub3 <= sub1 || sub3 <= delSub {
		t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", delSub+1, sub3)
	}
}

func TestFSGetSeqFromTimestamp(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testGetSeqFromStartTime(t, fs)
}
