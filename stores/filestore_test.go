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
	"io/ioutil"
)

var testDefaultServerInfo = spb.ServerInfo{
	ClusterID:   "id",
	Discovery:   "discovery",
	Publish:     "publish",
	Subscribe:   "subscribe",
	Unsubscribe: "unsubscribe",
	Close:       "close",
}

var defaultDataStore string

func init() {
	tmpDir, err := ioutil.TempDir(".", "data_stores_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	defaultDataStore = tmpDir
}

func cleanupDatastore(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		stackFatalf(t, "Error cleaning up datastore: %v", err)
	}
}

func createDefaultFileStore(t *testing.T) *FileStore {
	fs, state, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs
}

func openDefaultFileStore(t *testing.T) (*FileStore, *RecoveredState) {
	fs, state, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	return fs, state
}

func expectedErrorOpeningDefaultFileStore(t *testing.T) error {
	fs, _, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
	if err == nil {
		fs.Close()
		stackFatalf(t, "Expected an error opening the FileStore, got none")
	}
	return err
}

func TestFSBasicCreate(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicCreate(t, fs, TypeFile)
}

func TestFSInit(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Init is done in createDefaultFileStore().
	// A second call to Init() should not fail, and data should be replaced.
	newInfo := testDefaultServerInfo
	newInfo.ClusterID = "newID"
	if err := fs.Init(&newInfo); err != nil {
		t.Fatalf("Unexpected failure on store init: %v", err)
	}

	// Close the store
	fs.Close()

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	// Check content
	info := *state.Info
	if !reflect.DeepEqual(newInfo, info) {
		t.Fatalf("Unexpected server info, expected %v, got %v",
			newInfo, info)
	}
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

	// Recover store (should fail)
	err = expectedErrorOpeningDefaultFileStore(t)
	fileVerStr := fmt.Sprintf("%d", (fileVersion + 1))
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}

	// Restore the correct version.
	writeVersion(t, filepath.Join(defaultDataStore, "foo", "msgs.1.dat"), fileVersion)

	// Overwrite the file version of the subscriptions store to an unsupported version
	writeVersion(t, filepath.Join(defaultDataStore, "foo", "subs.dat"), fileVersion+1)

	// Recover store (should fail)
	err = expectedErrorOpeningDefaultFileStore(t)
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

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	// Check that subscriptions are restored
	for channel, recoveredSubs := range subs {
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
	// Check that messages recovered from MsgStore are never
	// marked as redelivered.
	checkRedelivered := func(ms MsgStore) bool {
		start, end := ms.FirstAndLastSequence()
		for i := start; i <= end; i++ {
			if m := ms.Lookup(i); m != nil && m.Redelivered {
				return true
			}
		}
		return false
	}
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
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
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
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
	limit.MaxNumMsgs = 4
	limit.MaxSubs = 1
	fs, state, err := NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	// Make sure that all our channels are recovered.
	if len(subs) != chanCount {
		t.Fatalf("Unexpected count of recovered channels. Expected %v, got %v", chanCount, len(state.Subs))
	}
	// Make sure that all our subscriptions are recovered.
	for _, recoveredSubs := range subs {
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

	// Store one message
	lastMsg := storeMsg(t, fs, "channel.1", payload)

	// Check limits (should be 4 msgs)
	recMsg, recBytes, err = fs.MsgsState("channel.1")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != limit.MaxNumMsgs {
		t.Fatalf("Unexpected count of recovered msgs. Expected %v, got %v", limit.MaxNumMsgs, recMsg)
	}
	expectedMsgBytes = uint64(limit.MaxNumMsgs * len(payload))
	if recBytes != expectedMsgBytes {
		t.Fatalf("Unexpected count of recovered bytes: Expected %v, got %v", expectedMsgBytes, recBytes)
	}

	cs := fs.channels["channel.1"]
	msgStore := cs.Msgs.(*FileMsgStore)

	// Check first avail message sequence
	expectedNewFirstSeq := uint64((msgCount + 1 - limit.MaxNumMsgs) + 1)
	if msgStore.first != expectedNewFirstSeq {
		t.Fatalf("Expected first sequence to be %v, got %v", expectedNewFirstSeq, msgStore.first)
	}
	// We should have moved to the second slice
	if msgStore.currSliceIdx != 1 {
		t.Fatalf("Expected file slice to be the second one, got %v", msgStore.currSliceIdx)
	}
	// Check second slice content
	secondSlice := msgStore.files[1]
	if secondSlice.msgsCount != 1 {
		t.Fatalf("Expected second slice to have 1 mesage, got %v", secondSlice.msgsCount)
	}
	if secondSlice.firstMsg != lastMsg {
		t.Fatalf("Expected last message to be %v, got %v", lastMsg, secondSlice.firstMsg)
	}
	// The first slice should have the new limit msgs count - 1.
	firstSlice := msgStore.files[0]
	if firstSlice.msgsCount != limit.MaxNumMsgs-1 {
		t.Fatalf("Expected first slice to have %v msgs, got %v", limit.MaxNumMsgs-1, firstSlice.msgsCount)
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

	testMaxMsgs(t, fs)
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

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// Only sub2 should be recovered
	recoveredSubs := subs["foo"]
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
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 should be recovered
	recoveredSubs := subs["foo"]
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
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs = state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 & sub2 should be recovered
	recoveredSubs = subs["foo"]
	if len(recoveredSubs) != 2 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}

	// Store new subscription
	sub3 := storeSub(t, fs, "foo")

	if sub3 <= sub1 || sub3 <= delSub {
		t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", delSub+1, sub3)
	}
}

func TestFSSubLastSentCorrectOnRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store a subscription.
	subID := storeSub(t, fs, "foo")

	// A message
	msg := []byte("hello")

	// Store msg seq 1 and 2
	m1 := storeMsg(t, fs, "foo", msg)
	m2 := storeMsg(t, fs, "foo", msg)

	// Store m1 and m2 for this subscription, then m1 again.
	storeSubPending(t, fs, "foo", subID, m1.Sequence, m2.Sequence, m1.Sequence)

	// Restart server
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("State should have been recovered")
	}
	subs := state.Subs["foo"]
	if subs == nil || len(subs) != 1 {
		t.Fatalf("One subscription should have been recovered, got %v", len(subs))
	}
	sub := subs[0]
	// Check that sub's last seq is m2.Sequence
	if sub.Sub.LastSent != m2.Sequence {
		t.Fatalf("Expected LastSent to be %v, got %v", m2.Sequence, sub.Sub.LastSent)
	}
}

func TestFSUpdatedSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Creeate a subscription.
	subID := storeSub(t, fs, "foo")

	// A message
	msg := []byte("hello")

	// Store msg seq 1 and 2
	m1 := storeMsg(t, fs, "foo", msg)
	m2 := storeMsg(t, fs, "foo", msg)
	m3 := storeMsg(t, fs, "foo", msg)

	// Store m1 and m2 for this subscription
	storeSubPending(t, fs, "foo", subID, m1.Sequence, m2.Sequence)

	// Update the subscription
	cs, _, _ := fs.LookupOrCreateChannel("foo")
	ss := cs.Subs
	updatedSub := &spb.SubState{
		ID:            subID,
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      "newAckInbox",
		AckWaitInSecs: 10,
	}
	if err := ss.UpdateSub(updatedSub); err != nil {
		t.Fatalf("Error updating subscription: %v", err)
	}
	// Store m3 for this subscription
	storeSubPending(t, fs, "foo", subID, m3.Sequence)

	// Store a subscription with update only, should be recovered
	subWithoutNew := &spb.SubState{
		ID:            subID + 1,
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.UpdateSub(subWithoutNew); err != nil {
		t.Fatalf("Error updating subscription: %v", err)
	}

	// Restart server
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("State should have been recovered")
	}
	subs := state.Subs["foo"]
	if subs == nil || len(subs) != 2 {
		t.Fatalf("Two subscriptions should have been recovered, got %v", len(subs))
	}
	// Subscriptions are recovered from a map, and then returned as an array.
	// There is no guarantee that we get them in the order they were persisted.
	for _, s := range subs {
		if s.Sub.ID == subID {
			// Check that sub's last seq is m3.Sequence
			if s.Sub.LastSent != m3.Sequence {
				t.Fatalf("Expected LastSent to be %v, got %v", m3.Sequence, s.Sub.LastSent)
			}
			// Update lastSent since we know it is correct.
			updatedSub.LastSent = m3.Sequence
			// Now compare that what we recovered is same that we used to update.
			if !reflect.DeepEqual(*s.Sub, *updatedSub) {
				t.Fatalf("Expected subscription to be %v, got %v", updatedSub, s.Sub)
			}
		} else if s.Sub.ID == subID+1 {
			// Compare that what we recovered is same that we used to update.
			if !reflect.DeepEqual(*s.Sub, *subWithoutNew) {
				t.Fatalf("Expected subscription to be %v, got %v", subWithoutNew, s.Sub)
			}
		} else {
			t.Fatalf("Unexpected subscription ID: %v", s.Sub.ID)
		}
	}
}

func TestFSGetSeqFromTimestamp(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testGetSeqFromStartTime(t, fs)
}

func TestFSBadClientFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the client's file
	fileName := filepath.Join(defaultDataStore, clientsFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the client's file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
	// Now test with various unexpected content
	contents := []string{"Nothing as expected", "A clientID-alone", "A", "D"}
	for _, c := range contents {
		// Delete the previous client file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Now create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating client file: %v", err)
		}
		// Add something that is not what we expect to read back
		if _, err := file.WriteString(c); err != nil {
			t.Fatalf("Unexpected error writing content: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("Unexpected error closing file: %v", err)
		}
		// We should fail to create the filestore
		fs, _, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits)
		if err == nil {
			fs.Close()
			t.Fatalf("Expected error opening file store with content %q, got none", c)
		}
	}
}

func TestFSAddDeleteClient(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testAddDeleteClient(t, fs)

	// Restart the store
	fs.Close()

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	if len(state.Clients) != 2 {
		t.Fatalf("Expected 2 clients to be recovered, got %v", len(state.Clients))
	}
	for _, c := range state.Clients {
		if c.ClientID != "client2" && c.ClientID != "client3" {
			t.Fatalf("Unexpected recovered client: %v", c.ClientID)
		}
	}
}

func TestFSBadServerFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the server's file
	fileName := filepath.Join(defaultDataStore, serverFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the client's file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating client file: %v", err)
		}
		return file
	}

	// Now test with two ServerInfos, and expect to fail
	file := resetToValidFile()

	// Write two server info records
	for i := 0; i < 2; i++ {
		info := testDefaultServerInfo
		b, _ := info.Marshal()
		// Write the size of the proto buf
		if err := util.WriteInt(file, info.Size()); err != nil {
			t.Fatalf("Error writing zie: %v", err)
		}
		// Write content
		if _, err := file.Write(b); err != nil {
			t.Fatalf("Error writing info: %v", err)
		}
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record, but with the size that's
	// more than the actual record.
	file = resetToValidFile()
	info := testDefaultServerInfo
	b, _ := info.Marshal()
	// Write the incorrect size (too big) of the proto buf
	if err := util.WriteInt(file, info.Size()+10); err != nil {
		t.Fatalf("Error writing zie: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record, but with the size that's
	// less than the actual record.
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the incorrect size (too small) of the proto buf
	if err := util.WriteInt(file, info.Size()-10); err != nil {
		t.Fatalf("Error writing zie: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record and then extra data
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the size of the proto buf
	if err := util.WriteInt(file, info.Size()); err != nil {
		t.Fatalf("Error writing zie: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Write some extra content
	if _, err := file.Write([]byte("more data")); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record but corrupt the protobuf
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the size of the proto buf
	if err := util.WriteInt(file, info.Size()); err != nil {
		t.Fatalf("Error writing zie: %v", err)
	}
	// Alter the content
	copy(b, []byte("hello"))
	// Write the corrupted content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
}
