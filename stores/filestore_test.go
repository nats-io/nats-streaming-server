// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
	"hash/crc32"
	"io"
	"io/ioutil"
	"time"
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

func TestFSOptions(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Check that default options are used
	fs.RLock()
	opts := fs.opts
	fs.RUnlock()

	checkOpts := func(expected, actual FileStoreOptions) {
		if !reflect.DeepEqual(actual, expected) {
			stackFatalf(t, "Expected options to be %v, got %v", expected, actual)
		}
	}
	expected := DefaultFileStoreOptions
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs := fs.LookupChannel("foo")
	ss := cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	// Now try to set the options in the constructor
	fs.Close()
	cleanupDatastore(t, defaultDataStore)

	// Prepare the golden options with custom values
	expected = FileStoreOptions{
		BufferSize:           1025 * 1024,
		CompactEnabled:       false,
		CompactFragmentation: 60,
		CompactInterval:      60,
		CompactMinFileSize:   1024 * 1024,
		DoCRC:                false,
		CRCPolynomial:        int64(crc32.Castagnoli),
		DoSync:               false,
	}
	// Create the file with custom options
	fs, _, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits,
		BufferSize(expected.BufferSize),
		CompactEnabled(expected.CompactEnabled),
		CompactFragmentation(expected.CompactFragmentation),
		CompactInterval(expected.CompactInterval),
		CompactMinFileSize(expected.CompactMinFileSize),
		DoCRC(expected.DoCRC),
		CRCPolynomial(expected.CRCPolynomial),
		DoSync(expected.DoSync))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	fs.Close()
	cleanupDatastore(t, defaultDataStore)
	// Create the file with custom options, pass all of them at once
	fs, _, err = NewFileStore(defaultDataStore, &testDefaultChannelLimits, AllOptions(&expected))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)
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
	if _, _, err := fs.CreateChannel("new.channel", nil); err == nil {
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

func TestFSRecoveryFileSlices(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	fs.Close()

	limit := testDefaultChannelLimits
	limit.MaxNumMsgs = 4
	fs, state, err := NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	storeMsg(t, fs, "foo", []byte("msg1"))
	storeMsg(t, fs, "foo", []byte("msg2"))

	// Close the store
	fs.Close()

	fs, state = openDefaultFileStore(t)
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to be recovered")
	}
	msgStore := cs.Msgs.(*FileMsgStore)

	// We should have moved to the second slice
	if msgStore.currSliceIdx != 1 {
		t.Fatalf("Expected file slice to be the second one, got %v", msgStore.currSliceIdx)
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
	fs.Close()
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
	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
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
	fs.Close()
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

	clientID := "this-is-a-valid-client-id"
	cli := spb.ClientInfo{ID: clientID, HbInbox: "hbInbox"}
	b, _ := cli.Marshal()

	//
	// WRONG CRC
	//
	file := resetToValidFile()
	// Write the header
	if err := util.WriteInt(file, int(addClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write WRONG crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))+3); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHAL addClient ERROR
	//
	file = resetToValidFile()
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, int(addClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHAL delClient ERROR
	//
	file = resetToValidFile()
	// First write a valid addClient
	writeRecord(file, nil, addClient, &cli, crc32.IEEETable)
	// Then write an invalid delClient
	delCli := spb.ClientDelete{ID: clientID}
	b, _ = delCli.Marshal()
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, int(delClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// INVALID TYPE
	//
	file = resetToValidFile()
	b, _ = cli.Marshal()
	// Write the header
	if err := util.WriteInt(file, 99<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)
}

func TestFSClientAPIs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testClientAPIs(t, fs)

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
		if c.ID != "client2" && c.ID != "client3" {
			t.Fatalf("Unexpected recovered client: %v", c.ID)
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
			t.Fatalf("Error writing header: %v", err)
		}
		// Write crc
		if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
			t.Fatalf("Error writing crc: %v", err)
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
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
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
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
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
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
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
		t.Fatalf("Error writing header: %v", err)
	}
	// Alter the content
	copy(b, []byte("hello"))
	// Write a valid CRC of the corrupted protobuf. This is checking
	// that Unmarshall error is correctly captured.
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
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

func TestFSBadMsgFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	// Store a message
	storeMsg(t, fs, "foo", []byte("msg"))

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to exist")
	}
	msgStore := cs.Msgs.(*FileMsgStore)
	firstSliceFileName := msgStore.files[0].fileName

	// Close it
	fs.Close()

	// First delete the file...
	if err := os.Remove(firstSliceFileName); err != nil {
		t.Fatalf("Unable to delete the msg file %q: %v", firstSliceFileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(firstSliceFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(firstSliceFileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(firstSliceFileName)
		if err != nil {
			t.Fatalf("Error creating file: %v", err)
		}
		return file
	}

	//
	// INVALID CONTENT
	//
	file := resetToValidFile()
	if err := util.WriteInt(file, 5); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatalf("Error writing content: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHALL ERROR
	//
	file = resetToValidFile()
	msg := &pb.MsgProto{Sequence: 1, Data: []byte("this is a message")}
	b, _ := msg.Marshal()
	// overwrite with dummy content
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write CRC
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Unexpected error writing CRC: %v", err)
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
}

func TestFSBadSubFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	// Store a subscription
	storeSub(t, fs, "foo")

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to exist")
	}

	// Close it
	fs.Close()

	// First delete the file...
	fileName := filepath.Join(defaultDataStore, "foo", subsFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the subscriptions file %q: %v", fileName, err)
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
			t.Fatalf("Error creating file: %v", err)
		}
		return file
	}

	// Restore a valid file
	file := resetToValidFile()
	// Write size that causes read of content to EOF
	if err := util.WriteInt(file, 100); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Test with various types
	types := []recordType{subRecNew, subRecUpdate, subRecDel, subRecMsg, subRecAck, 99}
	content := []byte("abc")
	crc := crc32.ChecksumIEEE(content)
	for _, oneType := range types {
		// Restore a valid file
		file = resetToValidFile()
		// Write a type that does not exist
		if err := util.WriteInt(file, int(oneType)<<24|len(content)); err != nil {
			t.Fatalf("Error writing header: %v", err)
		}
		// Write CRC
		if err := util.WriteInt(file, int(crc)); err != nil {
			t.Fatalf("Error writing crc: %v", err)
		}
		// Write dummy content
		if _, err := file.Write(content); err != nil {
			t.Fatalf("Error writing info: %v", err)
		}
		// Close the file
		if err := file.Close(); err != nil {
			t.Fatalf("Unexpected error closing file: %v", err)
		}
		// We should fail to create the filestore
		expectedErrorOpeningDefaultFileStore(t)
	}
}

func TestFSSwapFiles(t *testing.T) {
	var tmpFile, activeFile *os.File
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
		}
		if activeFile != nil {
			activeFile.Close()
		}
		os.Remove("file.dat.tmp")
		os.Remove("file.dat")
	}()
	resetFiles := func() {
		if tmpFile != nil {
			tmpFile.Close()
		}
		if activeFile != nil {
			activeFile.Close()
		}
		tmpFileName := "file.dat.tmp"
		os.Remove(tmpFileName)
		activeFileName := "file.dat"
		os.Remove(activeFileName)

		var err error
		tmpFile, err = openFile(tmpFileName)
		if err != nil {
			stackFatalf(t, "Unexpected error creating file: %v", tmpFile)
		}
		activeFile, err = openFile(activeFileName)
		if err != nil {
			stackFatalf(t, "Unexpected error creating file: %v", activeFile)
		}
	}
	doSwapWithError := func() {
		f, err := swapFiles(tmpFile, activeFile)
		if err == nil {
			stackFatalf(t, "Expected error swapping files, got none")
		}
		if f != activeFile {
			stackFatalf(t, "Expected returned file to be the active file")
		}
	}

	resetFiles()
	// Invoke with a closed tmpFile
	tmpFile.Close()
	doSwapWithError()

	resetFiles()
	// Invoke with a closed active file
	activeFile.Close()
	doSwapWithError()

	resetFiles()
	// Success test
	activeFile, err := swapFiles(tmpFile, activeFile)
	if err != nil {
		t.Fatalf("Unexpected error on swap: %v", err)
	}
	if _, err := os.Stat("file.dat"); err != nil {
		t.Fatalf("Active file should exist")
	}
	if _, err := os.Stat("file.dat.tmp"); err == nil {
		t.Fatalf("Temp file should no longer exist")
	}
}

func TestFSAddClientError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Test failure of AddClient (generic tested in common_test.go)
	// Close the client file to cause error
	fs.clientsFile.Close()
	// Should fail
	if c, _, err := fs.AddClient("c1", "hbInbox", "test"); err == nil {
		t.Fatal("Expected error, got none")
	} else if c != nil {
		t.Fatalf("Should not have gotten a client back, got %v", c)
	}
}

func TestFSCompactClientFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 10
	threshold := total / 2

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	check := func(fs *FileStore, expectedClients, expectedDelRecs int) {
		fs.RLock()
		numClients := len(fs.clients)
		delRecs := fs.cliDeleteRecs
		fs.RUnlock()
		if numClients != expectedClients {
			stackFatalf(t, "Expected %v clients, got %v", expectedClients, numClients)
		}
		if delRecs != expectedDelRecs {
			stackFatalf(t, "Expected %v delete records, got %v", expectedDelRecs, delRecs)
		}
	}

	// Create clients below threshold
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete half.
	for i := 0; i < threshold-1; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	check(fs, threshold+1, threshold-1)

	// Recover
	fs.Close()
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	// Verify our numbers are same after recovery
	check(fs, threshold+1, threshold-1)

	// Delete one more, this should trigger compaction
	cid := fmt.Sprintf("cid_%d", threshold)
	fs.DeleteClient(cid)
	// One client less, 0 del records after a compaction
	check(fs, threshold, 0)

	// Make sure we don't compact too often
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Delete almost all of them
	for i := 0; i < total-1; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		fs.DeleteClient(cid)
	}
	// The number of clients should be same than before + 1,
	// and lots of delete
	check(fs, threshold+1, total-1)
	// Now wait for the interval and a bit more
	time.Sleep(1500 * time.Millisecond)
	// Delete one more, compaction should occur
	cid = fmt.Sprintf("cid_%d", 2*total)
	fs.DeleteClient(cid)
	// One less client, 0 delete records after compaction
	check(fs, threshold, 0)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: disable compaction
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: have a big min file size
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)
}

func checkSubStoreRecCounts(t *testing.T, s *FileSubStore, expectedSubs, expectedRecs, expectedDelRecs int) {
	s.RLock()
	numSubs := len(s.subs)
	numRecs := s.numRecs
	numDel := s.delRecs
	s.RUnlock()
	if numSubs != expectedSubs {
		stackFatalf(t, "Expected %v subs, got %v", expectedSubs, numSubs)
	}
	if numRecs != expectedRecs {
		stackFatalf(t, "Expected %v recs, got %v", expectedRecs, numRecs)
	}
	if numDel != expectedDelRecs {
		stackFatalf(t, "Expected %v free recs, got %v", expectedDelRecs, numDel)
	}
}

func TestFSCompactSubsFileOnDelete(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 6
	threshold := 3

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs, _, err := fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	subIDs := make([]uint64, 0, total)
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		subID := subIDs[i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Delete more to cause compaction
	ss.DeleteSub(subIDs[threshold-1])

	// Since we compact, we now have the same number of recs and subs,
	// and no delete records.
	checkSubStoreRecCounts(t, ss, total-threshold, total-threshold, 0)

	// Make sure we don't compact too often
	count := total - threshold - 1
	for i := 0; i < count; i++ {
		subID := subIDs[threshold+i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 1, total-threshold, count)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact by adding and then removing a subscription
	ss.DeleteSub(subIDs[total-1])
	// Check stats
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)
}

func TestFSCompactSubsFileOnAck(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs, _, err := fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	totalSeqs := 10
	threshold := 1 + 5 // 1 for the sub, 5 for acks

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}

	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Add 1 more ack to cause compaction
	if err := ss.AckSeqPending(sub.ID, uint64(threshold)); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Now the number of acks should be 0.
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs-threshold, 0)
	startCount := 1 + totalSeqs - threshold

	// Make sure we don't compact too often
	start := 10000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Then remove them all. Total gain/loss is 0.
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 2*totalSeqs)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact
	willCompactID := uint64(20000)
	if err := ss.AddSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding seq: %v", err)
	}
	if err := ss.AckSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Check stats
	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Add more sequences
	start = 30000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Remove the subscription, this should cause a compact
	ss.DeleteSub(sub.ID)
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)
}

func TestFSFlush(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testFlush(t, fs)

	// Now specific tests to File store
	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	msg := storeMsg(t, fs, "foo", []byte("new msg"))
	subID := storeSub(t, fs, "foo")
	storeSubPending(t, fs, "foo", subID, msg.Sequence)
	// Close the underlying file
	ms := cs.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.file.Close()
	ms.Unlock()
	// Expect Flush to fail
	if err := cs.Msgs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
	// Close the underlying file
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.Close()
	ss.Unlock()
	// Expect Flush to fail
	if err := cs.Subs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
}

func TestFSDoSync(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	total := 100
	dur := [2]time.Duration{}

	for i := 0; i < 2; i++ {
		sOpts := DefaultFileStoreOptions
		sOpts.DoSync = true
		if i == 1 {
			sOpts.DoSync = false
		}
		fs, _, err := NewFileStore(defaultDataStore, &testDefaultChannelLimits, AllOptions(&sOpts))
		if err != nil {
			stackFatalf(t, "Unable to create a FileStore instance: %v", err)
		}
		defer fs.Close()

		cs, _, err := fs.CreateChannel("foo", nil)
		if err != nil {
			t.Fatalf("Unexpected error creating channel: %v", err)
		}
		subID := storeSub(t, fs, "foo")

		msg := make([]byte, 1024)
		start := time.Now()
		// Send more message when fsync is disabled. It should still be faster,
		// and would catch if bug in code where we always do fsync, regardless
		// of option.
		for j := 0; j < total+(i*total/10); j++ {
			m := storeMsg(t, fs, "foo", msg)
			cs.Msgs.Flush()
			storeSubPending(t, fs, "foo", subID, m.Sequence)
			cs.Subs.Flush()
		}
		dur[i] = time.Now().Sub(start)

		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
	if dur[0] < dur[1] {
		t.Fatalf("Expected File sync enabled to be slower than disabled: %v vs %v", dur[0], dur[1])
	}
}

type testReader struct {
	content     []byte
	start       int
	errToReturn error
}

func (t *testReader) setContent(content []byte) {
	t.content = content
	t.start = 0
}

func (t *testReader) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testReader) Read(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	if len(t.content)-t.start < len(p) {
		return 0, io.EOF
	}
	copy(p, t.content[t.start:t.start+len(p)])
	t.start += len(p)
	return len(t.content), nil
}

func TestFSReadRecord(t *testing.T) {
	r := &testReader{}

	var err error

	buf := make([]byte, 5)
	var retBuf []byte
	recType := recNoType
	recSize := 0

	// Reader returns an error
	errReturned := fmt.Errorf("Fake error")
	r.setErrToReturn(errReturned)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != errReturned {
		t.Fatalf("Expected error %v, got: %v", errReturned, err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Record not containing CRC
	_header := [4]byte{}
	header := _header[:]
	util.ByteOrder.PutUint32(header, 0)
	r.setErrToReturn(nil)
	r.setContent(header)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Wrong CRC
	b := make([]byte, recordHeaderSize+5)
	util.ByteOrder.PutUint32(b, 5)
	copy(b[recordHeaderSize:], []byte("hello"))
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}
	// Not asking for CRC should return ok
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if string(retBuf[:recSize]) != "hello" {
		t.Fatalf("Expected body to be \"hello\", got %q", string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check that returned buffer has expanded as required
	b = make([]byte, recordHeaderSize+10)
	payload := []byte("hellohello")
	util.ByteOrder.PutUint32(b, uint32(len(payload)))
	util.ByteOrder.PutUint32(b[4:recordHeaderSize], crc32.ChecksumIEEE(payload))
	copy(b[recordHeaderSize:], payload)
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check rec type returned properly
	util.ByteOrder.PutUint32(b, 1<<24|10) // reuse previous buf
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, true, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recordType(1) {
		t.Fatalf("Expected recType to be 1, got %v", recType)
	}
}

type testWriter struct {
	buf         []byte
	errToReturn error
}

func (t *testWriter) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testWriter) reset() {
	t.buf = t.buf[:0]
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	t.buf = append(t.buf, p...)
	return len(p), nil
}

type recordProduceErrorOnMarshal struct {
	errToReturn error
}

func (r *recordProduceErrorOnMarshal) Size() int {
	return 1
}

func (r *recordProduceErrorOnMarshal) MarshalTo(b []byte) (int, error) {
	return 0, r.errToReturn
}

func TestFSWriteRecord(t *testing.T) {
	w := &testWriter{}

	var err error

	buf := make([]byte, 100)
	var retBuf []byte
	size := 0

	cli := &spb.ClientInfo{ID: "me", HbInbox: "inbox"}
	cliBuf, _ := cli.Marshal()

	retBuf, size, err = writeRecord(w, buf, addClient, cli, crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header := util.ByteOrder.Uint32(w.buf[:4])
	recType := recordType(header >> 24 & 0xFF)
	if recType != addClient {
		t.Fatalf("Expected type %v, got %v", addClient, recType)
	}
	crc := util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:len(retBuf)])
	}

	// Check with no type
	w.reset()
	retBuf, size, err = writeRecord(w, buf, recNoType, cli, crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header = util.ByteOrder.Uint32(w.buf[:4])
	recType = recordType(header >> 24 & 0xFF)
	if recType != recNoType {
		t.Fatalf("Expected type %v, got %v", recNoType, recType)
	}
	crc = util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:len(retBuf)])
	}

	// Check for marshalling error
	w.reset()
	errReturned := fmt.Errorf("Fake error")
	corruptRec := &recordProduceErrorOnMarshal{errToReturn: errReturned}
	retBuf, size, err = writeRecord(w, buf, recNoType, corruptRec, crc32.IEEETable)
	if err != errReturned {
		t.Fatalf("Expected error %v, got %v", errReturned, err)
	}
	if retBuf == nil {
		t.Fatal("Returned slice should not be nil")
	}
	if size != 0 {
		t.Fatalf("Size should be 0, got %v", size)
	}
}

func TestFSNoPartialWriteDueToBuffering(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Make this big enough so that msg payload (50%) with MsgProto overhead is
	// less than the buffer size. As this time of writting, 100 works ok.
	bufferSize := 100
	s, _, err := NewFileStore(defaultDataStore, nil, BufferSize(bufferSize))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()
	info := testDefaultServerInfo
	if err := s.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}

	// Write a message that is less than buffer size
	msgSize := (bufferSize * 5) / 10
	msg := make([]byte, msgSize)
	for i := 0; i < len(msg); i++ {
		msg[i] = byte('A' + (i % 26))
	}
	storeMsg(t, s, "foo", msg)
	// Store 2nd message
	storeMsg(t, s, "foo", msg)
	// At this point manually close and remove the reference of the msg store file
	// so that on file store close, no flush is done.
	cs := s.LookupChannel("foo")
	msgStore := cs.Msgs.(*FileMsgStore)
	if err := msgStore.file.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	msgStore.file = nil
	// Close the file store
	s.Close()
	// Reopen, there should be no error due to partials
	s, _ = openDefaultFileStore(t)
	defer s.Close()
	// And there should be only 1 message
	cs = s.LookupChannel("foo")
	msgStore = cs.Msgs.(*FileMsgStore)
	if n, _, _ := msgStore.State(); n != 1 {
		t.Fatalf("Expected 1 message, got: %v", n)
	}
}
