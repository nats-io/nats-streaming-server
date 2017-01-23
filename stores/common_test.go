// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nuid"
)

var testDefaultStoreLimits = StoreLimits{
	100,
	ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  1000000,
			MaxBytes: 1000000 * 1024,
		},
		SubStoreLimits{
			MaxSubscriptions: 1000,
		},
	},
	nil,
}

var nuidGen *nuid.NUID

func init() {
	nuidGen = nuid.New()
}

type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func storeMsg(t *testing.T, s Store, channel string, data []byte) *pb.MsgProto {
	cs := s.LookupChannel(channel)
	if cs == nil {
		var err error
		cs, _, err = s.CreateChannel(channel, nil)
		if err != nil {
			stackFatalf(t, "Error creating channel [%v]: %v", channel, err)
		}
	}
	ms := cs.Msgs
	seq, err := ms.Store(data)
	if err != nil {
		stackFatalf(t, "Error storing message into channel [%v]: %v", channel, err)
	}
	return ms.Lookup(seq)
}

func storeSub(t *testing.T, s Store, channel string) uint64 {
	cs := s.LookupChannel(channel)
	if cs == nil {
		var err error
		cs, _, err = s.CreateChannel(channel, nil)
		if err != nil {
			stackFatalf(t, "Error creating channel [%v]: %v", channel, err)
		}
	}
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub); err != nil {
		stackFatalf(t, "Error storing subscription into channel [%v]: %v", channel, err)
	}
	return sub.ID
}

func storeSubPending(t *testing.T, s Store, channel string, subID uint64, seqs ...uint64) {
	cs := s.LookupChannel(channel)
	if cs == nil {
		t.Fatalf("Channel [%v] not found", channel)
	}
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AddSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubAck(t *testing.T, s Store, channel string, subID uint64, seqs ...uint64) {
	cs := s.LookupChannel(channel)
	if cs == nil {
		t.Fatalf("Channel [%v] not found", channel)
	}
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AckSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubDelete(t *testing.T, s Store, channel string, subID ...uint64) {
	cs := s.LookupChannel(channel)
	if cs == nil {
		t.Fatalf("Channel [%v] not found", channel)
	}
	ss := cs.Subs
	for _, s := range subID {
		ss.DeleteSub(s)
	}
}

func testBasicCreate(t *testing.T, s Store, expectedName string) {
	if s.Name() != expectedName {
		t.Fatalf("Expecting name to be %q, got %q", expectedName, s.Name())
	}
}

func testNothingRecoveredOnFreshStart(t *testing.T, s Store) {
	if s.HasChannel() {
		t.Fatal("Nothing should have been recovered!")
	}
}

func testNewChannel(t *testing.T, s Store) {
	myUserData := "test"
	cs, _, err := s.CreateChannel("foo", myUserData)
	if err != nil {
		t.Fatalf("Unexpected error creating new channel: %v", err)
	}
	if !s.HasChannel() {
		t.Fatal("HasChannel should return true")
	}
	if cs.Subs == nil {
		t.Fatal("SubStore should not be nil")
	}
	if cs.Msgs == nil {
		t.Fatal("MsgStore should not be nil")
	}
	// Lookup the channel and make sure UserData is properly set
	cs = s.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel should exist")
	}
	if cs.UserData != myUserData {
		t.Fatalf("UserData not properly set, got %v", cs.UserData)
	}
	// Creating the same channel should fail
	ncs, isNew, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isNew {
		t.Fatal("isNew should be false")
	}
	if cs != ncs {
		t.Fatalf("Channel should exist: %v", ncs)
	}
}

func testCloseIdempotent(t *testing.T, s Store) {
	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating new channel: %v", err)
	}

	ms := cs.Msgs
	if err := ms.Close(); err != nil {
		t.Fatalf("Error closing store: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("Close should be idempotent: %v", err)
	}

	ss := cs.Subs
	if err := ss.Close(); err != nil {
		t.Fatalf("Error closing store: %v", err)
	}
	if err := ss.Close(); err != nil {
		t.Fatalf("Close should be idempotent: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Error closing store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close should be idempotent: %v", err)
	}
}

func testBasicMsgStore(t *testing.T, s Store) {
	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Failed to create channel foo: %v", err)
	}
	ms := cs.Msgs

	// No message is stored, verify expected values.
	if ms.FirstMsg() != nil {
		t.Fatalf("Unexpected first message: %v vs %v", ms.FirstMsg(), nil)
	}

	if ms.LastMsg() != nil {
		t.Fatalf("Unexpected first message: %v vs %v", ms.LastMsg(), nil)
	}

	if ms.FirstSequence() != 0 {
		t.Fatalf("Unexpected first sequence: %v vs %v", ms.FirstSequence(), 0)
	}

	if ms.LastSequence() != 0 {
		t.Fatalf("Unexpected first sequence: %v vs %v", ms.FirstSequence(), 0)
	}

	if s1, s2 := ms.FirstAndLastSequence(); s1 != 0 || s2 != 0 {
		t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
	}

	payload1 := []byte("m1")
	m1 := storeMsg(t, s, "foo", payload1)

	payload2 := []byte("m2")
	m2 := storeMsg(t, s, "foo", payload2)

	if string(payload1) != string(m1.Data) {
		t.Fatalf("Unexpected payload: %v", string(m1.Data))
	}
	if string(payload2) != string(m2.Data) {
		t.Fatalf("Unexpected payload: %v", string(m1.Data))
	}

	if !reflect.DeepEqual(ms.FirstMsg(), m1) {
		t.Fatalf("Unexpected first message: %v vs %v", ms.FirstMsg(), m1)
	}

	if !reflect.DeepEqual(ms.LastMsg(), m2) {
		t.Fatalf("Unexpected last message: %v vs %v", ms.LastMsg(), m2)
	}

	if ms.FirstSequence() != m1.Sequence {
		t.Fatalf("Unexpected first sequence: %v vs %v", ms.FirstSequence(), m1.Sequence)
	}

	if ms.LastSequence() != m2.Sequence {
		t.Fatalf("Unexpected first sequence: %v vs %v", ms.FirstSequence(), m2.Sequence)
	}

	if s1, s2 := ms.FirstAndLastSequence(); s1 != m1.Sequence || s2 != m2.Sequence {
		t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
	}

	lm1 := ms.Lookup(m1.Sequence)
	if !reflect.DeepEqual(lm1, m1) {
		t.Fatalf("Unexpected lookup result: %v instead of %v", lm1, m1)
	}

	lm2 := ms.Lookup(m2.Sequence)
	if !reflect.DeepEqual(lm2, m2) {
		t.Fatalf("Unexpected lookup result: %v instead of %v", lm2, m2)
	}

	count, bytes, err := ms.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	expectedBytes := uint64(m1.Size() + m2.Size())
	if _, ok := s.(*FileStore); ok {
		// FileStore counts more toward the number of bytes
		expectedBytes += 2 * (msgRecordOverhead)
	}
	if count != 2 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: %v, %v vs %v, %v", count, bytes, 2, expectedBytes)
	}

	// Store one more mesasge to check that LastMsg is correctly updated
	m3 := storeMsg(t, s, "foo", []byte("last"))
	if !reflect.DeepEqual(ms.LastMsg(), m3) {
		t.Fatalf("Expected last message to be %v, got %v", m3, ms.LastMsg())
	}
}

func testMsgsState(t *testing.T, s Store) {
	payload := []byte("hello")

	m1 := storeMsg(t, s, "foo", payload)
	m2 := storeMsg(t, s, "bar", payload)

	_, isFileStore := s.(*FileStore)

	count, bytes, err := s.MsgsState("foo")
	expectedBytes := uint64(m1.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}
	if count != 1 || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, expectedBytes, err)
	}

	count, bytes, err = s.MsgsState("bar")
	expectedBytes = uint64(m2.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}
	if count != 1 || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, expectedBytes, err)
	}

	count, bytes, err = s.MsgsState(AllChannels)
	expectedBytes = uint64(m1.Size() + m2.Size())
	if isFileStore {
		expectedBytes += 2 * (msgRecordOverhead)
	}
	if count != 2 || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, expectedBytes, err)
	}
}

func testMaxMsgs(t *testing.T, s Store) {
	payload := []byte("hello")

	_, isFileStore := s.(*FileStore)

	limitCount := 0
	stopBytes := uint64(500)
	expectedBytes := uint64(0)
	for i := 0; ; i++ {
		seq := uint64(i + 1)
		m := pb.MsgProto{Data: payload, Subject: "foo", Sequence: seq, Timestamp: time.Now().UnixNano()}
		expectedBytes += uint64(m.Size())
		if isFileStore {
			expectedBytes += msgRecordOverhead
		}
		limitCount++
		if expectedBytes >= stopBytes {
			break
		}
	}

	limits := testDefaultStoreLimits
	limits.MaxMsgs = limitCount
	limits.MaxBytes = int64(expectedBytes)
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	totalSent := limitCount + 60
	firstSeqAfterLimitReached := uint64(totalSent - limitCount + 1)

	for i := 0; i < totalSent; i++ {
		storeMsg(t, s, "foo", payload)
	}

	count, bytes, err := s.MsgsState("foo")
	if count != limitCount || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, limitCount, bytes, expectedBytes, err)
	}

	cs := s.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel fpp should exist")
	}

	// Check that older messages are no longer avail.
	if cs.Msgs.Lookup(1) != nil || cs.Msgs.Lookup(uint64(firstSeqAfterLimitReached-1)) != nil {
		t.Fatal("Older messages still available")
	}

	firstMsg := cs.Msgs.FirstMsg()
	firstSeq := cs.Msgs.FirstSequence()
	lastMsg := cs.Msgs.LastMsg()
	lastSeq := cs.Msgs.LastSequence()

	if firstMsg == nil || firstMsg.Sequence != firstSeq || firstSeq != firstSeqAfterLimitReached {
		t.Fatalf("Incorrect first message: msg=%v seq=%v", firstMsg, firstSeq)
	}
	if lastMsg == nil || lastMsg.Sequence != lastSeq || lastSeq != uint64(totalSent) {
		t.Fatalf("Incorrect last message: msg=%v seq=%v", firstMsg, firstSeq)
	}

	// Store a message with a payload larger than the limit.
	// Make sure that the message is stored, but all others should
	// be removed.
	bigMsg := make([]byte, limits.MaxBytes+100)
	m := storeMsg(t, s, "foo", bigMsg)
	expectedBytes = uint64(m.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}

	count, bytes, err = s.MsgsState("foo")
	if count != 1 || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, expectedBytes, err)
	}

	// Test that we check only on non-zero limits
	expectedCount := 10
	expectedBytes = uint64(0)
	channelName := "maxcount"
	for i := 0; i < expectedCount; i++ {
		seq := uint64(i + 1)
		m := pb.MsgProto{Data: payload, Subject: channelName, Sequence: seq, Timestamp: time.Now().UnixNano()}
		expectedBytes += uint64(m.Size())
		if isFileStore {
			expectedBytes += msgRecordOverhead
		}
	}
	limits.MaxMsgs = expectedCount
	limits.MaxBytes = 0
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	for i := 0; i < expectedCount+10; i++ {
		storeMsg(t, s, channelName, payload)
	}
	n, b, err := s.MsgsState(channelName)
	if err != nil {
		t.Fatalf("Unexpected error on MsgsState: %v", err)
	}
	if n != expectedCount {
		t.Fatalf("Expected %v messages, got %v", expectedCount, n)
	}
	if b != expectedBytes {
		t.Fatalf("Expected %v bytes, got %v", expectedBytes, b)
	}

	expectedCount = 0
	expectedBytes = uint64(0)
	channelName = "maxbytes"
	for i := 0; ; i++ {
		seq := uint64(i + 1)
		m := pb.MsgProto{Data: payload, Subject: channelName, Sequence: seq, Timestamp: time.Now().UnixNano()}
		expectedBytes += uint64(m.Size())
		if isFileStore {
			expectedBytes += msgRecordOverhead
		}
		expectedCount++
		if expectedBytes >= 1000 {
			break
		}
	}
	limits.MaxMsgs = 0
	limits.MaxBytes = int64(expectedBytes)
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	for i := 0; i < expectedCount+10; i++ {
		storeMsg(t, s, channelName, payload)
	}
	n, b, err = s.MsgsState(channelName)
	if err != nil {
		t.Fatalf("Unexpected error on MsgsState: %v", err)
	}
	if n != expectedCount {
		t.Fatalf("Expected %d messages, got %v", expectedCount, n)
	}
	if b != expectedBytes {
		t.Fatalf("Expected %v bytes, got %v", expectedBytes, b)
	}
}

func testMaxChannels(t *testing.T, s Store, maxChannels int) {
	total := maxChannels + 1
	if maxChannels == 0 {
		total = 10
	}
	var err error
	numCh := 0
	for i := 0; i < total; i++ {
		_, _, err = s.CreateChannel(fmt.Sprintf("foo.%d", i), nil)
		if err != nil {
			break
		}
		numCh++
	}
	if maxChannels == 0 && err != nil {
		t.Fatalf("Should not have failed, got %v", err)
	} else if maxChannels > 0 {
		if err == nil || err != ErrTooManyChannels {
			t.Fatalf("Error should have been ErrTooManyChannels, got %v", err)
		}
		if numCh != maxChannels {
			t.Fatalf("Wrong number of channels: %v vs %v", numCh, maxChannels)
		}
	}
}

func testMaxSubs(t *testing.T, s Store, channel string, maxSubs int) {
	total := maxSubs + 1
	if maxSubs == 0 {
		total = 10
	}
	cs, _, err := s.CreateChannel(channel, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	sub := &spb.SubState{}
	numSubs := 0
	for i := 0; i < total; i++ {
		err = cs.Subs.CreateSub(sub)
		if err != nil {
			break
		}
		numSubs++
	}
	if maxSubs == 0 && err != nil {
		t.Fatalf("Should not have failed, got %v", err)
	} else if maxSubs > 0 {
		if err == nil || err != ErrTooManySubs {
			t.Fatalf("Error should have been ErrTooManySubs, got %v", err)
		}
		if numSubs != maxSubs {
			t.Fatalf("Wrong number of subs: %v vs %v", numSubs, maxSubs)
		}
	}
}

func testMaxAge(t *testing.T, s Store) {
	sl := testDefaultStoreLimits
	sl.MaxAge = 250 * time.Millisecond
	s.SetLimits(&sl)

	msg := []byte("hello")
	for i := 0; i < 10; i++ {
		storeMsg(t, s, "foo", msg)
	}
	// Wait a bit
	time.Sleep(200 * time.Millisecond)
	// Send more
	for i := 0; i < 5; i++ {
		storeMsg(t, s, "foo", msg)
	}
	// Wait a bit
	time.Sleep(100 * time.Millisecond)
	// We should have the first 10 expired and 5 left.
	cs := s.LookupChannel("foo")
	expectedFirst := uint64(11)
	expectedLast := uint64(15)
	first, last := cs.Msgs.FirstAndLastSequence()
	if first != expectedFirst || last != expectedLast {
		t.Fatalf("Expected first/last to be %v/%v, got %v/%v",
			expectedFirst, expectedLast, first, last)
	}
	// Wait more and all should be gone.
	time.Sleep(sl.MaxAge)
	if n, _, _ := cs.Msgs.State(); n != 0 {
		t.Fatalf("All messages should have expired, got %v", n)
	}
}

func testBasicSubStore(t *testing.T, s Store) {
	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ss := cs.Subs
	sub := &spb.SubState{}
	sub.AckInbox = "AckInbox"

	err = ss.CreateSub(sub)
	if err != nil {
		t.Fatalf("Unexpected error on create sub: %v", err)
	}
	if sub.ID == 0 {
		t.Fatalf("Expected a positive subID, got: %v", sub.ID)
	}
	if err := ss.AddSeqPending(sub.ID, 1); err != nil {
		t.Fatalf("Unexpected error on AddSeqPending: %v", err)
	}
	if err := ss.AckSeqPending(sub.ID, 1); err != nil {
		t.Fatalf("Unexpected error on AckSeqPending: %v", err)
	}
	// Update the subscription
	sub.AckInbox = "newAckInbox"
	if err := ss.UpdateSub(sub); err != nil {
		t.Fatalf("Unexpected error on update sub: %v", err)
	}
	ss.DeleteSub(sub.ID)

	// Chekck that there is no error if we add updates for deleted sub.
	if err := ss.AddSeqPending(sub.ID, 2); err != nil {
		t.Fatalf("Unexpected error on AddSeqPending: %v", err)
	}
	// Check that ack update for non existent sub is OK
	if err := ss.AckSeqPending(sub.ID+1, 10); err != nil {
		t.Fatalf("Unexpected error on AddSeqPending: %v", err)
	}
}

func testGetSeqFromStartTime(t *testing.T, s Store) {
	// Force creation of channel without storing anything yet
	s.CreateChannel("foo", nil)
	// Lookup channel store
	cs := s.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	// Check before storing anything
	seq := cs.Msgs.GetSequenceFromTimestamp(time.Now().UnixNano())
	if seq != 0 {
		t.Fatalf("Invalid start sequence. Expected %v got %v", 0, seq)
	}

	count := 100
	msgs := make([]*pb.MsgProto, 0, count)
	payload := []byte("hello")
	for i := 0; i < count; i++ {
		m := storeMsg(t, s, "foo", payload)
		msgs = append(msgs, m)
		time.Sleep(1 * time.Millisecond)
	}

	startMsg := msgs[count/2]
	seq = cs.Msgs.GetSequenceFromTimestamp(startMsg.Timestamp)
	if seq != startMsg.Sequence {
		t.Fatalf("Invalid start sequence. Expected %v got %v", startMsg.Sequence, seq)
	}
	seq = cs.Msgs.GetSequenceFromTimestamp(msgs[0].Timestamp - int64(time.Second))
	if seq != msgs[0].Sequence {
		t.Fatalf("Expected seq to be %v, got %v", msgs[0].Sequence, seq)
	}
	seq = cs.Msgs.GetSequenceFromTimestamp(msgs[count-1].Timestamp + int64(time.Second))
	if seq != msgs[count-1].Sequence+1 {
		t.Fatalf("Expected seq to be %v, got %v", msgs[count-1].Sequence+1, seq)
	}
}

func testClientAPIs(t *testing.T, s Store) {
	// Delete client that does not exist
	s.DeleteClient("client1")

	// Delete a client before adding it
	s.DeleteClient("client2")

	// Adding it after the delete
	sc, _, err := s.AddClient("client2", "hbInbox", nil)
	if err != nil {
		t.Fatalf("Unexpected error adding client: %v", err)
	}
	// Adding it another time should return the first and isNew false
	if sc2, isNew, err := s.AddClient("client2", "hbInbox", nil); err != nil {
		t.Fatalf("Unexpected error on add client: %v", err)
	} else if isNew {
		t.Fatal("isNew should be false")
	} else if sc2 != sc {
		t.Fatalf("Old client should be %v, got %v", sc, sc2)
	}

	// Add a client
	userData := "test"
	sc3, _, err := s.AddClient("client3", "hbInbox", userData)
	if err != nil {
		t.Fatalf("Unexpected error adding client: %v", err)
	}

	// Add a client then..
	sc, _, err = s.AddClient("client4", "hbInbox", nil)
	if err != nil {
		t.Fatalf("Unexpected error adding client: %v", err)
	}
	// Delete it.
	if dsc := s.DeleteClient("client4"); dsc != sc {
		t.Fatalf("Expected delete to return %v, got %v", sc, dsc)
	}

	// Try to retrieve client3
	if gc := s.GetClient("client3"); gc != sc3 {
		t.Fatalf("Expected %v, got %v", sc3, gc)
	}
	if count := s.GetClientsCount(); count != 2 {
		t.Fatalf("Expected 2 clients, got %v", count)
	}
	clients := s.GetClients()
	if len(clients) != 2 {
		t.Fatalf("Expected 2 client, got %v", len(clients))
	}
	for cID, sc := range clients {
		if cID != "client2" && cID != "client3" {
			t.Fatalf("Unexpected CID: %v", cID)
		}
		if sc.HbInbox != "hbInbox" {
			t.Fatalf("Invalid hbInbox: %v", sc.HbInbox)
		}
		if cID == "client3" && sc.UserData != userData {
			t.Fatalf("Expected user data to be %v, got %v", userData, sc.UserData)
		}
	}
}

func testFlush(t *testing.T, s Store) {
	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	seq, err := cs.Msgs.Store([]byte("hello"))
	if err != nil {
		t.Fatalf("Unexpected error on store: %v", err)
	}
	if err := cs.Msgs.Flush(); err != nil {
		t.Fatalf("Unexpected error on flush: %v", err)
	}
	sub := spb.SubState{}
	if err := cs.Subs.CreateSub(&sub); err != nil {
		t.Fatalf("Unexpected error creating sub: %v", err)
	}
	if err := cs.Subs.AddSeqPending(sub.ID, seq); err != nil {
		t.Fatalf("Unexpected error adding sequence to substore: %v", err)
	}
	if err := cs.Subs.Flush(); err != nil {
		t.Fatalf("Unexpected error on flush: %v", err)
	}
}

func TestGSNoOps(t *testing.T) {
	gs := &genericStore{}
	defer gs.Close()
	limits := DefaultStoreLimits
	gs.init("test generic", &limits)
	if _, _, err := gs.CreateChannel("foo", nil); err == nil {
		t.Fatal("Expected to get an error since this should not be implemented for generic store")
	}
	if err := gs.Close(); err != nil {
		t.Fatalf("Expected nil, got %v", err)
	}

	gms := &genericMsgStore{}
	defer gms.Close()
	gms.init("foo", &limits.MsgStoreLimits)
	if gms.Lookup(1) != nil || gms.FirstMsg() != nil || gms.LastMsg() != nil || gms.Flush() != nil ||
		gms.GetSequenceFromTimestamp(0) != 0 || gms.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}

	gss := &genericSubStore{}
	defer gss.Close()
	gss.init("foo", &limits.SubStoreLimits)
	if gss.AddSeqPending(1, 1) != nil || gss.AckSeqPending(1, 1) != nil || gss.Flush() != nil ||
		gss.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}
}

func testPerChannelLimits(t *testing.T, s Store) {
	storeLimits := &StoreLimits{MaxChannels: 10}
	storeLimits.MaxSubscriptions = 10
	storeLimits.MaxMsgs = 100
	storeLimits.MaxBytes = 100 * 1024

	fooLimits := ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  3,
			MaxBytes: 3 * 1024,
		},
		SubStoreLimits{
			MaxSubscriptions: 1,
		},
	}
	barLimits := ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  5,
			MaxBytes: 5 * 1024,
		},
		SubStoreLimits{
			MaxSubscriptions: 2,
		},
	}
	noSubsOverrideLimits := ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  6,
			MaxBytes: 6 * 1024,
		},
		SubStoreLimits{},
	}
	noMaxMsgOverrideLimits := ChannelLimits{
		MsgStoreLimits{
			MaxBytes: 7 * 1024,
		},
		SubStoreLimits{},
	}
	noMaxBytesOverrideLimits := ChannelLimits{
		MsgStoreLimits{
			MaxMsgs: 10,
		},
		SubStoreLimits{},
	}

	storeLimits.AddPerChannel("foo", &fooLimits)
	storeLimits.AddPerChannel("bar", &barLimits)
	storeLimits.AddPerChannel("baz", &noSubsOverrideLimits)
	storeLimits.AddPerChannel("abc", &noMaxMsgOverrideLimits)
	storeLimits.AddPerChannel("def", &noMaxBytesOverrideLimits)
	if err := s.SetLimits(storeLimits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	checkLimitsForChannel := func(channelName string, maxMsgs, maxSubs int) {
		cs, _, err := s.CreateChannel(channelName, nil)
		if err != nil {
			stackFatalf(t, "Unexpected error on create channel: %v", err)
		}
		for i := 0; i < maxMsgs+10; i++ {
			if _, err := cs.Msgs.Store([]byte("hello")); err != nil {
				stackFatalf(t, "Unexpected error on store: %v", err)
			}
		}
		n, _, err := cs.Msgs.State()
		if err != nil {
			stackFatalf(t, "Unexpected error on State: %v", err)
		}
		if n != maxMsgs {
			stackFatalf(t, "Expected %v messages, got %v", maxMsgs, n)
		}
		for i := 0; i < maxSubs+1; i++ {
			err := cs.Subs.CreateSub(&spb.SubState{})
			if i < maxSubs && err != nil {
				stackFatalf(t, "Unexpected error on create sub: %v", err)
			} else if i == maxSubs && err == nil {
				stackFatalf(t, "Expected error on createSub, did not get one")
			}
		}
	}
	checkLimitsForChannel("foo", fooLimits.MaxMsgs, fooLimits.MaxSubscriptions)
	checkLimitsForChannel("bar", barLimits.MaxMsgs, barLimits.MaxSubscriptions)
	checkLimitsForChannel("baz", noSubsOverrideLimits.MaxMsgs, storeLimits.MaxSubscriptions)
	checkLimitsForChannel("abc", storeLimits.MaxMsgs, storeLimits.MaxSubscriptions)
	checkLimitsForChannel("def", noMaxBytesOverrideLimits.MaxMsgs, storeLimits.MaxSubscriptions)
	checkLimitsForChannel("global", storeLimits.MaxMsgs, storeLimits.MaxSubscriptions)
}

func testIncrementalTimestamp(t *testing.T, s Store) {
	limits := DefaultStoreLimits
	limits.MaxMsgs = 2
	s.SetLimits(&limits)

	cs, _, _ := s.CreateChannel("foo", nil)
	ms := cs.Msgs

	msg := []byte("msg")

	total := 8000000
	for i := 0; i < total; i++ {
		seq1, err1 := ms.Store(msg)
		seq2, err2 := ms.Store(msg)
		if err1 != nil || err2 != nil {
			t.Fatalf("Unexpected error on store: %v %v", err1, err2)
		}
		m1 := ms.Lookup(seq1)
		m2 := ms.Lookup(seq2)
		if m2.Timestamp < m1.Timestamp {
			t.Fatalf("Timestamp of msg %v is smaller than previous one. Diff is %vms",
				m2.Sequence, m1.Timestamp-m2.Timestamp)
		}
	}
}
