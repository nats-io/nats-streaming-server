// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nuid"
	"reflect"
	"runtime"
	"strings"
)

var testDefaultChannelLimits = ChannelLimits{
	MaxChannels: 100,
	MaxNumMsgs:  1000000,
	MaxMsgBytes: 1000000 * 1024,
	MaxSubs:     1000,
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
		if ok == false {
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
	m, err := ms.Store("", data)
	if err != nil {
		stackFatalf(t, "Error storing message into channel [%v]: %v", channel, err)
	}
	return m
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

	if ms.FirstMsg() != m1 {
		t.Fatalf("Unexpected first message: %v vs %v", ms.FirstMsg(), m1)
	}

	if ms.LastMsg() != m2 {
		t.Fatalf("Unexpected first message: %v vs %v", ms.LastMsg(), m2)
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
	if count != 2 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: %v, %v vs %v, %v", count, bytes, 2, expectedBytes)
	}
}

func testMsgsState(t *testing.T, s Store) {
	payload := []byte("hello")

	m1 := storeMsg(t, s, "foo", payload)
	m2 := storeMsg(t, s, "bar", payload)

	count, bytes, err := s.MsgsState("foo")
	if count != 1 || bytes != uint64(m1.Size()) || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, m1.Size(), err)
	}

	count, bytes, err = s.MsgsState("bar")
	if count != 1 || bytes != uint64(m2.Size()) || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, m2.Size(), err)
	}

	count, bytes, err = s.MsgsState(AllChannels)
	if count != 2 || bytes != uint64(m1.Size()+m2.Size()) || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, m1.Size()+m2.Size(), err)
	}
}

func testMaxMsgs(t *testing.T, s Store) {
	payload := []byte("hello")

	limitCount := 0
	stopBytes := uint64(500)
	expectedBytes := uint64(0)
	for i := 0; ; i++ {
		seq := uint64(i + 1)
		m := pb.MsgProto{Data: payload, Subject: "foo", Sequence: seq, Timestamp: time.Now().UnixNano()}
		expectedBytes += uint64(m.Size())
		limitCount++
		if expectedBytes >= stopBytes {
			break
		}
	}

	limits := testDefaultChannelLimits
	limits.MaxNumMsgs = limitCount
	limits.MaxMsgBytes = uint64(expectedBytes)

	s.SetChannelLimits(limits)

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
	bigMsg := make([]byte, limits.MaxMsgBytes+100)
	m := storeMsg(t, s, "foo", bigMsg)
	expectedBytes = uint64(m.Size())

	count, bytes, err = s.MsgsState("foo")
	if count != 1 || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, expectedBytes, err)
	}
}

func testMaxChannels(t *testing.T, s Store, maxChannels int) {
	var err error
	numCh := 0
	for i := 0; i < maxChannels+1; i++ {
		_, _, err = s.CreateChannel(fmt.Sprintf("foo.%d", i), nil)
		if err != nil {
			break
		}
		numCh++
	}
	if err == nil || err != ErrTooManyChannels {
		t.Fatalf("Error should have been ErrTooManyChannels, got %v", err)
	}
	if numCh != maxChannels {
		t.Fatalf("Wrong number of channels: %v vs %v", numCh, maxChannels)
	}
}

func testMaxSubs(t *testing.T, s Store, maxSubs int) {
	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	sub := &spb.SubState{}
	numSubs := 0
	for i := 0; i < maxSubs+1; i++ {
		err = cs.Subs.CreateSub(sub)
		if err != nil {
			break
		}
		numSubs++
	}
	if err == nil || err != ErrTooManySubs {
		t.Fatalf("Error should have been ErrTooManySubs, got %v", err)
	}
	if numSubs != maxSubs {
		t.Fatalf("Wrong number of subs: %v vs %v", numSubs, maxSubs)
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
	count := 100
	msgs := make([]*pb.MsgProto, 0, count)
	payload := []byte("hello")
	for i := 0; i < count; i++ {
		m := storeMsg(t, s, "foo", payload)
		msgs = append(msgs, m)
		time.Sleep(1 * time.Millisecond)
	}

	cs := s.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	startMsg := msgs[count/2]
	seq := cs.Msgs.GetSequenceFromTimestamp(startMsg.Timestamp)
	if seq != startMsg.Sequence {
		t.Fatalf("Invalid start sequence. Expected %v got %v", startMsg.Sequence, seq)
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
	msg, err := cs.Msgs.Store("", []byte("hello"))
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
	if err := cs.Subs.AddSeqPending(sub.ID, msg.Sequence); err != nil {
		t.Fatalf("Unexpected error adding sequence to substore: %v", err)
	}
	if err := cs.Subs.Flush(); err != nil {
		t.Fatalf("Unexpected error on flush: %v", err)
	}
}
