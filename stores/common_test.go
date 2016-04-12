// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nuid"
	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan/pb"
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

func storeMsg(t *testing.T, s Store, channel string, data []byte) *pb.MsgProto {
	cs, _, err := s.LookupOrCreateChannel(channel)
	if err != nil {
		t.Fatalf("Error looking up channel [%v]: %v", channel, err)
	}
	ms := cs.Msgs
	m, err := ms.Store("", data)
	if err != nil {
		t.Fatalf("Error storing message into channel [%v]: %v", channel, err)
	}
	return m
}

func storeSub(t *testing.T, s Store, channel string) uint64 {
	cs, _, err := s.LookupOrCreateChannel(channel)
	if err != nil {
		t.Fatalf("Error looking up channel [%v]: %v", channel, err)
	}
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	err = ss.CreateSub(sub)
	if err != nil {
		t.Fatalf("Error storing subscription into channel [%v]: %v", channel, err)
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
	cs, isNew, err := s.LookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Unexpected error creating new channel: %v", err)
	}
	if !isNew {
		t.Fatal("isNew should be true")
	}
	if !s.HasChannel() {
		t.Fatal("HasChannel should return true")
	}
	if cs.Subs == nil {
		t.Fatalf("SubStore should not be nil")
	}
	if cs.Msgs == nil {
		t.Fatalf("MsgStore should not be nil")
	}
}

func testCloseIdempotent(t *testing.T, s Store) {
	cs, _, err := s.LookupOrCreateChannel("foo")
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
	cs, isNew, err := s.LookupOrCreateChannel("foo")
	if err != nil || !isNew {
		t.Fatalf("Failed to create channel foo: %v (isNew=%v)", err, isNew)
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

	if lm1 := ms.Lookup(m1.Sequence); lm1 != m1 {
		t.Fatalf("Unexpected lookup result: %v instead of %v", lm1, m1)
	}

	if lm2 := ms.Lookup(m2.Sequence); lm2 != m2 {
		t.Fatalf("Unexpected lookup result: %v instead of %v", lm2, m2)
	}

	count, bytes, err := ms.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	expectedBytes := uint64(len(payload1) + len(payload2))
	if count != 2 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: %v, %v vs %v, %v", count, bytes, 2, expectedBytes)
	}
}

func testMsgsState(t *testing.T, s Store) {
	payload := []byte("hello")
	lenPayload := uint64(len(payload))

	storeMsg(t, s, "foo", payload)
	storeMsg(t, s, "bar", payload)

	count, bytes, err := s.MsgsState("foo")
	if count != 1 || bytes != lenPayload || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, lenPayload, err)
	}

	count, bytes, err = s.MsgsState("bar")
	if count != 1 || bytes != lenPayload || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, lenPayload, err)
	}

	count, bytes, err = s.MsgsState(AllChannels)
	if count != 2 || bytes != 2*lenPayload || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, 1, bytes, 2*lenPayload, err)
	}
}

func testMaxMsgs(t *testing.T, s Store, limitCount int) {
	payload := []byte("hello")
	for i := 0; i < 2*limitCount; i++ {
		storeMsg(t, s, "foo", payload)
	}

	expectedBytes := uint64(limitCount * len(payload))

	count, bytes, err := s.MsgsState("foo")
	if count != limitCount || bytes != expectedBytes || err != nil {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v err=%v vs nil", count, limitCount, bytes, expectedBytes, err)
	}

	cs := s.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel fpp should exist")
	}

	// Check that older messages are no longer avail.
	if cs.Msgs.Lookup(1) != nil || cs.Msgs.Lookup(uint64(limitCount/2)) != nil {
		t.Fatal("Older messages still available")
	}

	firstMsg := cs.Msgs.FirstMsg()
	firstSeq := cs.Msgs.FirstSequence()
	lastMsg := cs.Msgs.LastMsg()
	lastSeq := cs.Msgs.LastSequence()

	if firstMsg == nil || firstMsg.Sequence != firstSeq || firstSeq == 1 {
		t.Fatalf("Incorrect first message: msg=%v seq=%v", firstMsg, firstSeq)
	}
	if lastMsg == nil || lastMsg.Sequence != lastSeq || lastSeq != uint64(2*limitCount) {
		t.Fatalf("Incorrect first message: msg=%v seq=%v", firstMsg, firstSeq)
	}
}

func testMaxChannels(t *testing.T, s Store, maxChannels int) {
	var err error
	numCh := 0
	for i := 0; i < maxChannels+1; i++ {
		_, _, err = s.LookupOrCreateChannel(fmt.Sprintf("foo.%d", i))
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
	cs, _, err := s.LookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}

	err = nil
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
	cs, isNew, err := s.LookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !isNew {
		t.Fatal("Channel should be new")
	}

	ss := cs.Subs
	sub := &spb.SubState{}

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
