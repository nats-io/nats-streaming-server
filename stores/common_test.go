package stores

import (
	"testing"

	"github.com/nats-io/stan/pb"
)

var DefaultChannelLimits = ChannelLimits{
	MaxNumMsgs:  1000000,
	MaxMsgBytes: 1000000 * 1024,
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

	ms := s.LookupChannel("foo").Msgs

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

func testLimits(t *testing.T, s Store, limitCount int) {
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

type StructWithInterface struct {
	Str      string
	UserData interface{}
}

type StructWithStruct struct {
	Str      string
	UserData *OtherStruct
}

type OtherStruct struct {
	Int int64
}

func BenchmarkInterface(b *testing.B) {
	s := &StructWithInterface{Str: "hello"}
	u := &OtherStruct{Int: int64(19)}
	s.UserData = u

	total := int64(0)

	for i := 0; i < b.N; i++ {
		t := s.UserData.(*OtherStruct)
		total += t.Int
	}
}

func BenchmarkDirectAccess(b *testing.B) {
	s := &StructWithStruct{Str: "hello"}
	u := &OtherStruct{Int: int64(19)}
	s.UserData = u

	total := int64(0)

	for i := 0; i < b.N; i++ {
		t := s.UserData
		total += t.Int
	}
}
