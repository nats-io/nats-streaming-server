// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
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

var testDefaultServerInfo = spb.ServerInfo{
	ClusterID:   "id",
	Discovery:   "discovery",
	Publish:     "publish",
	Subscribe:   "subscribe",
	Unsubscribe: "unsubscribe",
	Close:       "close",
}

var (
	nuidGen    *nuid.NUID
	testLogger logger.Logger
	storeType  = TypeMemory
)

func init() {
	nuidGen = nuid.New()
	// Create an empty logger (no actual logger is set without calling SetLogger())
	testLogger = logger.NewStanLogger()
}

func startTest(t *testing.T) Store {
	switch storeType {
	case TypeMemory:
		return createDefaultMemStore(t)
	case TypeFile:
		cleanupDatastore(t)
		return createDefaultFileStore(t)
	}
	return nil
}

func endTest(t *testing.T) {
	if storeType == TypeFile {
		cleanupDatastore(t)
	}
}

func TestMain(m *testing.M) {
	var st string
	flag.StringVar(&st, "store", "mem", "Store type (mem,file)")
	flag.BoolVar(&disableBufferWriters, "no_buffer", false, "Disable use of buffer writers")
	flag.BoolVar(&setFDsLimit, "set_fds_limit", false, "Set some FDs limit")
	flag.Parse()

	st = strings.ToLower(st)
	// Will add DB store and others when avail
	switch st {
	case "":
		// use default
	case "mem":
		storeType = TypeMemory
	case "file":
		storeType = TypeFile
	default:
		fmt.Printf("Unknown store %q for store tests\n", st)
		os.Exit(2)
	}
	os.Exit(m.Run())
}

// Used by both testing.B and testing.T so need to use
// a common interface: tLogger
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

func msgStoreLookup(t tLogger, ms MsgStore, seq uint64) *pb.MsgProto {
	m, err := ms.Lookup(seq)
	if err != nil {
		stackFatalf(t, "Error looking up message %v: %v", seq, err)
	}
	return m
}

func msgStoreFirstSequence(t tLogger, ms MsgStore) uint64 {
	f, err := ms.FirstSequence()
	if err != nil {
		stackFatalf(t, "Error getting first sequence: %v", err)
	}
	return f
}

func msgStoreLastSequence(t tLogger, ms MsgStore) uint64 {
	l, err := ms.LastSequence()
	if err != nil {
		stackFatalf(t, "Error getting last sequence: %v", err)
	}
	return l
}

func msgStoreFirstAndLastSequence(t tLogger, ms MsgStore) (uint64, uint64) {
	f, l, err := ms.FirstAndLastSequence()
	if err != nil {
		stackFatalf(t, "Error getting first and last sequence: %v", err)
	}
	return f, l
}

func msgStoreGetSequenceFromTimestamp(t tLogger, ms MsgStore, timestamp int64) uint64 {
	s, err := ms.GetSequenceFromTimestamp(timestamp)
	if err != nil {
		stackFatalf(t, "Error getting sequence from timestamp: %v", err)
	}
	return s
}

func msgStoreFirstMsg(t tLogger, ms MsgStore) *pb.MsgProto {
	m, err := ms.FirstMsg()
	if err != nil {
		stackFatalf(t, "Error getting sequence first message: %v", err)
	}
	return m
}

func msgStoreLastMsg(t tLogger, ms MsgStore) *pb.MsgProto {
	m, err := ms.LastMsg()
	if err != nil {
		stackFatalf(t, "Error getting sequence last message: %v", err)
	}
	return m
}

func msgStoreState(t tLogger, ms MsgStore) (int, uint64) {
	n, b, err := ms.State()
	if err != nil {
		stackFatalf(t, "Error getting messages state: %v", err)
	}
	return n, b
}

func subStoreDeleteSub(t tLogger, ss SubStore, subid uint64) {
	if err := ss.DeleteSub(subid); err != nil {
		stackFatalf(t, "Error deleting subscription %v: %v", subid, err)
	}
}

func storeAddClient(t tLogger, s Store, clientID, hbInbox string) *Client {
	c, err := s.AddClient(clientID, hbInbox)
	if err != nil {
		stackFatalf(t, "Error adding client %q: %v", clientID, err)
	}
	return c
}

func storeCreateChannel(t tLogger, s Store, name string) *Channel {
	c, err := s.CreateChannel(name)
	if err != nil {
		stackFatalf(t, "Error creating channel %q: %v", name, err)
	}
	return c
}

func storeDeleteClient(t tLogger, s Store, clientID string) {
	if err := s.DeleteClient(clientID); err != nil {
		stackFatalf(t, "Error deleting client %q: %v", clientID, err)
	}
}

func storeMsg(t *testing.T, cs *Channel, channel string, data []byte) *pb.MsgProto {
	ms := cs.Msgs
	seq, err := ms.Store(data)
	if err != nil {
		stackFatalf(t, "Error storing message into channel [%v]: %v", channel, err)
	}
	return msgStoreLookup(t, ms, seq)
}

func storeSub(t *testing.T, cs *Channel, channel string) uint64 {
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

func storeSubPending(t *testing.T, cs *Channel, channel string, subID uint64, seqs ...uint64) {
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AddSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubAck(t *testing.T, cs *Channel, channel string, subID uint64, seqs ...uint64) {
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AckSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubDelete(t *testing.T, cs *Channel, channel string, subID ...uint64) {
	ss := cs.Subs
	for _, s := range subID {
		subStoreDeleteSub(t, ss, s)
	}
}

func TestCSBasicCreate(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	if s.Name() != storeType {
		t.Fatalf("Expecting name to be %q, got %q", storeType, s.Name())
	}
}

func TestCSInit(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	info := spb.ServerInfo{
		ClusterID:   "id",
		Discovery:   "discovery",
		Publish:     "publish",
		Subscribe:   "subscribe",
		Unsubscribe: "unsubscribe",
		Close:       "close",
	}
	// Should not fail
	if err := s.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
	info.ClusterID = "newId"
	// Should not fail
	if err := s.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
}

func TestCSNothingRecoveredOnFreshStart(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	state, err := s.Recover()
	if err != nil {
		stackFatalf(t, "Error recovering state: %v", err)
	}
	if state != nil && (len(state.Channels) > 0 || len(state.Clients) > 0) {
		t.Fatalf("Nothing should have been recovered: %v", state)
	}
}

func TestCSNewChannel(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	if cs.Subs == nil {
		t.Fatal("SubStore should not be nil")
	}
	if cs.Msgs == nil {
		t.Fatal("MsgStore should not be nil")
	}
	if cs, err := s.CreateChannel("foo"); cs != nil || err != ErrAlreadyExists {
		stackFatalf(t, "Expected create channel to return (nil, %v), got (%v, %v)", ErrAlreadyExists, cs, err)
	}
}

func TestCSCloseIdempotent(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")

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

func TestCSBasicMsgStore(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	ms := cs.Msgs

	// No message is stored, verify expected values.
	if m := msgStoreFirstMsg(t, ms); m != nil {
		t.Fatalf("Unexpected first message: %v vs %v", m, nil)
	}

	if m := msgStoreLastMsg(t, ms); m != nil {
		t.Fatalf("Unexpected first message: %v vs %v", m, nil)
	}

	if seq := msgStoreFirstSequence(t, ms); seq != 0 {
		t.Fatalf("Unexpected first sequence: %v vs %v", seq, 0)
	}

	if seq := msgStoreLastSequence(t, ms); seq != 0 {
		t.Fatalf("Unexpected first sequence: %v vs %v", seq, 0)
	}

	if s1, s2 := msgStoreFirstAndLastSequence(t, ms); s1 != 0 || s2 != 0 {
		t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
	}

	payload1 := []byte("m1")
	m1 := storeMsg(t, cs, "foo", payload1)

	payload2 := []byte("m2")
	m2 := storeMsg(t, cs, "foo", payload2)

	if string(payload1) != string(m1.Data) {
		t.Fatalf("Unexpected payload: %v", string(m1.Data))
	}
	if string(payload2) != string(m2.Data) {
		t.Fatalf("Unexpected payload: %v", string(m1.Data))
	}

	firstMsg := msgStoreFirstMsg(t, ms)
	if !reflect.DeepEqual(firstMsg, m1) {
		t.Fatalf("Unexpected first message: %v vs %v", firstMsg, m1)
	}

	lastMsg := msgStoreLastMsg(t, ms)
	if !reflect.DeepEqual(lastMsg, m2) {
		t.Fatalf("Unexpected last message: %v vs %v", lastMsg, m2)
	}

	if seq := msgStoreFirstSequence(t, ms); seq != m1.Sequence {
		t.Fatalf("Unexpected first sequence: %v vs %v", seq, m1.Sequence)
	}

	if seq := msgStoreLastSequence(t, ms); seq != m2.Sequence {
		t.Fatalf("Unexpected first sequence: %v vs %v", seq, m2.Sequence)
	}

	if s1, s2 := msgStoreFirstAndLastSequence(t, ms); s1 != m1.Sequence || s2 != m2.Sequence {
		t.Fatalf("Unexpected sequences: %v,%v", s1, s2)
	}

	lm1 := msgStoreLookup(t, ms, m1.Sequence)
	if !reflect.DeepEqual(lm1, m1) {
		t.Fatalf("Unexpected lookup result: %v instead of %v", lm1, m1)
	}

	lm2 := msgStoreLookup(t, ms, m2.Sequence)
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
	m3 := storeMsg(t, cs, "foo", []byte("last"))
	lastMsg = msgStoreLastMsg(t, ms)
	if !reflect.DeepEqual(lastMsg, m3) {
		t.Fatalf("Expected last message to be %v, got %v", m3, lastMsg)
	}
}

func TestCSMsgsState(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	payload := []byte("hello")

	cs1 := storeCreateChannel(t, s, "foo")
	cs2 := storeCreateChannel(t, s, "bar")

	m1 := storeMsg(t, cs1, "foo", payload)
	m2 := storeMsg(t, cs2, "bar", payload)

	_, isFileStore := s.(*FileStore)

	count, bytes := msgStoreState(t, cs1.Msgs)
	expectedBytes := uint64(m1.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}
	if count != 1 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
	}

	count, bytes = msgStoreState(t, cs2.Msgs)
	expectedBytes = uint64(m2.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}
	if count != 1 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
	}
}

func TestCSMaxMsgs(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

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

	cs := storeCreateChannel(t, s, "foo")

	for i := 0; i < totalSent; i++ {
		storeMsg(t, cs, "foo", payload)
	}

	count, bytes := msgStoreState(t, cs.Msgs)
	if count != limitCount || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, limitCount, bytes, expectedBytes)
	}

	// Check that older messages are no longer avail.
	if msgStoreLookup(t, cs.Msgs, 1) != nil ||
		msgStoreLookup(t, cs.Msgs, uint64(firstSeqAfterLimitReached-1)) != nil {
		t.Fatal("Older messages still available")
	}

	firstMsg := msgStoreFirstMsg(t, cs.Msgs)
	firstSeq := msgStoreFirstSequence(t, cs.Msgs)
	lastMsg := msgStoreLastMsg(t, cs.Msgs)
	lastSeq := msgStoreLastSequence(t, cs.Msgs)

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
	m := storeMsg(t, cs, "foo", bigMsg)
	expectedBytes = uint64(m.Size())
	if isFileStore {
		expectedBytes += msgRecordOverhead
	}

	count, bytes = msgStoreState(t, cs.Msgs)
	if count != 1 || bytes != expectedBytes {
		t.Fatalf("Unexpected counts: count=%v vs %v - bytes=%v vs %v", count, 1, bytes, expectedBytes)
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
	cs = storeCreateChannel(t, s, channelName)
	for i := 0; i < expectedCount+10; i++ {
		storeMsg(t, cs, channelName, payload)
	}
	n, b := msgStoreState(t, cs.Msgs)
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
	cs = storeCreateChannel(t, s, channelName)
	for i := 0; i < expectedCount+10; i++ {
		storeMsg(t, cs, channelName, payload)
	}
	n, b = msgStoreState(t, cs.Msgs)
	if n != expectedCount {
		t.Fatalf("Expected %d messages, got %v", expectedCount, n)
	}
	if b != expectedBytes {
		t.Fatalf("Expected %v bytes, got %v", expectedBytes, b)
	}
}

func TestCSMaxChannels(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	tests := []struct {
		name  string
		limit int
	}{
		{"foo", 2},
		{"bar", 0},
	}

	for _, tm := range tests {
		maxChannels := tm.limit
		limits := testDefaultStoreLimits
		limits.MaxChannels = maxChannels
		if err := s.SetLimits(&limits); err != nil {
			t.Fatalf("Error setting limits: %v", err)
		}

		total := maxChannels + 1
		if maxChannels == 0 {
			total = 10
		}
		var err error
		numCh := 0
		for i := 0; i < total; i++ {
			_, err = s.CreateChannel(fmt.Sprintf("%s.foo.%d", tm.name, i))
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
}

func TestCSMaxSubs(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	tests := []struct {
		name  string
		limit int
	}{
		{"foo", 2},
		{"bar", 0},
	}

	for _, tm := range tests {
		maxSubs := tm.limit
		limits := testDefaultStoreLimits
		limits.MaxSubscriptions = maxSubs
		if err := s.SetLimits(&limits); err != nil {
			t.Fatalf("Error setting limits: %v", err)
		}

		total := maxSubs + 1
		if maxSubs == 0 {
			total = 10
		}
		cs := storeCreateChannel(t, s, tm.name)
		sub := &spb.SubState{}
		numSubs := 0
		var err error
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
}

func testMaxAge(t *testing.T, s Store) *Channel {
	sl := testDefaultStoreLimits
	sl.MaxAge = 250 * time.Millisecond
	s.SetLimits(&sl)

	cs := storeCreateChannel(t, s, "foo")
	msg := []byte("hello")
	for i := 0; i < 10; i++ {
		storeMsg(t, cs, "foo", msg)
	}
	// Wait a bit
	time.Sleep(200 * time.Millisecond)
	// Send more
	for i := 0; i < 5; i++ {
		storeMsg(t, cs, "foo", msg)
	}
	// Wait a bit
	time.Sleep(100 * time.Millisecond)
	// We should have the first 10 expired and 5 left.
	expectedFirst := uint64(11)
	expectedLast := uint64(15)
	first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
	if first != expectedFirst || last != expectedLast {
		t.Fatalf("Expected first/last to be %v/%v, got %v/%v",
			expectedFirst, expectedLast, first, last)
	}
	// Wait more and all should be gone.
	time.Sleep(sl.MaxAge)
	if n, _ := msgStoreState(t, cs.Msgs); n != 0 {
		t.Fatalf("All messages should have expired, got %v", n)
	}
	return cs
}

func TestCSBasicSubStore(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	ss := cs.Subs
	sub := &spb.SubState{}
	sub.AckInbox = "AckInbox"

	err := ss.CreateSub(sub)
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
	subStoreDeleteSub(t, ss, sub.ID)

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
	limits := testDefaultStoreLimits
	limits.MaxAge = 500 * time.Millisecond
	s.SetLimits(&limits)
	// Force creation of channel without storing anything yet
	cs := storeCreateChannel(t, s, "foo")
	// Check before storing anything
	seq := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano())
	if seq != 0 {
		t.Fatalf("Invalid start sequence. Expected %v got %v", 0, seq)
	}

	count := 100
	msgs := make([]*pb.MsgProto, 0, count)
	payload := []byte("hello")
	for i := 0; i < count; i++ {
		m := storeMsg(t, cs, "foo", payload)
		msgs = append(msgs, m)
		time.Sleep(1 * time.Millisecond)
	}

	startMsg := msgs[count/2]
	seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, startMsg.Timestamp)
	if seq != startMsg.Sequence {
		t.Fatalf("Invalid start sequence. Expected %v got %v", startMsg.Sequence, seq)
	}
	seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, msgs[0].Timestamp-int64(time.Second))
	if seq != msgs[0].Sequence {
		t.Fatalf("Expected seq to be %v, got %v", msgs[0].Sequence, seq)
	}
	seq = msgStoreGetSequenceFromTimestamp(t, cs.Msgs, msgs[count-1].Timestamp+int64(time.Second))
	if seq != msgs[count-1].Sequence+1 {
		t.Fatalf("Expected seq to be %v, got %v", msgs[count-1].Sequence+1, seq)
	}
	// Wait for all messages to expire
	time.Sleep(600 * time.Millisecond)
	// Now these calls should all return the lastSeq + 1
	seq1 := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano()-int64(time.Hour))
	seq2 := msgStoreGetSequenceFromTimestamp(t, cs.Msgs, time.Now().UnixNano()+int64(time.Hour))
	if seq1 != seq2 || seq1 != msgs[count-1].Sequence+1 {
		t.Fatalf("After expiration, returned sequence should be: %v, got %v %v", msgs[count-1].Sequence+1, seq1, seq2)
	}
}

func testClientAPIs(t *testing.T, s Store) {
	// Delete client that does not exist
	storeDeleteClient(t, s, "client1")

	// Delete a client before adding it
	storeDeleteClient(t, s, "client2")

	// Adding it after the delete
	storeAddClient(t, s, "client2", "hbInbox")

	// Adding it another time should not return an error
	storeAddClient(t, s, "client2", "hbInbox")

	// Add a client
	storeAddClient(t, s, "client3", "hbInbox")

	// Add a client then..
	storeAddClient(t, s, "client4", "hbInbox")
	// Delete it.
	storeDeleteClient(t, s, "client4")
}

func testFlush(t *testing.T, s Store) *Channel {
	cs := storeCreateChannel(t, s, "foo")
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
	return cs
}

func TestGSNoOps(t *testing.T) {
	gs := &genericStore{}
	defer gs.Close()
	limits := DefaultStoreLimits
	gs.init("test generic", testLogger, &limits)
	if _, err := gs.GetExclusiveLock(); err != ErrNotSupported {
		t.Fatalf("Expected %v error, got %v", ErrNotSupported, err)
	}
	// All other calls should be a no-op
	if err := gs.Init(&spb.ServerInfo{}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state, err := gs.Recover(); state != nil || err != nil {
		t.Fatalf("Unexpected state or error: %v - %v", state, err)
	}
	if err := gs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if c, err := gs.CreateChannel("foo"); c != nil || err != nil {
		t.Fatalf("Unexpected channel or error: %v - %v", c, err)
	}
	if err := gs.DeleteClient("me"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := gs.Close(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	gms := &genericMsgStore{}
	defer gms.Close()
	gms.init("foo", testLogger, &limits.MsgStoreLimits)
	if msgStoreLookup(t, gms, 1) != nil ||
		msgStoreFirstMsg(t, gms) != nil ||
		msgStoreLastMsg(t, gms) != nil ||
		gms.Flush() != nil ||
		msgStoreGetSequenceFromTimestamp(t, gms, 0) != 0 ||
		gms.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}
	if seq, err := gms.Store([]byte("hello")); seq != 0 || err != nil {
		t.Fatal("Expected no value since this should not be implemented for generic store")
	}

	gss := &genericSubStore{}
	defer gss.Close()
	gss.init("foo", testLogger, &limits.SubStoreLimits)
	if gss.UpdateSub(&spb.SubState{}) != nil ||
		gss.AddSeqPending(1, 1) != nil ||
		gss.AckSeqPending(1, 1) != nil ||
		gss.Flush() != nil ||
		gss.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}
}

func TestCSPerChannelLimits(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

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
		cs := storeCreateChannel(t, s, channelName)
		for i := 0; i < maxMsgs+10; i++ {
			if _, err := cs.Msgs.Store([]byte("hello")); err != nil {
				stackFatalf(t, "Unexpected error on store: %v", err)
			}
		}
		if n, _ := msgStoreState(t, cs.Msgs); n != maxMsgs {
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

func testNegativeLimit(t *testing.T, s Store) {
	limits := DefaultStoreLimits

	checkLimitError := func() {
		if err := s.SetLimits(&limits); err == nil {
			stackFatalf(t, "Setting negative limit should have failed")
		}
	}
	limits.MaxAge, _ = time.ParseDuration("-1.5h")
	checkLimitError()
	limits = DefaultStoreLimits
	limits.MaxBytes = -1000
	checkLimitError()
	limits = DefaultStoreLimits
	limits.MaxChannels = -1000
	checkLimitError()
	limits = DefaultStoreLimits
	limits.MaxMsgs = -1000
	checkLimitError()
	limits = DefaultStoreLimits
	limits.MaxSubscriptions = -1000
	checkLimitError()
}

func TestCSLimitWithWildcardsInConfig(t *testing.T) {
	defer endTest(t)
	s := startTest(t)
	defer s.Close()

	lv := DefaultStoreLimits
	l := &lv
	cl := &ChannelLimits{}
	cl.MaxMsgs = 3
	l.AddPerChannel(">", cl)
	cl2 := &ChannelLimits{}
	cl2.MaxMsgs = 2
	l.AddPerChannel("foo.>", cl2)
	s.SetLimits(l)
	foobar := "foo.bar"
	cFooBar := storeCreateChannel(t, s, foobar)
	m1 := storeMsg(t, cFooBar, foobar, []byte("msg1"))
	storeMsg(t, cFooBar, foobar, []byte("msg2"))
	// This should kick out m1 since for foo.bar, limit will be 2
	storeMsg(t, cFooBar, foobar, []byte("msg3"))
	if msgStoreLookup(t, cFooBar.Msgs, m1.Sequence) != nil {
		stackFatalf(t, "M1 should have been removed")
	}
	// For bar, however, we should be able to store 3 messages
	bar := "bar"
	cBar := storeCreateChannel(t, s, bar)
	m1 = storeMsg(t, cBar, bar, []byte("msg1"))
	storeMsg(t, cBar, bar, []byte("msg2"))
	storeMsg(t, cBar, bar, []byte("msg3"))
	// Now, a 4th one should evict m1
	storeMsg(t, cBar, bar, []byte("msg4"))
	if msgStoreLookup(t, cBar.Msgs, m1.Sequence) != nil {
		stackFatalf(t, "M1 should have been removed")
	}
}
