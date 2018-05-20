// Copyright 2016-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stores

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/test"
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
		0,
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

type testStore struct {
	name        string
	recoverable bool
}

var (
	testLogger logger.Logger
	testStores = []*testStore{
		&testStore{TypeMemory, false},
		&testStore{TypeFile, true},
		&testStore{TypeSQL, true},
		&testStore{TypeRaft, false},
	}
	testTimestampMu   sync.Mutex
	testLastTimestamp int64
)

func init() {
	// Create an empty logger (no actual logger is set without calling SetLogger())
	testLogger = logger.NewStanLogger()
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
	client := &spb.ClientInfo{
		ID:      clientID,
		HbInbox: hbInbox,
	}
	c, err := s.AddClient(client)
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

func storeMsg(t tLogger, cs *Channel, channel string, seq uint64, data []byte) *pb.MsgProto {
	testTimestampMu.Lock()
	tm := time.Now().UnixNano()
	if testLastTimestamp > 0 && tm < testLastTimestamp {
		tm = testLastTimestamp
	}
	testLastTimestamp = tm
	testTimestampMu.Unlock()
	ms := cs.Msgs
	seq, err := ms.Store(&pb.MsgProto{
		Sequence:  seq,
		Data:      data,
		Subject:   channel,
		Timestamp: tm,
	})
	if err != nil {
		stackFatalf(t, "Error storing message into channel [%v]: %v", channel, err)
	}
	return msgStoreLookup(t, ms, seq)
}

func storeSub(t tLogger, cs *Channel, channel string) uint64 {
	nid := nuid.New()
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nid.Next(),
		AckInbox:      nid.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub); err != nil {
		stackFatalf(t, "Error storing subscription into channel [%v]: %v", channel, err)
	}
	return sub.ID
}

func storeSubPending(t tLogger, cs *Channel, channel string, subID uint64, seqs ...uint64) {
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AddSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubAck(t tLogger, cs *Channel, channel string, subID uint64, seqs ...uint64) {
	ss := cs.Subs
	for _, s := range seqs {
		if err := ss.AckSeqPending(subID, s); err != nil {
			t.Fatalf("Unexpected error adding pending for sub [%v] on channel [%v]: %v", subID, channel, err)
		}
	}
}

func storeSubFlush(t tLogger, cs *Channel, channel string) {
	if err := cs.Subs.Flush(); err != nil {
		stackFatalf(t, "Error flushing sub store for channel %q: %v", channel, err)
	}
}

func storeSubDelete(t tLogger, cs *Channel, channel string, subID ...uint64) {
	ss := cs.Subs
	for _, s := range subID {
		subStoreDeleteSub(t, ss, s)
	}
}

func getRecoveredChannel(t tLogger, state *RecoveredState, name string) *Channel {
	if state == nil {
		stackFatalf(t, "Expected state to be recovered")
	}
	rc := state.Channels[name]
	if rc == nil {
		stackFatalf(t, "Channel %q should have been recovered", name)
	}
	return rc.Channel
}

func getRecoveredSubs(t tLogger, state *RecoveredState, name string, expected int) []*RecoveredSubscription {
	if state == nil {
		stackFatalf(t, "Expected state to be recovered")
	}
	rc := state.Channels[name]
	if rc == nil {
		stackFatalf(t, "Channel %q should have been recovered", name)
	}
	subs := rc.Subscriptions
	if len(subs) != expected {
		stackFatalf(t, "Channel %q should have %v subscriptions, got %v", name, expected, len(subs))
	}
	return subs
}

func startTest(t tLogger, ts *testStore) Store {
	switch ts.name {
	case TypeMemory:
		return createDefaultMemStore(t)
	case TypeFile:
		cleanupFSDatastore(t)
		return createDefaultFileStore(t)
	case TypeSQL:
		cleanupSQLDatastore(t)
		return createDefaultSQLStore(t)
	case TypeRaft:
		cleanupRaftDatastore(t)
		return createDefaultRaftStore(t)
	default:
		// This is used with testStores table. If a store type has been
		// added there, it needs to be added here.
		panic(fmt.Sprintf("Add new store type %q in startTest", ts.name))
	}
}

func endTest(t tLogger, ts *testStore) {
	switch ts.name {
	case TypeFile:
		cleanupFSDatastore(t)
	case TypeSQL:
		cleanupSQLDatastore(t)
	case TypeRaft:
		cleanupRaftDatastore(t)
	}
}

func testReOpenStore(t tLogger, ts *testStore, limits *StoreLimits) (Store, *RecoveredState) {
	if !ts.recoverable {
		stackFatalf(t, "Cannot reopen a store (%v) that is not recoverable", ts.name)
	}
	switch ts.name {
	case TypeFile:
		return openDefaultFileStoreWithLimits(t, limits)
	case TypeSQL:
		return openDefaultSQLStoreWithLimits(t, limits)
	default:
		// This is used with testStores table. If a recoverable
		// store type has been added there, it needs to be added here.
		panic(fmt.Sprintf("Add new store type %q in testReopenStore", ts.name))
	}
}

func isStorageBasedOnFile(s Store) bool {
	switch s.(type) {
	case *FileStore:
		return true
	case *RaftStore:
		return true
	default:
		return false
	}
}

var doSQL bool

func TestMain(m *testing.M) {
	flag.BoolVar(&testFSDisableBufferWriters, "fs_no_buffer", false, "Disable use of buffer writers")
	flag.BoolVar(&testFSSetFDsLimit, "fs_set_fds_limit", false, "Set some FDs limit")
	flag.BoolVar(&doSQL, "sql", true, "Set this to false if you don't want SQL to be tested")
	test.AddSQLFlags(flag.CommandLine, &testSQLDriver, &testSQLSource, &testSQLSourceAdmin, &testSQLDatabaseName)
	flag.Parse()

	if doSQL {
		defaultSources := make(map[string][]string)
		defaultSources[test.DriverMySQL] = []string{testDefaultMySQLSource, testDefaultMySQLSourceAdmin}
		defaultSources[test.DriverPostgres] = []string{testDefaultPostgresSource, testDefaultPostgresSourceAdmin}
		if err := test.ProcessSQLFlags(flag.CommandLine, defaultSources); err != nil {
			fmt.Println(err.Error())
			os.Exit(2)
		}
		// Create the SQL Database once, the cleanup is simply deleting
		// content from tables (so we don't have to recreate them).
		if err := test.CreateSQLDatabase(testSQLDriver, testSQLSourceAdmin,
			testSQLSource, testSQLDatabaseName); err != nil {
			fmt.Printf("Error initializing SQL Datastore: %v", err)
			os.Exit(2)
		}
	} else {
		// Remove SQL Store from the testStores array
		newArray := []*testStore{}
		for _, st := range testStores {
			if st.name != TypeSQL {
				newArray = append(newArray, st)
			}
		}
		testStores = newArray
	}
	ret := m.Run()
	if doSQL {
		// Now that the tests/bench have all run, delete the database.
		test.DeleteSQLDatabase(testSQLDriver, testSQLSourceAdmin, testSQLDatabaseName)
	}
	os.Exit(ret)
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
		gms.Empty() != nil ||
		gms.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}
	if seq, err := gms.Store(&pb.MsgProto{Data: []byte("hello")}); seq != 0 || err != nil {
		t.Fatal("Expected no value since this should not be implemented for generic store")
	}

	gss := &genericSubStore{}
	defer gss.Close()
	gss.init(testLogger, &limits.SubStoreLimits)
	if gss.UpdateSub(&spb.SubState{}) != nil ||
		gss.AddSeqPending(1, 1) != nil ||
		gss.AckSeqPending(1, 1) != nil ||
		gss.Flush() != nil ||
		gss.Close() != nil {
		t.Fatal("Expected no value since these should not be implemented for generic store")
	}
}

func TestCSBasicCreate(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			expectedName := st.name
			if st.name == TypeRaft {
				expectedName = TypeRaft + "_" + s.(*RaftStore).Store.Name()
			}

			if s.Name() != expectedName {
				t.Fatalf("Expecting name to be %q, got %q", expectedName, s.Name())
			}
		})
	}
}

func TestCSInit(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)

			var (
				s   Store
				err error
			)
			switch st.name {
			case TypeMemory:
				s, err = NewMemoryStore(testLogger, nil)
			case TypeFile:
				s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
			case TypeSQL:
				s, err = NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil)
			case TypeRaft:
				s, err = NewFileStore(testLogger, testRSDefaultDatastore, nil)
				if err == nil {
					s = NewRaftStore(s)
				}
			default:
				panic(fmt.Errorf("Add store type %q in this test", st.name))
			}
			if err != nil {
				t.Fatalf("Error creating store: %v", err)
			}
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
			newInfo := info
			newInfo.ClusterID = "newId"
			// Should not fail
			if err := s.Init(&newInfo); err != nil {
				t.Fatalf("Error during init: %v", err)
			}

			if st.recoverable {
				// Close the store
				s.Close()

				s, state := testReOpenStore(t, st, nil)
				defer s.Close()
				if state == nil {
					t.Fatal("Expected state to be recovered")
				}
				// Check content
				info = *state.Info
				if !reflect.DeepEqual(newInfo, info) {
					t.Fatalf("Unexpected server info, expected %v, got %v",
						newInfo, info)
				}
			}
		})
	}
}

func TestCSNothingRecoveredOnFreshStart(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			state, err := s.Recover()
			if err != nil {
				stackFatalf(t, "Error recovering state: %v", err)
			}
			if state != nil && (len(state.Channels) > 0 || len(state.Clients) > 0) {
				t.Fatalf("Nothing should have been recovered: %v", state)
			}
		})
	}
}

func TestCSBasicRecovery(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			state, err := s.Recover()
			if !st.recoverable {
				if err != nil {
					t.Fatalf("Recover should not return an error, got %v", err)
				}
				if st.name != TypeRaft && state != nil {
					t.Fatalf("State should be nil, got %v", state)
				}
				// We are done for non recoverable stores.
				return
			}
			if err != nil {
				t.Fatalf("Error recovering state: %v", err)
			}
			if state != nil && (len(state.Clients) > 0 || len(state.Channels) > 0) {
				t.Fatal("Nothing should have been recovered")
			}

			cFoo := storeCreateChannel(t, s, "foo")

			foo1 := storeMsg(t, cFoo, "foo", 1, []byte("foomsg"))
			foo2 := storeMsg(t, cFoo, "foo", 2, []byte("foomsg"))
			foo3 := storeMsg(t, cFoo, "foo", 3, []byte("foomsg"))

			cBar := storeCreateChannel(t, s, "bar")

			bar1 := storeMsg(t, cBar, "bar", 1, []byte("barmsg"))
			bar2 := storeMsg(t, cBar, "bar", 2, []byte("barmsg"))
			bar3 := storeMsg(t, cBar, "bar", 3, []byte("barmsg"))
			bar4 := storeMsg(t, cBar, "bar", 4, []byte("barmsg"))

			sub1 := storeSub(t, cFoo, "foo")
			sub2 := storeSub(t, cBar, "bar")

			storeSubPending(t, cFoo, "foo", sub1, foo1.Sequence, foo2.Sequence, foo3.Sequence)
			storeSubAck(t, cFoo, "foo", sub1, foo1.Sequence, foo3.Sequence)

			storeSubPending(t, cBar, "bar", sub2, bar1.Sequence, bar2.Sequence, bar3.Sequence, bar4.Sequence)
			storeSubAck(t, cBar, "bar", sub2, bar4.Sequence)

			s.Close()

			s, state = testReOpenStore(t, st, nil)
			defer s.Close()
			if state == nil {
				t.Fatal("Expected state to be recovered")
			}

			// Check that subscriptions are restored
			for channel, rc := range state.Channels {
				recoveredSubs := rc.Subscriptions
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
					for seq := range recSub.Pending {
						if seq != foo2.Sequence {
							t.Fatalf("Unexpected recovered pending seqno for sub1: %v", seq)
						}
					}
				case "bar":
					if subID != sub2 {
						t.Fatalf("Invalid subscription id. Expected %v, got %v", sub2, subID)
					}
					for seq := range recSub.Pending {
						if seq != bar1.Sequence && seq != bar2.Sequence && seq != bar3.Sequence {
							t.Fatalf("Unexpected recovered pending seqno for sub2: %v", seq)
						}
					}
				default:
					t.Fatalf("Recovered unknown channel: %v", channel)
				}
			}

			cs := getRecoveredChannel(t, state, "foo")
			// In message store, the first message should still be foo1,
			// regardless of what has been consumed.
			m := msgStoreFirstMsg(t, cs.Msgs)
			if m == nil || m.Sequence != foo1.Sequence {
				t.Fatalf("Unexpected message for foo channel: %v", m)
			}
			// Check that messages recovered from MsgStore are never
			// marked as redelivered.
			checkRedelivered := func(ms MsgStore) bool {
				start, end := msgStoreFirstAndLastSequence(t, ms)
				for i := start; i <= end; i++ {
					if m := msgStoreLookup(t, ms, i); m != nil && m.Redelivered {
						return true
					}
				}
				return false
			}
			if checkRedelivered(cs.Msgs) {
				t.Fatalf("Messages in MsgStore should not be marked as redelivered")
			}

			cs = getRecoveredChannel(t, state, "bar")
			// In message store, the first message should still be bar1,
			// regardless of what has been consumed.
			m = msgStoreFirstMsg(t, cs.Msgs)
			if m == nil || m.Sequence != bar1.Sequence {
				t.Fatalf("Unexpected message for bar channel: %v", m)
			}
			if checkRedelivered(cs.Msgs) {
				t.Fatalf("Messages in MsgStore should not be marked as redelivered")
			}

			rc := state.Channels["baz"]
			if rc != nil {
				t.Fatal("Expected to get nil channel for baz, got something instead")
			}
		})
	}
}

func TestCSNewChannel(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
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
		})
	}
}

func TestCSCloseIdempotent(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
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
		})
	}
}

func TestCSMaxChannels(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
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
		})
	}
}

func TestCSClientAPIs(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

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

			if st.recoverable {
				// Restart the store
				s.Close()

				s, state := testReOpenStore(t, st, nil)
				defer s.Close()
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
		})
	}
}

func TestCSFlush(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			seq, err := cs.Msgs.Store(&pb.MsgProto{Sequence: 1, Data: []byte("hello")})
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

			if st.name == TypeFile {
				// Now specific tests to File store
				msg := storeMsg(t, cs, "foo", 2, []byte("new msg"))
				subID := storeSub(t, cs, "foo")
				storeSubPending(t, cs, "foo", subID, msg.Sequence)
				// Close the underlying file
				ms := cs.Msgs.(*FileMsgStore)
				ms.Lock()
				ms.writeSlice.file.handle.Close()
				ms.Unlock()
				// Expect Flush to fail
				if err := cs.Msgs.Flush(); err == nil {
					t.Fatal("Expected Flush to fail, did not")
				}
				// Close the underlying file
				ss := cs.Subs.(*FileSubStore)
				ss.Lock()
				ss.file.handle.Close()
				ss.Unlock()
				// Expect Flush to fail
				if err := cs.Subs.Flush(); err == nil {
					t.Fatal("Expected Flush to fail, did not")
				}

				// Close and re-open
				s.Close()
				s, state := openDefaultFileStore(t, DoSync(true))
				defer s.Close()

				// Since files are closed after recovery, Flush() is now
				// not expected to fail.
				cs = getRecoveredChannel(t, state, "foo")
				// Close the underlying file
				ms = cs.Msgs.(*FileMsgStore)
				ms.Lock()
				ms.writeSlice.file.handle.Close()
				ms.Unlock()
				// Expect Flush to fail
				if err := cs.Msgs.Flush(); err != nil {
					t.Fatalf("Error on flush: %v", err)
				}
				// Close the underlying file
				ss = cs.Subs.(*FileSubStore)
				ss.Lock()
				ss.file.handle.Close()
				// Simulate that there was activity (alternatively,
				// we would need a buffer size smaller than a sub record
				// being written so that buffer writer is by-passed).
				ss.activity = true
				ss.Unlock()
				if err := cs.Subs.Flush(); err != nil {
					t.Fatalf("Error on flush: %v", err)
				}
			}
		})
	}
}

func TestCSPerChannelLimits(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
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
				0,
			}
			barLimits := ChannelLimits{
				MsgStoreLimits{
					MaxMsgs:  5,
					MaxBytes: 5 * 1024,
				},
				SubStoreLimits{
					MaxSubscriptions: 2,
				},
				0,
			}
			noSubsOverrideLimits := ChannelLimits{
				MsgStoreLimits{
					MaxMsgs:  6,
					MaxBytes: 6 * 1024,
				},
				SubStoreLimits{},
				0,
			}
			noMaxMsgOverrideLimits := ChannelLimits{
				MsgStoreLimits{
					MaxBytes: 7 * 1024,
				},
				SubStoreLimits{},
				0,
			}
			noMaxBytesOverrideLimits := ChannelLimits{
				MsgStoreLimits{
					MaxMsgs: 10,
				},
				SubStoreLimits{},
				0,
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
					storeMsg(t, cs, channelName, uint64(i+1), []byte("hello"))
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
		})
	}
}

func TestCSNegativeLimit(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

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
		})
	}
}

func TestCSLimitWithWildcardsInConfig(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
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
			m1 := storeMsg(t, cFooBar, foobar, 1, []byte("msg1"))
			storeMsg(t, cFooBar, foobar, 2, []byte("msg2"))
			// This should kick out m1 since for foo.bar, limit will be 2
			storeMsg(t, cFooBar, foobar, 3, []byte("msg3"))
			if msgStoreLookup(t, cFooBar.Msgs, m1.Sequence) != nil {
				stackFatalf(t, "M1 should have been removed")
			}
			// For bar, however, we should be able to store 3 messages
			bar := "bar"
			cBar := storeCreateChannel(t, s, bar)
			m1 = storeMsg(t, cBar, bar, 1, []byte("msg1"))
			storeMsg(t, cBar, bar, 2, []byte("msg2"))
			storeMsg(t, cBar, bar, 3, []byte("msg3"))
			// Now, a 4th one should evict m1
			storeMsg(t, cBar, bar, 4, []byte("msg4"))
			if msgStoreLookup(t, cBar.Msgs, m1.Sequence) != nil {
				stackFatalf(t, "M1 should have been removed")
			}
		})
	}
}

func TestCSGetChannelLimits(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			limits := &StoreLimits{}
			limits.MaxChannels = 10
			limits.MaxMsgs = 20
			limits.MaxBytes = 30
			limits.MaxAge = 40
			limits.MaxSubscriptions = 50
			limits.MaxInactivity = 60

			clFooStar := &ChannelLimits{}
			clFooStar.MaxMsgs = 70
			clFooStar.MaxInactivity = -1
			limits.AddPerChannel("foo.*", clFooStar)

			if err := s.SetLimits(limits); err != nil {
				t.Fatalf("Error setting limits: %v", err)
			}

			// The store owns a copy of the limits and inheritance
			// has been applied in that copy, not with the limits
			// given by the caller. So for the rest of the test
			// we need to apply inheritance to limits.
			if err := limits.Build(); err != nil {
				t.Fatalf("Error building limits: %v", err)
			}
			clFooStar = limits.PerChannel["foo.*"]

			// Check for non existing channel
			cl := s.GetChannelLimits("unknown")
			if cl != nil {
				t.Fatalf("Should have returned nil, returned %v", cl)
			}

			// This channel should have same limits than foo.*
			storeCreateChannel(t, s, "foo.bar")
			cl = s.GetChannelLimits("foo.bar")
			if cl == nil {
				t.Fatal("Should have returned the channel limits")
			}
			if !reflect.DeepEqual(clFooStar, cl) {
				t.Fatalf("Expected channel limits to be %+v, got %+v", clFooStar, cl)
			}

			// This channel should have same limits than the global ones
			storeCreateChannel(t, s, "foo")
			cl = s.GetChannelLimits("foo")
			if cl == nil {
				t.Fatal("Should have returned the channel limits")
			}
			if !reflect.DeepEqual(&limits.ChannelLimits, cl) {
				t.Fatalf("Expected channel limits to be %+v, got %+v", &limits.ChannelLimits, cl)
			}

			// Check that the returned value is a copy
			cl.MaxBytes = 2
			newCL := s.GetChannelLimits("foo")
			if newCL == nil {
				t.Fatal("Should have returned the channel limits")
			}
			// There should be different
			if reflect.DeepEqual(cl, newCL) {
				t.Fatal("These should have been different")
			}
			// newCL should be same than global limits
			if !reflect.DeepEqual(&limits.ChannelLimits, newCL) {
				t.Fatalf("Expected channel limits to be %+v, got %+v", &limits.ChannelLimits, newCL)
			}
		})
	}
}

func TestCSDeleteChannel(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			if err := s.DeleteChannel("notfound"); err != ErrNotFound {
				t.Fatalf("Expected %v error, got %v", ErrNotFound, err)
			}
			storeCreateChannel(t, s, "foo")
			if err := s.DeleteChannel("foo"); err != nil {
				t.Fatalf("Error on delete: %v", err)
			}

			if !st.recoverable {
				return
			}
			// Restart the store and ensure channel "foo" is not reconvered
			s.Close()
			s, state := testReOpenStore(t, st, nil)
			defer s.Close()
			if state != nil && len(state.Channels) > 0 {
				t.Fatal("Channel recovered after restart")
			}
		})
	}
}

func TestCSAddClientProto(t *testing.T) {
	for _, st := range testStores {
		if !st.recoverable {
			continue
		}
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			info := &spb.ClientInfo{
				ID:       "me",
				HbInbox:  "hbInbox",
				ConnID:   []byte("connID"),
				Protocol: 1,
			}
			c, err := s.AddClient(info)
			if err != nil {
				t.Fatalf("Error adding client: %v", err)
			}
			if !reflect.DeepEqual(&c.ClientInfo, info) {
				t.Fatalf("Expected %v, got %v", info, c.ClientInfo)
			}
			s.Close()

			s, state := testReOpenStore(t, st, nil)
			defer s.Close()

			if l := len(state.Clients); l != 1 {
				t.Fatalf("Expected to have recovered 1 client, got %v", l)
			}
			rc := state.Clients[0]
			if !reflect.DeepEqual(c, rc) {
				t.Fatalf("Expected %v, got %v", c, rc)
			}
		})
	}
}
