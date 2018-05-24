// Copyright 2017-2018 The NATS Authors
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
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/test"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	_ "github.com/lib/pq"              // postgres driver
)

// The SourceAdmin is used by the test setup to have access
// to the database server and create the test streaming database.
// The Source contains the URL that the Store needs to actually
// connect to the server and use the database.
const (
	testDefaultDatabaseName = "test_nats_streaming"

	testDefaultMySQLSource      = "nss:password@/" + testDefaultDatabaseName
	testDefaultMySQLSourceAdmin = "nss:password@/"

	testDefaultPostgresSource      = "dbname=" + testDefaultDatabaseName + " sslmode=disable"
	testDefaultPostgresSourceAdmin = "sslmode=disable"
)

var (
	testSQLDriver       = driverMySQL
	testSQLDatabaseName = testDefaultDatabaseName
	testSQLSource       = testDefaultMySQLSource
	testSQLSourceAdmin  = testDefaultMySQLSourceAdmin
)

func cleanupSQLDatastore(t tLogger) {
	test.CleanupSQLDatastore(t, testSQLDriver, testSQLSource)
}

func newSQLStore(t tLogger, driver, source string, limits *StoreLimits) (*SQLStore, *RecoveredState, error) {
	// Disable caching for most tests. To test caching, there will be
	// specific tests that create the store with caching enabled.
	// Also, set the max number of connections to rather low value.
	ss, err := NewSQLStore(testLogger, driver, source, limits, SQLNoCaching(true), SQLMaxOpenConns(5))
	if err != nil {
		return nil, nil, err
	}
	state, err := ss.Recover()
	if err != nil {
		ss.Close()
		return nil, nil, err
	}
	return ss, state, nil
}

func createDefaultSQLStore(t tLogger) *SQLStore {
	limits := testDefaultStoreLimits
	ss, state, err := newSQLStore(t, testSQLDriver, testSQLSource, &limits)
	if err != nil {
		stackFatalf(t, "Unable to create a SQLStore instance: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo
		if err := ss.Init(&info); err != nil {
			ss.Close()
			stackFatalf(t, "Error on Init: %v", err)
		}
	}
	return ss
}

func openDefaultSQLStoreWithLimits(t tLogger, limits *StoreLimits) (*SQLStore, *RecoveredState) {
	if limits == nil {
		l := testDefaultStoreLimits
		limits = &l
	}
	ss, state, err := newSQLStore(t, testSQLDriver, testSQLSource, limits)
	if err != nil {
		stackFatalf(t, "unable to open SqlStore instance: %v", err)
	}
	return ss, state
}

func failDBConnection(t *testing.T, s Store) {
	ss := s.(*SQLStore)
	ss.Lock()
	err := ss.db.Close()
	ss.Unlock()
	if err != nil {
		stackFatalf(t, "Error failing db connection: %v", err)
	}
}

func restoreDBConnection(t *testing.T, s Store) {
	ss := s.(*SQLStore)
	ss.Lock()
	db, err := sql.Open(testSQLDriver, testSQLSource)
	if err == nil {
		ss.db = db
		if ss.dbLock != nil {
			ss.dbLock.Lock()
			ss.dbLock.db = db
			ss.dbLock.Unlock()
		}
		for _, c := range ss.channels {
			ms := c.Msgs.(*SQLMsgStore)
			ms.Lock()
			subs := c.Subs.(*SQLSubStore)
			subs.Lock()
		}
		err = ss.createPreparedStmts()
		for _, c := range ss.channels {
			ms := c.Msgs.(*SQLMsgStore)
			ms.Unlock()
			subs := c.Subs.(*SQLSubStore)
			subs.Unlock()
		}
	}
	ss.Unlock()
	if err != nil {
		stackFatalf(t, "Error failing db connection: %v", err)
	}
}

func getDBConnection(t *testing.T) *sql.DB {
	db, err := sql.Open(testSQLDriver, testSQLSource)
	if err != nil {
		stackFatalf(t, "Error opening db: %v", err)
	}
	return db
}

func TestSQLAllOptions(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	opts := &SQLStoreOptions{
		NoCaching:    true,
		MaxOpenConns: 123,
	}
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLAllOptions(opts))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	s.RLock()
	so := s.opts
	s.RUnlock()
	if !so.NoCaching {
		t.Fatal("NoCaching should be true")
	}
	if so.MaxOpenConns != 123 {
		t.Fatalf("MaxOpenConns should be 123, got %v", so.MaxOpenConns)
	}
}

func TestSQLPostgresDriverInit(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	var realStmts []string
	realStmts = append(realStmts, sqlStmts...)
	defer func() {
		sqlStmts = nil
		sqlStmts = append(sqlStmts, realStmts...)
	}()

	// Make sure sqlStms table is set...
	initSQLStmtsTable(driverPostgres)

	// Make sure there is not ? but $ in the statements
	reg := regexp.MustCompile(`\?`)
	for _, stmt := range sqlStmts {
		if reg.FindString(stmt) != "" {
			t.Fatalf("Statement %q incorrect for Postgres driver", stmt)
		}
	}
	// Make sure there is not `row` in the statements
	reg = regexp.MustCompile("`row`")
	for _, stmt := range sqlStmts {
		if reg.FindString(stmt) != "" {
			t.Fatalf("Statement %q incorrect for Postgres driver", stmt)
		}
	}
}

func TestSQLErrorOnNewStore(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	params := []struct {
		d string
		s string
	}{
		{testSQLDriver, ""},
		{"", testSQLSource},
		{"", ""},
	}
	// Some invalid parameters
	for _, p := range params {
		s, err := NewSQLStore(testLogger, p.d, p.s, nil)
		if err == nil || s != nil {
			if s != nil {
				s.Close()
			}
			t.Fatalf("Expecting to fail and s to be nil with driver=%q source=%q, got s=%p err=%v", p.d, p.s, s, err)
		}
	}

	// Negative limits
	limits := DefaultStoreLimits
	limits.MaxMsgs = -1000
	if s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits); s != nil || err == nil {
		if s != nil {
			s.Close()
		}
		t.Fatal("Should have failed to create store with a negative limit")
	}
}

func TestSQLInitUniqueRow(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)
	s := createDefaultSQLStore(t)
	defer s.Close()

	si := testDefaultServerInfo
	if err := s.Init(&si); err != nil {
		t.Fatalf("Error on init: %v", err)
	}
	si.ClusterID = "other id"
	if err := s.Init(&si); err != nil {
		t.Fatalf("Error on init: %v", err)
	}

	// Ensure there is only 1 row in the ServerInfo table
	db := getDBConnection(t)
	defer db.Close()
	r := db.QueryRow("select count(*) from ServerInfo")
	count := 0
	if err := r.Scan(&count); err != nil {
		t.Fatalf("Error on scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 row, got %v", count)
	}
}

func TestSQLErrorsDueToFailDBConnection(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)
	s := createDefaultSQLStore(t)
	defer s.Close()

	sl := testDefaultStoreLimits
	cl := &ChannelLimits{}
	cl.MaxSubscriptions = -1 // no sub limit for this channel
	sl.AddPerChannel("baz", cl)
	s.SetLimits(&sl)

	cs := storeCreateChannel(t, s, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg"))
	subID1 := storeSub(t, cs, "foo")
	subID2 := storeSub(t, cs, "foo")

	cs2 := storeCreateChannel(t, s, "baz")

	failDBConnection(t, s)

	expectToFail := func(f func() error) {
		if err := f(); err == nil || !strings.Contains(err.Error(), "closed") {
			stackFatalf(t, "Expected error about db closed, got %v", err)
		}
	}

	expectToFail(func() error { return s.Init(&testDefaultServerInfo) })
	expectToFail(func() error {
		_, err := s.CreateChannel("bar")
		return err
	})
	expectToFail(func() error { return s.DeleteChannel("foo") })
	expectToFail(func() error {
		_, err := s.AddClient(&spb.ClientInfo{ID: "me", HbInbox: "hbInbox"})
		return err
	})
	expectToFail(func() error {
		_, err := s.Recover()
		return err
	})
	expectToFail(func() error { return s.DeleteClient("me") })
	expectToFail(func() error {
		_, err := cs.Msgs.Store(&pb.MsgProto{
			Sequence:  2,
			Data:      []byte("hello"),
			Subject:   "foo",
			Timestamp: time.Now().UnixNano(),
		})
		return err
	})
	expectToFail(func() error {
		_, err := cs.Msgs.Lookup(1)
		return err
	})
	expectToFail(func() error {
		_, err := cs.Msgs.GetSequenceFromTimestamp(time.Now().UnixNano())
		return err
	})
	expectToFail(func() error { return cs.Subs.CreateSub(&spb.SubState{}) })
	expectToFail(func() error { return cs.Subs.UpdateSub(&spb.SubState{}) })
	expectToFail(func() error { return cs.Subs.AddSeqPending(subID1, 1) })
	expectToFail(func() error { return cs.Subs.AckSeqPending(subID1, 1) })
	expectToFail(func() error { return cs.Subs.DeleteSub(subID1) })
	expectToFail(func() error { return cs.Subs.DeleteSub(subID2) })
	expectToFail(func() error { return cs2.Subs.CreateSub(&spb.SubState{}) })

	restoreDBConnection(t, s)
}

func TestSQLErrorOnMsgExpiration(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)
	s := createDefaultSQLStore(t)
	defer s.Close()

	sqlExpirationIntervalOnError = 15 * time.Millisecond
	defer func() { sqlExpirationIntervalOnError = sqlDefaultExpirationIntervalOnError }()

	sl := testDefaultStoreLimits
	sl.MaxAge = 100 * time.Millisecond
	s.SetLimits(&sl)

	cs := storeCreateChannel(t, s, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg"))

	failDBConnection(t, s)

	// Wait for past expiration
	time.Sleep(120 * time.Millisecond)

	// Restore connection
	restoreDBConnection(t, s)

	// If message is gone, we are done
	if n, _ := msgStoreState(t, cs.Msgs); n == 0 {
		return
	}
	time.Sleep(120 * time.Millisecond)
	// Message should be gone
	if n, _ := msgStoreState(t, cs.Msgs); n != 0 {
		t.Fatal("Message should have been expired")
	}
}

func TestSQLRandomFailureDuringStore(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	goodCount := make(chan int, 1)
	go func() {
		seq := uint64(1)
		for count := 0; ; count++ {
			if _, err := cs.Msgs.Store(&pb.MsgProto{
				Sequence:  seq,
				Data:      []byte("hello"),
				Subject:   "foo",
				Timestamp: time.Now().UnixNano(),
			}); err != nil {
				goodCount <- count
				return
			}
			seq++
		}
	}()
	time.Sleep(time.Duration(rand.Intn(400)+100) * time.Millisecond)
	failDBConnection(t, s)
	count := <-goodCount
	restoreDBConnection(t, s)
	for i := uint64(1); i < uint64(count); i++ {
		if m, err := cs.Msgs.Lookup(i); err != nil || m == nil || m.Sequence != i {
			t.Fatalf("Unexpected seq or error for message %v: %v - %v", i, m.Sequence, err)
		}
	}
}

func TestSQLUpdateNow(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	now := atomic.LoadInt64(&s.nowInNano)

	time.Sleep(1250 * time.Millisecond)

	newNow := atomic.LoadInt64(&s.nowInNano)
	if newNow == now {
		t.Fatalf("Looks like nowInNano was not updated")
	}
}

func TestSQLCloseOnMsgExpiration(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	limits := testDefaultStoreLimits
	limits.MaxChannels = 1000
	limits.MaxAge = 500 * time.Millisecond
	s.SetLimits(&limits)

	beforeSend := time.Now()
	for i := 0; i < limits.MaxChannels; i++ {
		cname := fmt.Sprintf("foo.%d", i)
		cs := storeCreateChannel(t, s, cname)
		storeMsg(t, cs, cname, uint64(i+1), []byte("hello"))
	}
	durSend := time.Since(beforeSend)
	time.Sleep(limits.MaxAge - durSend)

	// The close should break out of expiration process
	beforeClose := time.Now()
	s.Close()
	durClose := time.Since(beforeClose)
	// It should not take too long to close
	if durClose >= time.Second {
		t.Fatalf("Took too long to close the store")
	}
}

func TestSQLExpireMsgsForChannelsWithDifferentMaxAge(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	limits := testDefaultStoreLimits
	limits.AddPerChannel("foo", &ChannelLimits{MsgStoreLimits: MsgStoreLimits{MaxAge: 500 * time.Millisecond}})
	limits.AddPerChannel("bar", &ChannelLimits{MsgStoreLimits: MsgStoreLimits{MaxAge: 15 * time.Millisecond}})
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", err)
	}

	fooCS := storeCreateChannel(t, s, "foo")
	barCS := storeCreateChannel(t, s, "bar")

	// First, store message in the channel with the highest max age
	storeMsg(t, fooCS, "foo", 1, []byte("foo"))
	// Then in the store with the loweest
	storeMsg(t, barCS, "bar", 1, []byte("bar"))

	// Wait for message in bar to expire
	time.Sleep(30 * time.Millisecond)
	// It should have expired
	if n, _ := msgStoreState(t, barCS.Msgs); n != 0 {
		t.Fatalf("Should have no message, got %v", n)
	}
	// And still be a message in foo
	if n, _ := msgStoreState(t, fooCS.Msgs); n != 1 {
		t.Fatalf("Should have 1 message, got %v", n)
	}
	// Wait for message on bar to expire
	time.Sleep(600 * time.Millisecond)
	if n, _ := msgStoreState(t, fooCS.Msgs); n != 0 {
		t.Fatalf("Should have no message, got %v", n)
	}
}

func TestSQLExpireMsgsOnRecovery(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	limits := testDefaultStoreLimits
	limits.MaxAge = 250 * time.Millisecond
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", err)
	}
	cs := storeCreateChannel(t, s, "foo")
	storeMsg(t, cs, "foo", 1, []byte("hello"))
	// Close the store before msg expires.
	s.Close()
	// Sleep for longer than the max age
	time.Sleep(300 * time.Millisecond)
	// Re-open the store
	s, state := openDefaultSQLStoreWithLimits(t, &limits)
	defer s.Close()
	if len(state.Channels) != 1 {
		t.Fatalf("1 channel should have been recovered, got %v", len(state.Channels))
	}
	rc := state.Channels["foo"]
	// Message should have expired right away
	if n, _ := msgStoreState(t, rc.Channel.Msgs); n != 0 {
		t.Fatalf("Messages hould have expired on recovery")
	}
}

func TestSQLExpiredMsgsOnLookup(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	// Artificially change the SQL request that is supposed to find the
	// first message to expire to find nothing so that we verify that
	// a message that is supposed to be expired is not returned by
	// the Lookup
	realStmt1 := sqlStmts[sqlGetExpiredMessages]
	realStmt2 := sqlStmts[sqlGetFirstMsgTimestamp]
	defer func() {
		sqlStmts[sqlGetExpiredMessages] = realStmt1
		sqlStmts[sqlGetFirstMsgTimestamp] = realStmt2
		sqlTimeTickInterval = sqlDefaultTimeTickInterval
	}()

	// Dummy statement that will return an error
	sqlStmts[sqlGetExpiredMessages] = "SELECT 0, 0, 0 FROM Messages WHERE id=? AND timestamp<=?"
	sqlStmts[sqlGetFirstMsgTimestamp] = "SELECT 0 FROM Messages WHERE id=? AND seq>=? LIMIT 1"
	initSQLStmtsTable(testSQLDriver)
	sqlTimeTickInterval = 15 * time.Millisecond

	s := createDefaultSQLStore(t)
	defer s.Close()

	limits := testDefaultStoreLimits
	limits.MaxAge = 50 * time.Millisecond
	s.SetLimits(&limits)

	cs := storeCreateChannel(t, s, "foo")
	m := storeMsg(t, cs, "foo", 1, []byte("hello"))

	time.Sleep(150 * time.Millisecond)

	beforeLookup := time.Now().UnixNano()
	nm, _ := cs.Msgs.Lookup(m.Sequence)
	if nm != nil {
		t.Fatalf("Message should have expired about %v ago, but still got the message",
			time.Duration(beforeLookup-(m.Timestamp+int64(50*time.Millisecond))))
	}
}

func TestSQLDeleteLastSubKeepRecord(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")

	sub := &spb.SubState{}
	if err := cs.Subs.CreateSub(sub); err != nil {
		t.Fatalf("Error on create sub: %v", err)
	}
	if err := cs.Subs.DeleteSub(sub.ID); err != nil {
		t.Fatalf("Error on delete sub: %v", err)
	}

	db := getDBConnection(t)
	defer db.Close()
	r := db.QueryRow("SELECT deleted FROM Subscriptions WHERE id=1 AND subid=1")
	deleted := sql.NullBool{}
	if err := r.Scan(&deleted); err != nil {
		t.Fatalf("Error on Scan: %v", err)
	}
	if !deleted.Valid {
		t.Fatal("Deleted flag not found")
	}
	if !deleted.Bool {
		t.Fatalf("Deleted flag should have been set to true")
	}
}

func TestSQLRecoverErrors(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	db := getDBConnection(t)
	defer db.Close()
	// Change version
	test.MustExecuteSQL(t, db, "UPDATE ServerInfo SET version=2 WHERE uniquerow=1")

	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	expectRecoverFailure := func(errTxt string) {
		state, err := s.Recover()
		if state != nil || err == nil || !strings.Contains(err.Error(), errTxt) {
			t.Fatalf("Expected no state and error about %q, got %v - %v", errTxt, state, err)
		}
	}
	expectRecoverFailure("version")

	// Reset to proper version but change name of cluster
	test.MustExecuteSQL(t, db,
		fmt.Sprintf("UPDATE ServerInfo SET id='%s', version=%d WHERE uniquerow=1",
			"not-same-than-proto", sqlVersion))
	expectRecoverFailure("match")
}

func TestSQLPurgeSubsPending(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	subID := storeSub(t, cs, "foo")
	smallestThatShouldBeRecovered := uint64(0)
	for i := 0; i < 15; i++ {
		m := storeMsg(t, cs, "foo", uint64(i+1), []byte("msg"))
		if i == 10 {
			smallestThatShouldBeRecovered = m.Sequence
		}
		storeSubPending(t, cs, "foo", subID, m.Sequence)
	}
	s.Close()
	// Set some limit to discard 10 out of the 15 messages
	limits := testDefaultStoreLimits
	limits.MaxMsgs = 5
	s, state := openDefaultSQLStoreWithLimits(t, &limits)
	defer s.Close()
	rsubs := getRecoveredSubs(t, state, "foo", 1)
	rs := rsubs[0]
	if len(rs.Pending) != 5 {
		t.Fatalf("Expected only 5 pending messages, got %v", len(rs.Pending))
	}
	smallest := uint64(0x7FFFFFFFFFFFFFFF)
	for seq := range rs.Pending {
		if seq < smallest {
			smallest = seq
		}
	}
	if smallest != smallestThatShouldBeRecovered {
		t.Fatalf("Expected %v to be first, got %v", smallestThatShouldBeRecovered, smallest)
	}
}

func TestSQLNoCachingOption(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 5
	limits.MaxAge = 500 * time.Millisecond
	// Create a store with caching enabled (which is default, but invoke option here)
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	// Create a channel
	cs := storeCreateChannel(t, s, "foo")

	// Store some messages (more than what is allowed so we test
	// applying limits with caching).
	msg := []byte("hello")
	seq := uint64(1)
	for i := 0; i < 10; i++ {
		storeMsg(t, cs, "foo", seq, msg)
		seq++
	}

	// It should not be in database unless we call flush.
	db := getDBConnection(t)
	defer db.Close()
	checkDB := func(expected int) {
		r := db.QueryRow("SELECT COUNT(*) FROM Messages")
		nr := 0
		if err := r.Scan(&nr); err != nil {
			t.Fatalf("Error during scan: %v", err)
		}
		if nr != expected {
			t.Fatalf("Expected %d records, got %v", expected, nr)
		}
	}
	checkDB(0)

	// Lookup should work, getting it from internal cache
	totalSize := 0
	for i := 6; i <= 10; i++ {
		m := msgStoreLookup(t, cs.Msgs, uint64(i))
		if !reflect.DeepEqual(m.Data, msg) {
			t.Fatalf("Unexpected message content: %v", m)
		}
		totalSize += m.Size()
	}

	// Check store stats
	count, bytes := msgStoreState(t, cs.Msgs)
	if count != limits.MaxMsgs || bytes != uint64(totalSize) {
		t.Fatalf("Unexpected count/size, expected %v - %v, got %v - %v", limits.MaxMsgs, totalSize, count, bytes)
	}

	// Flush to database
	if err := cs.Msgs.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	// Should find it in the database
	checkDB(5)

	// Send more messages
	for i := 0; i < 10; i++ {
		storeMsg(t, cs, "foo", seq, msg)
		seq++
	}
	totalSize = 0
	for i := 16; i <= 20; i++ {
		m := msgStoreLookup(t, cs.Msgs, uint64(i))
		if !reflect.DeepEqual(m.Data, msg) {
			t.Fatalf("Unexpected message content: %v", m)
		}
		totalSize += m.Size()
	}
	count, bytes = msgStoreState(t, cs.Msgs)
	if count != limits.MaxMsgs || bytes != uint64(totalSize) {
		t.Fatalf("Unexpected count/size, expected %v - %v, got %v - %v", limits.MaxMsgs, totalSize, count, bytes)
	}
	// Flush to database
	if err := cs.Msgs.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	// Should find it in the database
	checkDB(5)

	// Wait for messages to expire
	time.Sleep(limits.MaxAge + 100*time.Millisecond)
	// There should be no more message
	count, bytes = msgStoreState(t, cs.Msgs)
	if count != 0 || bytes != 0 {
		t.Fatalf("There should be no message, got %v - %v", count, bytes)
	}
	checkDB(0)
}

func TestSQLCachingWithExpirationOnClose(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxAge = 5 * time.Second
	// Create a store with caching enabled (which is default, but invoke option here)
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	m := storeMsg(t, cs, "foo", 1, []byte("msg"))
	doneCh := make(chan struct{}, 1)
	go func() {
		s.Close()
		doneCh <- struct{}{}
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Store Close() hanged?")
	}
	db := getDBConnection(t)
	defer db.Close()
	r := db.QueryRow("SELECT seq FROM Messages WHERE id=1")
	seq := uint64(0)
	if err := r.Scan(&seq); err != nil {
		t.Fatalf("Error getting sequence: %v", err)
	}
	if seq != m.Sequence {
		t.Fatalf("Expected sequence %v, got %v", m.Sequence, seq)
	}
}

func testSQLCheckPendingOrAcksRow(t tLogger, db *sql.DB, subID uint64, columnName string, expected ...uint64) {
	rows, err := db.Query(fmt.Sprintf("SELECT %s FROM SubsPending WHERE subid=%v", columnName, subID))
	if err != nil {
		stackFatalf(t, "Error getting rows: %v", err)
	}
	defer rows.Close()
	m := make(map[uint64]struct{})
	for rows.Next() {
		var (
			bytes []byte
		)
		if err := rows.Scan(&bytes); err != nil {
			stackFatalf(t, "Error getting bytes: %v", err)
		}
		sqlDecodeSeqs(bytes, func(seq uint64) {
			m[seq] = struct{}{}
		})
	}
	rows.Close()
	if len(expected) != len(m) {
		stackFatalf(t, "Expected %v elements, got %v", len(expected), len(m))
	}
	for _, seq := range expected {
		if _, exists := m[seq]; !exists {
			stackFatalf(t, "Expected %v to be in %s, it was not", seq, columnName)
		}
	}
}

func testSQLCheckPendingRow(t tLogger, db *sql.DB, subID uint64, expectedPending ...uint64) {
	testSQLCheckPendingOrAcksRow(t, db, subID, "pending", expectedPending...)
}

func testSQLCheckAckRow(t tLogger, db *sql.DB, subID uint64, expectedPending ...uint64) {
	testSQLCheckPendingOrAcksRow(t, db, subID, "acks", expectedPending...)
}
func testSQLCheckSubLastSent(t tLogger, db *sql.DB, subID uint64, expected uint64) {
	r := db.QueryRow(fmt.Sprintf("SELECT lastsent FROM Subscriptions WHERE subid=%v", subID))
	lastSent := uint64(0)
	if err := r.Scan(&lastSent); err != nil {
		stackFatalf(t, "Error getting lastSent: %v", err)
	}
	if lastSent != expected {
		stackFatalf(t, "Expected lastSent to be %v, got %v", expected, lastSent)
	}
}

func TestSQLSubStoreCaching(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	sqlMaxPendingAcks = 10
	// "disable" timer based flusing for now
	sqlSubStoreFlushInterval = time.Hour
	defer func() {
		sqlMaxPendingAcks = sqlDefaultMaxPendingAcks
		sqlSubStoreFlushInterval = sqlDefaultSubStoreFlushInterval
	}()

	// Tests are by default not using caching, so this
	// test has to be explicit.
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	channel := "foo"
	cs := storeCreateChannel(t, s, channel)

	subID := storeSub(t, cs, channel)
	storeSubPending(t, cs, channel, subID, 1, 2)
	storeSubFlush(t, cs, channel)

	db := getDBConnection(t)
	defer db.Close()
	testSQLCheckPendingRow(t, db, subID, 1, 2)
	storeSubAck(t, cs, channel, subID, 1)
	storeSubFlush(t, cs, channel)
	// The two pending messages should still be present
	testSQLCheckPendingRow(t, db, subID, 1, 2)
	// Ack the last one
	storeSubAck(t, cs, channel, subID, 2)
	storeSubFlush(t, cs, channel)
	// There should not be any pending now
	testSQLCheckPendingRow(t, db, subID)
	// Check auto-flush
	seqs := make([]uint64, 0, sqlMaxPendingAcks)
	for i := 0; i < sqlMaxPendingAcks; i++ {
		seqs = append(seqs, uint64(3+i))
	}
	storeSubPending(t, cs, channel, subID, seqs...)
	testSQLCheckPendingRow(t, db, subID, seqs...)
	// Add a new row with the some more sequences and manually flush them
	nextSeq := seqs[sqlMaxPendingAcks-1] + 1
	storeSubPending(t, cs, channel, subID, nextSeq, nextSeq+1)
	storeSubFlush(t, cs, channel)
	// Ack the max number of elements but make sure
	// we are not deleting completely the first row
	storeSubAck(t, cs, channel, subID, seqs[0:sqlMaxPendingAcks-1]...)
	storeSubAck(t, cs, channel, subID, nextSeq)
	// Ack the missing ones
	storeSubAck(t, cs, channel, subID, seqs[sqlMaxPendingAcks-1], nextSeq+1)
	// They should be all gone now
	testSQLCheckPendingRow(t, db, subID)
	// Add more pending and flush them out
	storeSubPending(t, cs, channel, subID, nextSeq+2, nextSeq+3)
	storeSubFlush(t, cs, channel)
	testSQLCheckPendingRow(t, db, subID, nextSeq+2, nextSeq+3)
	// Deleting the subscription should delete any pending
	storeSubDelete(t, cs, channel, subID)
	testSQLCheckPendingRow(t, db, subID)

	// Start with fresh subscription
	subID = storeSub(t, cs, channel)
	// Check that if pending is followed by ack within same flush
	// nothing needs to be persisted.
	storeSubPending(t, cs, channel, subID, 1, 2)
	storeSubAck(t, cs, channel, subID, 1, 2)
	storeSubFlush(t, cs, channel)
	testSQLCheckPendingRow(t, db, subID)
	// However, the subscription's lastSent should have been updated
	testSQLCheckSubLastSent(t, db, subID, 2)

	storeSubPending(t, cs, channel, subID, 3, 4)
	storeSubFlush(t, cs, channel)
	storeSubAck(t, cs, channel, subID, 3)
	storeSubFlush(t, cs, channel)
	storeSubAck(t, cs, channel, subID, 4)
	storeSubFlush(t, cs, channel)
	storeSubPending(t, cs, channel, subID, 5)
	storeSubAck(t, cs, channel, subID, 5)
	storeSubFlush(t, cs, channel)
	testSQLCheckPendingRow(t, db, subID)
	testSQLCheckSubLastSent(t, db, subID, 5)
	// In case we were to store an ack for a sequence that
	// we don't have, lastSent should still be updated properly.
	storeSubAck(t, cs, channel, subID, 6)
	storeSubFlush(t, cs, channel)
	testSQLCheckSubLastSent(t, db, subID, 6)
	// Delete sub
	storeSubDelete(t, cs, channel, subID)
	testSQLCheckPendingRow(t, db, subID)
}

func TestSQLSubStoreCachingFlushInterval(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	// set the flush timer to low value for this test
	sqlSubStoreFlushInterval = 15 * time.Millisecond
	defer func() {
		sqlSubStoreFlushInterval = sqlDefaultSubStoreFlushInterval
	}()

	// Tests are by default not using caching, so this
	// test has to be explicit.
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	db := getDBConnection(t)
	defer db.Close()

	channel := "foo"
	cs := storeCreateChannel(t, s, channel)

	subID := storeSub(t, cs, channel)
	storeSubPending(t, cs, channel, subID, 1, 2)
	time.Sleep(3 * sqlSubStoreFlushInterval)
	testSQLCheckPendingRow(t, db, subID, 1, 2)

	storeSubAck(t, cs, channel, subID, 1)
	time.Sleep(3 * sqlSubStoreFlushInterval)
	testSQLCheckAckRow(t, db, subID, 1)
}

func TestSQLSubStoreCachingAndRecovery(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	// "disable" timer based flusing for now
	sqlSubStoreFlushInterval = time.Hour
	defer func() {
		sqlSubStoreFlushInterval = sqlDefaultSubStoreFlushInterval
	}()

	// Tests are by default not using caching, so this
	// test has to be explicit.
	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()
	info := testDefaultServerInfo
	if err := s.Init(&info); err != nil {
		t.Fatalf("Unable to init store: %v", err)
	}

	channel := "foo"
	cs := storeCreateChannel(t, s, channel)
	for i := 0; i < 2; i++ {
		storeMsg(t, cs, channel, uint64(i+1), []byte("msg"))
	}

	subID := storeSub(t, cs, channel)
	storeSubPending(t, cs, channel, subID, 1, 2)
	storeSubFlush(t, cs, channel)
	storeSubAck(t, cs, channel, subID, 1)
	// Store should be flushed on close
	if err := s.Close(); err != nil {
		t.Fatalf("Error closing store: %v", err)
	}

	db := getDBConnection(t)
	defer db.Close()
	testSQLCheckPendingRow(t, db, subID, 1, 2)

	// Restart the store
	s, err = NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()
	state, err := s.Recover()
	if err != nil {
		t.Fatalf("Error recovering state: %v", err)
	}
	cs = getRecoveredChannel(t, state, channel)
	subs := getRecoveredSubs(t, state, channel, 1)
	sub := subs[0]
	if len(sub.Pending) != 1 {
		t.Fatalf("Expected 1 pending, got %v", len(sub.Pending))
	}
	if _, exists := sub.Pending[2]; !exists {
		t.Fatal("Expected seq=2 to be pending, was not")
	}
	if sub.Sub.LastSent != 2 {
		t.Fatalf("Expected sub's lastSent to be 2, got %v", sub.Sub.LastSent)
	}

	// Make sure we still have the pending as before
	testSQLCheckPendingRow(t, db, subID, 1, 2)
	// Check that what we have in memory after restart is correct
	ss := cs.Subs.(*SQLSubStore)
	ss.Lock()
	ap := ss.cache.subs[subID]
	if ap.lastSent != 2 {
		ss.Unlock()
		t.Fatalf("Expected lastSent to be 2, got %v", ap.lastSent)
	}
	if ap.prevLastSent != ap.lastSent {
		ss.Unlock()
		t.Fatalf("Expected prevLastSent to be same than lastSent, got %v, %v", ap.prevLastSent, ap.lastSent)
	}
	if len(ap.msgToRow) != 1 || ap.msgToRow[2] == nil {
		ss.Unlock()
		t.Fatalf("Expected seq 2 to be in msgToRow, got %v", ap.msgToRow)
	}
	if len(ap.ackToRow) != 1 || ap.ackToRow[1] == nil {
		ss.Unlock()
		t.Fatalf("Expected seq 1 to be in ackToRow, got %v", ap.ackToRow)
	}
	row := ap.msgToRow[2]
	if row == nil || len(row.msgs) != 2 {
		ss.Unlock()
		t.Fatalf("Expected row to have 1 and 2, got %v", row.msgs)
	}
	if row.msgsRefs != 1 || row.acksRefs != 0 {
		ss.Unlock()
		t.Fatalf("Expected row pending refs to be 1, acksRef to be 0, got %v and %v", row.msgsRefs, row.acksRefs)
	}

	row = ap.ackToRow[1]
	if row == nil || len(row.msgs) != 0 {
		ss.Unlock()
		t.Fatalf("Expected row to have no message, got %v", row.msgs)
	}
	if row.msgsRefs != 0 || row.acksRefs != 1 {
		ss.Unlock()
		t.Fatalf("Expected row pending refs to be 0, acksRef to be 1, got %v and %v", row.msgsRefs, row.acksRefs)
	}

	if len(ap.msgs) != 0 || len(ap.acks) != 0 {
		ss.Unlock()
		t.Fatalf("ap.msgs and ap.acks should be empty on recovery, got %v and %v", ap.msgs, ap.acks)
	}
	rowID := row.ID
	ss.Unlock()

	// Normally, when a row has no more pending and ack references,
	// the row is deleted. For instance, ack'ing seq 2 here would
	// cause all rows to be deleted. So to check the case where the
	// deletion would not occur at runtime, but those "empty" rows
	// are recovered, add the ACK for 2 after the store was closed.
	s.Close()
	acks := make(map[uint64]struct{})
	acks[2] = struct{}{}
	ackBytes, _ := sqlEncodeSeqs(acks, func(_ uint64) {})
	stmt := "INSERT INTO SubsPending (subid, row, lastsent, pending, acks) VALUES (?, ?, ?, ?, ?)"
	if testSQLDriver == driverPostgres {
		stmt = "INSERT INTO SubsPending (subid, row, lastsent, pending, acks) VALUES ($1, $2, $3, $4, $5)"
	}
	if _, err := db.Exec(stmt, subID, rowID+1, 0, nil, ackBytes); err != nil {
		t.Fatalf("Error inserting into subspending: %v", err)
	}
	// Restart the store
	s, err = NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(false))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()
	state, err = s.Recover()
	if err != nil {
		t.Fatalf("Error recovering state: %v", err)
	}
	getRecoveredChannel(t, state, channel)
	subs = getRecoveredSubs(t, state, channel, 1)
	sub = subs[0]
	if len(sub.Pending) != 0 {
		t.Fatalf("Expected 0 pending, got %v", len(sub.Pending))
	}
	if sub.Sub.LastSent != 2 {
		t.Fatalf("Expected sub's lastSent to be 2, got %v", sub.Sub.LastSent)
	}
	// Verify that rows have been removed on recovery.
	testSQLCheckPendingRow(t, db, subID)
}

func TestSQLMaxConnections(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxChannels = 500

	s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits, SQLNoCaching(true), SQLMaxOpenConns(10))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	var css []*Channel
	for i := 0; i < limits.MaxChannels; i++ {
		channel := fmt.Sprintf("foo.%d", i)
		cs := storeCreateChannel(t, s, channel)
		storeMsg(t, cs, channel, uint64(i+1), []byte("msg"))
		css = append(css, cs)
	}

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(len(css))
	for _, cs := range css {
		go func(cs *Channel) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := cs.Msgs.Lookup(1)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}(cs)
	}
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatalf("Error: %v", e)
	default:
	}
}

type getExclusiveLockLogger struct {
	sync.Mutex
	msgs []string
}

func (d *getExclusiveLockLogger) log(format string, args ...interface{}) {
	d.Lock()
	d.msgs = append(d.msgs, fmt.Sprintf(format, args...))
	d.Unlock()
}

func (d *getExclusiveLockLogger) Noticef(format string, args ...interface{}) {}
func (d *getExclusiveLockLogger) Debugf(format string, args ...interface{})  {}
func (d *getExclusiveLockLogger) Tracef(format string, args ...interface{})  {}
func (d *getExclusiveLockLogger) Errorf(format string, args ...interface{})  { d.log(format, args...) }
func (d *getExclusiveLockLogger) Fatalf(format string, args ...interface{})  { d.log(format, args...) }

func TestSQLGetExclusiveLock(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	sqlLockUpdateInterval = 250 * time.Millisecond
	sqlLockLostCount = 2
	sqlNoPanic = true
	defer func() {
		sqlLockUpdateInterval = sqlDefaultLockUpdateInterval
		sqlLockLostCount = sqlDefaultLockLostCount
		sqlNoPanic = false
	}()

	s1 := createDefaultSQLStore(t)
	defer s1.Close()

	// We should be able to acquire the lock, and calling GetExclusiveLock
	// while we have the lock should still return true and no error
	for i := 0; i < 2; i++ {
		hasLock, err := s1.GetExclusiveLock()
		if !hasLock || err != nil {
			t.Fatalf("Expected lock to be acquired, got %v, %v", hasLock, err)
		}
	}

	dl := &getExclusiveLockLogger{}
	s2, err := NewSQLStore(dl, testSQLDriver, testSQLSource, nil, SQLNoCaching(true))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s2.Close()

	hasLock, err := s2.GetExclusiveLock()
	if hasLock || err != nil {
		t.Fatalf("Expected lock to fail, got %v, %v", hasLock, err)
	}

	// Closing store should release the lock immediately.
	s1.Close()

	// This should return false, no error
	hasLock, err = s1.GetExclusiveLock()
	if hasLock || err != nil {
		t.Fatalf("Expected lock to fail, got %v, %v", hasLock, err)
	}

	// This should succeed
	hasLock, err = s2.GetExclusiveLock()
	if !hasLock || err != nil {
		t.Fatalf("Expected lock to be acquired, got %v, %v", hasLock, err)
	}

	// Create a 3rd store
	s3, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(true))
	if err != nil {
		t.Fatalf("Unable to create store: %v", err)
	}
	defer s3.Close()

	// Try to get the lock for s3 once, so that it keeps track of current
	// owner.
	hasLock, err = s3.GetExclusiveLock()
	if hasLock || err != nil {
		t.Fatalf("Expected lock to fail, got %v, %v", hasLock, err)
	}

	// Make it so that s2 stops updating the lock table
	failDBConnection(t, s2)

	// This call will take some time (2 times the update interval) but it
	// should then succeed.
	start := time.Now()
	hasLock, err = s3.GetExclusiveLock()
	if !hasLock || err != nil {
		t.Fatalf("Expected lock to be acquired, got %v, %v", hasLock, err)
	}
	dur := time.Since(start)

	maxTime := time.Duration(float64(sqlLockLostCount)*(1.5*float64(sqlLockUpdateInterval))) + 100*time.Millisecond
	if dur > maxTime {
		t.Fatalf("Took too long to acquire lock: %v", dur)
	}

	// Check s2 log. It should have complained about updating lock (2 times)
	// and then says that it aborts.
	dl.Lock()
	good := 0
	for i, m := range dl.msgs {
		if i < 2 {
			if strings.Contains(m, "Unable to update store lock") {
				good++
			}
		} else if m == "Aborting" {
			good++
		}
	}
	dl.Unlock()
	if good != 3 {
		t.Fatalf("Unexpected errors in log: %v", dl.msgs)
	}

	restoreDBConnection(t, s2)
	s2.Close()
	s3.Close()

	// All stores have been shutdown
	s4, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(true))
	if err != nil {
		t.Fatalf("Unable to create store: %v", err)
	}
	defer s4.Close()
	// Fail DB
	failDBConnection(t, s4)
	// Trying to get lock should return error
	hasLock, err = s4.GetExclusiveLock()
	if hasLock || err == nil {
		t.Fatalf("Expected error, got lock=%v err=%v", hasLock, err)
	}
	restoreDBConnection(t, s4)
}

func TestSQLFirstSeqAfterMsgsExpireAndStoreRestart(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	limits := StoreLimits{}
	limits.MaxAge = 100 * time.Millisecond
	s.SetLimits(&limits)

	cs := storeCreateChannel(t, s, "foo")
	for i := 0; i < 3; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("msg"))
	}

	time.Sleep(200 * time.Millisecond)

	count, size := msgStoreState(t, cs.Msgs)
	if count != 0 || size != 0 {
		t.Fatalf("Unexpected count and size: %v and %v", count, size)
	}
	first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
	if first != 4 || last != 3 {
		t.Fatalf("Unexpected first and/or last: %v, %v", first, last)
	}

	// Restart the store
	s.Close()
	s, state := openDefaultSQLStoreWithLimits(t, &limits)
	defer s.Close()

	cs = getRecoveredChannel(t, state, "foo")
	count, size = msgStoreState(t, cs.Msgs)
	if count != 0 || size != 0 {
		t.Fatalf("Unexpected count and size: %v and %v", count, size)
	}
	first, last = msgStoreFirstAndLastSequence(t, cs.Msgs)
	if first != 4 || last != 3 {
		t.Fatalf("Unexpected first and/or last: %v, %v", first, last)
	}
}

func TestSQLMsgStoreEmpty(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	defer cleanupSQLDatastore(t)

	cachingModes := []bool{true, false}
	for _, caching := range cachingModes {
		cleanupSQLDatastore(t)

		s, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, nil, SQLNoCaching(caching))
		if err != nil {
			t.Fatalf("Error creating sql store: %v", err)
		}
		defer s.Close()

		limits := StoreLimits{}
		limits.MaxAge = 250 * time.Millisecond
		if err := s.SetLimits(&limits); err != nil {
			t.Fatalf("Error setting limits: %v", err)
		}

		cs := storeCreateChannel(t, s, "foo")
		storeMsg(t, cs, "foo", 1, []byte("hello"))
		// Wait for this to expire
		time.Sleep(300 * time.Millisecond)

		// Send more messages
		for i := 0; i < 3; i++ {
			storeMsg(t, cs, "foo", uint64(i+2), []byte("hello"))
		}
		// Then empty the message store
		if err := cs.Msgs.Empty(); err != nil {
			t.Fatalf("Error on Empty(): %v", err)
		}

		ms := cs.Msgs.(*SQLMsgStore)
		ms.RLock()
		if ms.writeCache != nil && (ms.writeCache.head != nil || ms.writeCache.tail != nil) {
			ms.RUnlock()
			t.Fatal("WriteCache not empty")
		}
		if ms.expireTimer != nil {
			ms.RUnlock()
			t.Fatal("ExpireTimer not nil")
		}
		if ms.first != 0 || ms.last != 0 {
			ms.RUnlock()
			t.Fatalf("First and/or Last not reset")
		}
		channelID := ms.channelID
		ms.RUnlock()

		db := getDBConnection(t)
		defer db.Close()
		maxSeq := uint64(0)
		r := db.QueryRow(fmt.Sprintf("SELECT maxseq FROM Channels WHERE id=%v", channelID))
		if err := r.Scan(&maxSeq); err != nil {
			t.Fatalf("Error on scan: %v", err)
		}
		if maxSeq != 0 {
			t.Fatalf("MaxSeq should have been reset, got %v", maxSeq)
		}
		s.Close()
	}
}

func TestSQLDeleteChannel(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	checkTables := func(channelName string, shouldExist bool) {
		db := getDBConnection(t)
		defer db.Close()

		var cid uint64
		row := db.QueryRow(fmt.Sprintf("SELECT id FROM Channels WHERE name='%s'", channelName))
		row.Scan(&cid)
		if shouldExist && cid == 0 {
			stackFatalf(t, "Channel %q should exist", channelName)
		} else if !shouldExist && cid != 0 {
			stackFatalf(t, "Channel %q should not exist", channelName)
		}
		var subid uint64
		row = db.QueryRow(fmt.Sprintf("SELECT subid FROM Subscriptions WHERE id=%v", cid))
		row.Scan(&subid)
		if shouldExist && subid == 0 {
			stackFatalf(t, "Channel %q should have a subscription", channelName)
		} else if !shouldExist && subid != 0 {
			stackFatalf(t, "Channel %q should not have a subscription", channelName)
		}
		var count int
		row = db.QueryRow(fmt.Sprintf("SELECT COUNT(seq) FROM Messages WHERE id=%v", cid))
		row.Scan(&count)
		if shouldExist && count == 0 {
			stackFatalf(t, "There should be messages for channel %q", channelName)
		} else if !shouldExist && count > 0 {
			stackFatalf(t, "There should be no message for channel %q", channelName)
		}
		row = db.QueryRow(fmt.Sprintf("SELECT COUNT(row) FROM SubsPending WHERE subid=%v", subid))
		row.Scan(&count)
		if shouldExist && count == 0 {
			stackFatalf(t, "There should be subspending for channel %q", channelName)
		} else if !shouldExist && count > 0 {
			stackFatalf(t, "There should be no subspending for channel %q", channelName)
		}
	}

	fooChannel := storeCreateChannel(t, s, "foo")
	// Add some messages, subscriptions entries, etc...
	m1 := storeMsg(t, fooChannel, "foo", 1, []byte("msg1"))
	m2 := storeMsg(t, fooChannel, "foo", 2, []byte("msg2"))
	subID1 := storeSub(t, fooChannel, "foo")
	storeSubPending(t, fooChannel, "foo", subID1, m1.Sequence, m2.Sequence)
	checkTables("foo", true)

	barChannel := storeCreateChannel(t, s, "bar")
	// Add some messages, subscriptions entries, etc...
	m1 = storeMsg(t, barChannel, "bar", 1, []byte("msg1"))
	m2 = storeMsg(t, barChannel, "bar", 2, []byte("msg2"))
	subID2 := storeSub(t, barChannel, "bar")
	storeSubPending(t, barChannel, "bar", subID2, m1.Sequence, m2.Sequence)
	checkTables("bar", true)

	if err := s.DeleteChannel("foo"); err != nil {
		t.Fatalf("Error deleting channel: %v", err)
	}
	// Close store to ensure deep delete of channel
	s.Close()
	checkTables("foo", false)
	checkTables("bar", true)
}

func TestSQLRecoverWithMaxBytes(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	s := createDefaultSQLStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	payload := make([]byte, 100)
	storeMsg(t, cs, "foo", 1, payload)
	storeMsg(t, cs, "foo", 2, payload)
	storeMsg(t, cs, "foo", 3, payload)
	storeMsg(t, cs, "foo", 4, payload)
	storeMsg(t, cs, "foo", 5, payload)
	s.Close()

	limits := testDefaultStoreLimits
	// Since the message is a bit more than the payload, with this
	// value we should be keeping the 3 last ones.
	limits.MaxBytes = 350
	s, state := openDefaultSQLStoreWithLimits(t, &limits)
	defer s.Close()
	cs = state.Channels["foo"].Channel
	first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
	if first != 3 && last != 5 {
		t.Fatalf("Should be left with 3..5, got %v..%v", first, last)
	}
	s.Close()

	limits.MaxBytes = 10
	s, state = openDefaultSQLStoreWithLimits(t, &limits)
	defer s.Close()
	cs = state.Channels["foo"].Channel
	if n, _ := msgStoreState(t, cs.Msgs); n != 1 {
		t.Fatalf("Should have left the last message")
	}
	s.Close()
}
