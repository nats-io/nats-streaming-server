// Copyright 2017 Apcera Inc. All rights reserved.

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

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/test"
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
	storeMsg(t, cs, "foo", []byte("msg"))
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
	expectToFail(func() error {
		_, err := s.AddClient("me", "hbInbox")
		return err
	})
	expectToFail(func() error {
		_, err := s.Recover()
		return err
	})
	expectToFail(func() error { return s.DeleteClient("me") })
	expectToFail(func() error {
		_, err := cs.Msgs.Store([]byte("hello"))
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
	storeMsg(t, cs, "foo", []byte("msg"))

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
		for count := 0; ; count++ {
			if _, err := cs.Msgs.Store([]byte("hello")); err != nil {
				goodCount <- count
				return
			}
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
		storeMsg(t, cs, cname, []byte("hello"))
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
	storeMsg(t, fooCS, "foo", []byte("foo"))
	// Then in the store with the loweest
	storeMsg(t, barCS, "bar", []byte("bar"))

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
	storeMsg(t, cs, "foo", []byte("hello"))
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
	m := storeMsg(t, cs, "foo", []byte("hello"))

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
		m := storeMsg(t, cs, "foo", []byte("msg"))
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
	for i := 0; i < 10; i++ {
		if _, err := cs.Msgs.Store(msg); err != nil {
			t.Fatalf("Error storing message: %v", err)
		}
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
		if _, err := cs.Msgs.Store(msg); err != nil {
			t.Fatalf("Error storing message: %v", err)
		}
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
	m := storeMsg(t, cs, "foo", []byte("msg"))
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
		storeMsg(t, cs, channel, []byte("msg"))
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
		storeMsg(t, cs, channel, []byte("msg"))
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
