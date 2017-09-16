// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
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
	ss, err := NewSQLStore(testLogger, driver, source, limits)
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
	ss.expireMu.Lock()
	db, err := sql.Open(testSQLDriver, testSQLSource)
	if err == nil {
		ss.db = db
		for _, c := range ss.channels {
			ms := c.Msgs.(*SQLMsgStore)
			ms.Lock()
			ms.db = db
			ms.Unlock()
			subs := c.Subs.(*SQLSubStore)
			subs.Lock()
			subs.db = db
			subs.Unlock()
		}
	}
	ss.expireMu.Unlock()
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

	sqlExpirationWaitTimeOnError = 15 * time.Millisecond
	defer func() { sqlExpirationWaitTimeOnError = sqlDefaultExpirationWaitTimeOnError }()

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
	realStmt := sqlStmts[sqlGetFirstMsgToExpire]
	defer func() {
		sqlStmts[sqlGetFirstMsgToExpire] = realStmt
		sqlTimeTickInterval = sqlDefaultTimeTickInterval
	}()

	// Dummy statement that will return no row
	sqlStmts[sqlGetFirstMsgToExpire] = "SELECT expiration FROM Messages WHERE expiration = 1"
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
