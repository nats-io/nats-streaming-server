// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
)

var (
	testSQLDriver = "mysql"
	testSQLSource = "root@/test_nats_streaming"
	// These 2 are used in order to connect to the database daemon and create
	// the test streaming database, tables, etc.. The source here should not
	// contain the database name
	testSQLSourceAdmin  = "root@/"
	testSQLDatabaseName = "test_nats_streaming"
)

func createDefaultSQLStore(t *testing.T) Store {
	limits := testDefaultStoreLimits
	ss, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits)
	if err != nil {
		stackFatalf(t, "Unexpected error: %v", err)
	}
	return ss
}

func cleanupSQLDatastore(t *testing.T) {
	db, err := sql.Open(testSQLDriver, testSQLSourceAdmin)
	if err != nil {
		stackFatalf(t, "Error cleaning up SQL datastore", err)
	}
	defer db.Close()
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + testSQLDatabaseName); err != nil {
		stackFatalf(t, "Error dropping database: %v", err)
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + testSQLDatabaseName); err != nil {
		stackFatalf(t, "Error creating database: %v", err)
	}
	if _, err = db.Exec("USE " + testSQLDatabaseName); err != nil {
		stackFatalf(t, "Error using database %q: %v", testSQLDatabaseName, err)
	}
	var sqlCreateDatabase []string
	if testSQLDriver == "mysql" {
		sqlCreateDatabase = []string{
			"CREATE TABLE IF NOT EXISTS ServerInfo (id VARCHAR(1024) PRIMARY KEY, data BLOB, version INTEGER)",
			"CREATE TABLE IF NOT EXISTS Clients (id VARCHAR(1024) PRIMARY KEY, hbinbox TEXT)",
			"CREATE TABLE IF NOT EXISTS Channels (id INTEGER PRIMARY KEY, name VARCHAR(1024) NOT NULL, deleted BOOL DEFAULT FALSE, INDEX Idx_ChannelsName (name))",
			"CREATE TABLE IF NOT EXISTS Messages (id INTEGER, seq BIGINT UNSIGNED, timestamp BIGINT, expiration BIGINT, size INTEGER, data BLOB, INDEX Idx_MsgsTimestamp (timestamp), INDEX Idx_MsgsExpiration (expiration), CONSTRAINT PK_MsgKey PRIMARY KEY(id, seq))",
			"CREATE TABLE IF NOT EXISTS Subscriptions (id INTEGER, subid BIGINT UNSIGNED, proto BLOB, deleted BOOL DEFAULT FALSE, CONSTRAINT PK_SubKey PRIMARY KEY(id, subid))",
			"CREATE TABLE IF NOT EXISTS SubsPending (subid BIGINT UNSIGNED, seq BIGINT UNSIGNED, CONSTRAINT PK_MsgPendingKey PRIMARY KEY(subid, seq))",
		}
	}
	for _, stmt := range sqlCreateDatabase {
		if _, err := db.Exec(stmt); err != nil {
			stackFatalf(t, "Error executing statement (%s): %v", stmt, err)
		}
	}
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
			ms.db = db
			ms.Unlock()
			subs := c.Subs.(*SQLSubStore)
			subs.Lock()
			subs.db = db
			subs.Unlock()
		}
	}
	ss.Unlock()
	if err != nil {
		stackFatalf(t, "Error failing db connection: %v", err)
	}
}

func TestSQLErrorOnNewStore(t *testing.T) {
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

func TestSQLErrorsDueToFailDBConnection(t *testing.T) {
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
	cleanupSQLDatastore(t)
	defer cleanupSQLDatastore(t)

	gs := createDefaultSQLStore(t)
	defer gs.Close()

	s := gs.(*SQLStore)

	now := atomic.LoadInt64(&s.nowInNano)

	time.Sleep(1250 * time.Millisecond)

	newNow := atomic.LoadInt64(&s.nowInNano)
	if newNow == now {
		t.Fatalf("Looks like nowInNano was not updated")
	}
}

func TestSQLCloseOnMsgExpiration(t *testing.T) {
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

func TestSQLExpiredMsgsOnLookup(t *testing.T) {
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

	db, err := sql.Open(testSQLDriver, testSQLSource)
	if err != nil {
		t.Fatalf("Error on sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("USE " + testSQLDatabaseName); err != nil {
		t.Fatalf("Error selecting database: %v", err)
	}
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
