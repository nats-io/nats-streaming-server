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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nuid"
)

const (
	driverMySQL    = "mysql"
	driverPostgres = "postgres"
)

const (
	sqlDBLockSelect = iota
	sqlDBLockInsert
	sqlDBLockUpdate
	sqlHasServerInfoRow
	sqlUpdateServerInfo
	sqlAddServerInfo
	sqlAddClient
	sqlDeleteClient
	sqlAddChannel
	sqlStoreMsg
	sqlLookupMsg
	sqlGetSequenceFromTimestamp
	sqlUpdateChannelMaxSeq
	sqlGetExpiredMessages
	sqlGetFirstMsgTimestamp
	sqlDeletedMsgsWithSeqLowerThan
	sqlGetSizeOfMessage
	sqlDeleteMessage
	sqlCheckMaxSubs
	sqlCreateSub
	sqlUpdateSub
	sqlMarkSubscriptionAsDeleted
	sqlDeleteSubscription
	sqlDeleteSubMarkedAsDeleted
	sqlDeleteSubPendingMessages
	sqlSubUpdateLastSent
	sqlSubAddPending
	sqlSubAddPendingRow
	sqlSubDeletePending
	sqlSubDeletePendingRow
	sqlRecoverServerInfo
	sqlRecoverClients
	sqlRecoverMaxChannelID
	sqlRecoverMaxSubID
	sqlRecoverChannelsList
	sqlRecoverChannelMsgs
	sqlRecoverChannelSubs
	sqlRecoverDoPurgeSubsPending
	sqlRecoverSubPending
	sqlRecoverGetChannelLimits
	sqlRecoverDoExpireMsgs
	sqlRecoverGetMessagesCount
	sqlRecoverGetSeqFloorForMaxMsgs
	sqlRecoverGetChannelTotalSize
	sqlRecoverGetSeqFloorForMaxBytes
	sqlRecoverUpdateChannelLimits
	sqlDeleteChannelFast
	sqlDeleteChannelGetSubIds
	sqlDeleteChannelDelSubsPending
	sqlDeleteChannelDelSubscriptions
	sqlDeleteChannelGetSomeMessagesSeq
	sqlDeleteChannelDelSomeMessages
	sqlDeleteChannelDelChannel
	sqlGetLastSeq
)

var sqlStmts = []string{
	"SELECT id, tick from StoreLock FOR UPDATE",                                                                                                                    // sqlDBLockSelect
	"INSERT INTO StoreLock (id, tick) VALUES (?, ?)",                                                                                                               // sqlDBLockInsert
	"UPDATE StoreLock SET id=?, tick=?",                                                                                                                            // sqlDBLockUpdate
	"SELECT COUNT(uniquerow) FROM ServerInfo",                                                                                                                      // sqlHasServerInfoRow
	"UPDATE ServerInfo SET id=?, proto=?, version=? WHERE uniquerow=1",                                                                                             // sqlUpdateServerInfo
	"INSERT INTO ServerInfo (id, proto, version) VALUES (?, ?, ?)",                                                                                                 // sqlAddServerInfo
	"INSERT INTO Clients (id, hbinbox, proto) VALUES (?, ?, ?)",                                                                                                    // sqlAddClient
	"DELETE FROM Clients WHERE id=?",                                                                                                                               // sqlDeleteClient
	"INSERT INTO Channels (id, name, maxmsgs, maxbytes, maxage) VALUES (?, ?, ?, ?, ?)",                                                                            // sqlAddChannel
	"INSERT INTO Messages VALUES (?, ?, ?, ?, ?)",                                                                                                                  // sqlStoreMsg
	"SELECT timestamp, data FROM Messages WHERE id=? AND seq=?",                                                                                                    // sqlLookupMsg
	"SELECT seq FROM Messages WHERE id=? AND timestamp>=? LIMIT 1",                                                                                                 // sqlGetSequenceFromTimestamp
	"UPDATE Channels SET maxseq=? WHERE id=?",                                                                                                                      // sqlUpdateChannelMaxSeq
	"SELECT COUNT(seq), COALESCE(MAX(seq), 0), COALESCE(SUM(size), 0) FROM Messages WHERE id=? AND timestamp<=?",                                                   // sqlGetExpiredMessages
	"SELECT timestamp FROM Messages WHERE id=? AND seq>=? LIMIT 1",                                                                                                 // sqlGetFirstMsgTimestamp
	"DELETE FROM Messages WHERE id=? AND seq<=?",                                                                                                                   // sqlDeletedMsgsWithSeqLowerThan
	"SELECT size FROM Messages WHERE id=? AND seq=?",                                                                                                               // sqlGetSizeOfMessage
	"DELETE FROM Messages WHERE id=? AND seq=?",                                                                                                                    // sqlDeleteMessage
	"SELECT COUNT(subid) FROM Subscriptions WHERE id=? AND deleted=FALSE",                                                                                          // sqlCheckMaxSubs
	"INSERT INTO Subscriptions (id, subid, proto) VALUES (?, ?, ?)",                                                                                                // sqlCreateSub
	"UPDATE Subscriptions SET proto=? WHERE id=? AND subid=?",                                                                                                      // sqlUpdateSub
	"UPDATE Subscriptions SET deleted=TRUE WHERE id=? AND subid=?",                                                                                                 // sqlMarkSubscriptionAsDeleted
	"DELETE FROM Subscriptions WHERE id=? AND subid=?",                                                                                                             // sqlDeleteSubscription
	"DELETE FROM Subscriptions WHERE id=? AND deleted=TRUE",                                                                                                        // sqlDeleteSubMarkedAsDeleted
	"DELETE FROM SubsPending WHERE subid=?",                                                                                                                        // sqlDeleteSubPendingMessages
	"UPDATE Subscriptions SET lastsent=? WHERE id=? AND subid=?",                                                                                                   // sqlSubUpdateLastSent
	"INSERT INTO SubsPending (subid, `row`, seq) VALUES (?, ?, ?)",                                                                                                 // sqlSubAddPending
	"INSERT INTO SubsPending (subid, `row`, lastsent, pending, acks) VALUES (?, ?, ?, ?, ?)",                                                                       // sqlSubAddPendingRow
	"DELETE FROM SubsPending WHERE subid=? AND seq=?",                                                                                                              // sqlSubDeletePending
	"DELETE FROM SubsPending WHERE subid=? AND `row`=?",                                                                                                            // sqlSubDeletePendingRow
	"SELECT id, proto, version FROM ServerInfo WHERE uniquerow=1",                                                                                                  // sqlRecoverServerInfo
	"SELECT id, hbinbox, proto FROM Clients",                                                                                                                       // sqlRecoverClients
	"SELECT COALESCE(MAX(id), 0) FROM Channels",                                                                                                                    // sqlRecoverMaxChannelID
	"SELECT COALESCE(MAX(subid), 0) FROM Subscriptions",                                                                                                            // sqlRecoverMaxSubID
	"SELECT id, name, maxseq FROM Channels WHERE deleted=FALSE",                                                                                                    // sqlRecoverChannelsList
	"SELECT COUNT(seq), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0), COALESCE(SUM(size), 0), COALESCE(MAX(timestamp), 0) FROM Messages WHERE id=?",                // sqlRecoverChannelMsgs
	"SELECT lastsent, proto FROM Subscriptions WHERE id=? AND deleted=FALSE",                                                                                       // sqlRecoverChannelSubs
	"DELETE FROM SubsPending WHERE subid=? AND (seq > 0 AND seq<?)",                                                                                                // sqlRecoverDoPurgeSubsPending
	"SELECT `row`, seq, lastsent, pending, acks FROM SubsPending WHERE subid=?",                                                                                    // sqlRecoverSubPending
	"SELECT maxmsgs, maxbytes, maxage FROM Channels WHERE id=?",                                                                                                    // sqlRecoverGetChannelLimits
	"DELETE FROM Messages WHERE id=? AND timestamp<=?",                                                                                                             // sqlRecoverDoExpireMsgs
	"SELECT COUNT(seq) FROM Messages WHERE id=?",                                                                                                                   // sqlRecoverGetMessagesCount
	"SELECT MIN(t.seq) FROM (SELECT seq FROM Messages WHERE id=? ORDER BY seq DESC LIMIT ?)t",                                                                      // sqlRecoverGetSeqFloorForMaxMsgs
	"SELECT COALESCE(SUM(size), 0) FROM Messages WHERE id=?",                                                                                                       // sqlRecoverGetChannelTotalSize
	"SELECT COALESCE(MIN(seq), 0) FROM (SELECT seq, @sum:=@sum+size AS total FROM Messages JOIN (SELECT @sum:=0)m WHERE id=? ORDER BY seq DESC)t WHERE t.total<=?", // sqlRecoverGetSeqFloorForMaxBytes
	"UPDATE Channels SET maxmsgs=?, maxbytes=?, maxage=? WHERE id=?",                                                                                               // sqlRecoverUpdateChannelLimits
	"UPDATE Channels SET deleted=true WHERE id=?",                                                                                                                  // sqlDeleteChannelFast
	"SELECT DISTINCT(SubsPending.subid) FROM SubsPending INNER JOIN Subscriptions ON Subscriptions.id=? AND Subscriptions.subid=SubsPending.subid LIMIT ?",         // sqlDeleteChannelGetSubIds
	"DELETE FROM SubsPending WHERE subid=?",                                                                                                                        // sqlDeleteChannelDelSubsPending
	"DELETE FROM Subscriptions WHERE id=?",                                                                                                                         // sqlDeleteChannelDelSubscriptions
	"SELECT COALESCE(MAX(seq), 0) FROM (SELECT seq FROM Messages WHERE id=? ORDER BY seq LIMIT ?) AS t1",                                                           // sqlDeleteChannelGetSomeMessagesSeq
	"DELETE FROM Messages WHERE id=? AND seq<=?",                                                                                                                   // sqlDeleteChannelDelSomeMessages
	"DELETE FROM Channels WHERE id=?",                                                                                                                              // sqlDeleteChannelDelChannel
	"SELECT COALESCE(MAX(seq), 0) FROM Messages WHERE id=?",                                                                                                        // sqlGetLastSeq
}

var initSQLStmts = sync.Once{}

const (
	// This is to detect changes in the tables, etc...
	sqlVersion = 1

	// If any of the SQL queries fail when finding out messages that
	// need to be expired, use this as the default retry interval
	sqlDefaultExpirationIntervalOnError = time.Second

	// Interval at which time is captured.
	sqlDefaultTimeTickInterval = time.Second

	// Max number of elements in the pending or acks column in SubsPending table
	// after which a flush is forced.
	sqlDefaultMaxPendingAcks = 2000

	// The SubStore flusher timer is created once and reset to this
	// duration to indicate that it is idle.
	sqlDefaultSubStoreFlushIdleInterval = time.Hour

	// This is the default interval after which the SubStore will be
	// flushed when a subspending row need updating
	sqlDefaultSubStoreFlushInterval = time.Second

	// This is the default interval at which the lock table is updated
	// when GetExclusiveLock() has returned that the lock is acquired.
	sqlDefaultLockUpdateInterval = time.Second

	// Number of missed update interval after which the lock is assumed
	// lost and another instance can update it.
	sqlDefaultLockLostCount = 3
)

// These are initialized based on the constants that have reasonable values.
// But for tests, it is often interesting to be able to lower values to
// make tests finish faster.
var (
	sqlExpirationIntervalOnError = sqlDefaultExpirationIntervalOnError
	sqlTimeTickInterval          = sqlDefaultTimeTickInterval
	sqlMaxPendingAcks            = sqlDefaultMaxPendingAcks
	sqlSubStoreFlushIdleInterval = sqlDefaultSubStoreFlushIdleInterval
	sqlSubStoreFlushInterval     = sqlDefaultSubStoreFlushInterval
	sqlLockUpdateInterval        = sqlDefaultLockUpdateInterval
	sqlLockLostCount             = sqlDefaultLockLostCount
	sqlNoPanic                   = false // Used in tests to avoid go-routine to panic
)

// SQLStoreOptions are used to configure the SQL Store.
type SQLStoreOptions struct {
	Driver string
	Source string

	// By default, MsgStore.Store(), SubStore.AddSeqPending() and
	// SubStore.AckSeqPending() are storing the actions in memory, and
	// actual SQL statements are executed only when MsgStore.Flush()
	// and SubStore.Flush() are called.
	// If this option is set to `true`, each call to aforementioned
	// APIs will cause execution of their respective SQL statements.
	NoCaching bool

	// Maximum number of open connections to the database.
	// If <= 0, then there is no limit on the number of open connections.
	// The default is 0 (unlimited).
	MaxOpenConns int
}

// DefaultSQLStoreOptions returns default store options for an SQL Store
func DefaultSQLStoreOptions() *SQLStoreOptions {
	return &SQLStoreOptions{
		NoCaching:    false,
		MaxOpenConns: 0,
	}
}

// SQLStoreOption is a function on the options for a SQL Store
type SQLStoreOption func(*SQLStoreOptions) error

// SQLNoCaching sets the NoCaching option
func SQLNoCaching(noCaching bool) SQLStoreOption {
	return func(o *SQLStoreOptions) error {
		o.NoCaching = noCaching
		return nil
	}
}

// SQLMaxOpenConns sets the MaxOpenConns option
func SQLMaxOpenConns(max int) SQLStoreOption {
	return func(o *SQLStoreOptions) error {
		o.MaxOpenConns = max
		return nil
	}
}

// SQLAllOptions is a convenient option to pass all options from a SQLStoreOptions
// structure to the constructor.
func SQLAllOptions(opts *SQLStoreOptions) SQLStoreOption {
	return func(o *SQLStoreOptions) error {
		o.NoCaching = opts.NoCaching
		o.MaxOpenConns = opts.MaxOpenConns
		return nil
	}
}

// SQLStore is a factory for message and subscription stores backed by
// a SQL Database.
type SQLStore struct {
	// These are used with atomic operations and need to be 64-bit aligned.
	// Position them at the beginning of the structure.
	maxSubID  uint64
	nowInNano int64

	genericStore
	dbLock        *sqlDBLock
	opts          *SQLStoreOptions
	db            *sql.DB
	maxChannelID  int64
	doneCh        chan struct{}
	wg            sync.WaitGroup
	preparedStmts []*sql.Stmt
	ssFlusher     *subStoresFlusher
}

type sqlDBLock struct {
	sync.Mutex
	db      *sql.DB
	id      string
	isOwner bool
}

type subStoresFlusher struct {
	sync.Mutex
	stores   map[*SQLSubStore]struct{}
	signalCh chan struct{}
	signaled bool
}

// SQLSubStore is a subscription store backed by an SQL Database
type SQLSubStore struct {
	commonStore
	maxSubID       *uint64 // Points to the uint64 stored in SQLStore and is used with atomic operations
	channelID      int64
	sqlStore       *SQLStore // Reference to "parent" store
	limits         SubStoreLimits
	hasMarkedAsDel bool
	subLastSent    map[uint64]uint64
	curRow         uint64
	cache          *sqlSubAcksPendingCache
}

type sqlSubAcksPendingCache struct {
	subs       map[uint64]*sqlSubAcksPending // Key is subscription ID.
	needsFlush bool
}

type sqlSubAcksPending struct {
	lastSent     uint64
	prevLastSent uint64
	msgToRow     map[uint64]*sqlSubsPendingRow
	ackToRow     map[uint64]*sqlSubsPendingRow
	msgs         map[uint64]struct{}
	acks         map[uint64]struct{}
}

type sqlSubsPendingRow struct {
	ID       uint64
	msgs     map[uint64]struct{}
	msgsRefs int
	acksRefs int
}

// SQLMsgStore is a per channel message store backed by an SQL Database
type SQLMsgStore struct {
	genericMsgStore
	channelID   int64
	sqlStore    *SQLStore // Reference to "parent" store
	expireTimer *time.Timer
	wg          sync.WaitGroup

	// If option NoBuffering is false, uses this cache for storing Store()
	// commands until caller calls Flush() in which case we use transaction
	// to execute all pending store commands.
	// The cache is also used in Lookup since messages may not be yet in the
	// database.
	writeCache *sqlMsgsCache
}

type sqlMsgsCache struct {
	msgs map[uint64]*sqlCachedMsg
	head *sqlCachedMsg
	tail *sqlCachedMsg
	free *sqlCachedMsg
}

type sqlCachedMsg struct {
	msg  *pb.MsgProto
	data []byte
	next *sqlCachedMsg
}

// sqlStmtError returns an error including the text of the offending SQL statement.
func sqlStmtError(code int, err error) error {
	return fmt.Errorf("sql: error executing %q: %v", sqlStmts[code], err)
}

var sqlSeqMapPool = &sync.Pool{
	New: func() interface{} {
		return make(map[uint64]struct{})
	},
}
var sqlSeqArrayPool = &sync.Pool{
	New: func() interface{} {
		// This is to silence megacheck that says that sync.Pool should
		// only be used with pointers.
		a := make([]uint64, 0, 1024)
		return &a
	},
}

////////////////////////////////////////////////////////////////////////////
// SQLStore methods
////////////////////////////////////////////////////////////////////////////

// NewSQLStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewSQLStore(log logger.Logger, driver, source string, limits *StoreLimits, options ...SQLStoreOption) (*SQLStore, error) {
	initSQLStmts.Do(func() { initSQLStmtsTable(driver) })
	db, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	// Start with empty options
	opts := DefaultSQLStoreOptions()
	// And apply whatever is given to us as options.
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, err
		}
	}
	db.SetMaxOpenConns(opts.MaxOpenConns)
	s := &SQLStore{
		opts:          opts,
		db:            db,
		doneCh:        make(chan struct{}),
		preparedStmts: make([]*sql.Stmt, 0, len(sqlStmts)),
	}
	if err := s.init(TypeSQL, log, limits); err != nil {
		s.Close()
		return nil, err
	}
	if err := s.createPreparedStmts(); err != nil {
		s.Close()
		return nil, err
	}
	s.Lock()
	s.wg.Add(1)
	go s.timeTick()
	if !s.opts.NoCaching {
		s.wg.Add(1)
		s.ssFlusher = &subStoresFlusher{
			stores:   make(map[*SQLSubStore]struct{}),
			signalCh: make(chan struct{}, 1),
		}
		go s.subStoresFlusher()
	}
	s.Unlock()
	return s, nil
}

// GetExclusiveLock implements the Store interface
func (s *SQLStore) GetExclusiveLock() (bool, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return false, nil
	}
	if s.dbLock == nil {
		s.dbLock = &sqlDBLock{
			id: nuid.Next(),
			db: s.db,
		}
	}
	if s.dbLock.isOwner {
		return true, nil
	}
	hasLock, id, tick, err := s.acquireDBLock(false)
	if err != nil {
		return false, err
	}
	if !hasLock {
		// We did not get the lock. Try to see if the table is updated
		// after 1 interval. If so, consider the lock "healthy" and just
		// return that we did not get the lock. If after a configured
		// number of tries the tick for current owner is not updated,
		// steal the lock.
		prevID := id
		prevTick := tick
		for i := 0; i < sqlLockLostCount; i++ {
			time.Sleep(time.Duration(1.5 * float64(sqlLockUpdateInterval)))
			hasLock, id, tick, err = s.acquireDBLock(false)
			if hasLock || err != nil || id != prevID || tick != prevTick {
				return hasLock, err
			}
			prevTick = tick
		}
		// Try to steal.
		hasLock, _, _, err = s.acquireDBLock(true)
	}
	if hasLock {
		// Success. Keep track that we own the lock so we can clear
		// the table on clean shutdown to release the lock immediately.
		s.dbLock.Lock()
		s.dbLock.isOwner = true
		s.wg.Add(1)
		go s.updateDBLock()
		s.dbLock.Unlock()
	}
	return hasLock, err
}

// This go-routine updates the DB store lock at regular intervals.
func (s *SQLStore) updateDBLock() {
	defer s.wg.Done()

	var (
		ticker  = time.NewTicker(sqlLockUpdateInterval)
		hasLock = true
		err     error
		failed  int
	)
	for {
		select {
		case <-ticker.C:
			hasLock, _, _, err = s.acquireDBLock(false)
			if !hasLock || err != nil {
				// If there is no error but we did not get the lock,
				// something is really wrong, abort right away.
				stopNow := !hasLock && err == nil
				if err != nil {
					failed++
					s.log.Errorf("Unable to update store lock (failed=%v err=%v)", failed, err)
				}
				if stopNow || failed == sqlLockLostCount {
					if sqlNoPanic {
						s.log.Fatalf("Aborting")
						return
					}
					panic("lost store lock, aborting")
				}
			} else {
				failed = 0
			}
		case <-s.doneCh:
			ticker.Stop()
			return
		}
	}
}

// Returns if lock is acquired, the owner and tick value of the lock record.
func (s *SQLStore) acquireDBLock(steal bool) (bool, string, uint64, error) {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()
	var (
		lockID  string
		tick    uint64
		hasLock bool
	)
	tx, err := s.dbLock.db.Begin()
	if err != nil {
		return false, "", 0, err
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	r := tx.QueryRow(sqlStmts[sqlDBLockSelect])
	err = r.Scan(&lockID, &tick)
	if err != nil && err != sql.ErrNoRows {
		return false, "", 0, sqlStmtError(sqlDBLockSelect, err)
	}
	if err == sql.ErrNoRows || steal || lockID == "" || lockID == s.dbLock.id {
		// If we are stealing, reset tick to 0 (so it will become 1 in update statement)
		if steal {
			tick = 0
		}
		stmt := sqlStmts[sqlDBLockUpdate]
		if err == sql.ErrNoRows {
			stmt = sqlStmts[sqlDBLockInsert]
		}
		if _, err := tx.Exec(stmt, s.dbLock.id, tick+1); err != nil {
			return false, "", 0, sqlStmtError(sqlDBLockUpdate, err)
		}
		hasLock = true
	}
	if err := tx.Commit(); err != nil {
		return false, "", 0, err
	}
	tx = nil
	return hasLock, lockID, tick, nil
}

// Release the store lock if this store was the owner of the lock
func (s *SQLStore) releaseDBLockIfOwner() {
	s.dbLock.Lock()
	defer s.dbLock.Unlock()
	if s.dbLock.isOwner {
		s.dbLock.db.Exec(sqlStmts[sqlDBLockUpdate], "", 0)
	}
}

// When a SubStore adds a pending message or an ack, it will
// notify this go-routine so that the store gets flushed after
// some time should it not be flushed explicitly.
// This go routine waits to be signaled and when that happens
// reset a timer to fire in a short period of time. It then
// go through the list of SubStore that have been registered
// as needing a flush and call Flush() on them.
func (s *SQLStore) subStoresFlusher() {
	defer s.wg.Done()

	s.Lock()
	flusher := s.ssFlusher
	s.Unlock()

	var (
		stores []*SQLSubStore
		tm     = time.NewTimer(sqlSubStoreFlushIdleInterval)
	)

	for {
		select {
		case <-s.doneCh:
			return
		case <-flusher.signalCh:
			if !tm.Stop() {
				<-tm.C
			}
			tm.Reset(sqlSubStoreFlushInterval)
		case <-tm.C:
			flusher.Lock()
			for ss := range flusher.stores {
				stores = append(stores, ss)
				delete(flusher.stores, ss)
			}
			flusher.signaled = false
			flusher.Unlock()
			for _, ss := range stores {
				ss.Flush()
			}
			stores = stores[:0]
			tm.Reset(sqlSubStoreFlushIdleInterval)
		}
	}
}

// Add this store to the list of SubStore needing flushing
// and signal the go-routine responsible for flushing if
// need be.
func (s *SQLStore) scheduleSubStoreFlush(ss *SQLSubStore) {
	needSignal := false
	f := s.ssFlusher
	f.Lock()
	f.stores[ss] = struct{}{}
	if !f.signaled {
		f.signaled = true
		needSignal = true
	}
	f.Unlock()
	if needSignal {
		select {
		case f.signalCh <- struct{}{}:
		default:
		}
	}
}

// creates an instance of a SQLMsgStore
func (s *SQLStore) newSQLMsgStore(channel string, channelID int64, limits *MsgStoreLimits) *SQLMsgStore {
	msgStore := &SQLMsgStore{
		sqlStore:  s,
		channelID: channelID,
	}
	msgStore.init(channel, s.log, limits)
	if !s.opts.NoCaching {
		msgStore.writeCache = &sqlMsgsCache{msgs: make(map[uint64]*sqlCachedMsg)}
	}
	return msgStore
}

// creates an instance of SQLSubStore
func (s *SQLStore) newSQLSubStore(channelID int64, limits *SubStoreLimits) *SQLSubStore {
	subStore := &SQLSubStore{
		sqlStore:  s,
		channelID: channelID,
		maxSubID:  &s.maxSubID,
		limits:    *limits,
	}
	subStore.log = s.log
	if s.opts.NoCaching {
		subStore.subLastSent = make(map[uint64]uint64)
	} else {
		subStore.cache = &sqlSubAcksPendingCache{
			subs: make(map[uint64]*sqlSubAcksPending),
		}
	}
	return subStore
}

func (s *SQLStore) createPreparedStmts() error {
	s.preparedStmts = []*sql.Stmt{}
	for _, stmt := range sqlStmts {
		ps, err := s.db.Prepare(stmt)
		if err != nil {
			return fmt.Errorf("unable to prepare statement %q: %v", stmt, err)
		}
		s.preparedStmts = append(s.preparedStmts, ps)
	}
	return nil
}

// initialize the global sqlStmts table to driver's one.
func initSQLStmtsTable(driver string) {
	// The sqlStmts table is initialized with MySQL statements.
	// Update the statements for the selected driver.
	switch driver {
	case driverPostgres:
		// Replace ? with $1, $2, etc...
		reg := regexp.MustCompile(`\?`)
		for i, stmt := range sqlStmts {
			n := 0
			stmt := reg.ReplaceAllStringFunc(stmt, func(string) string {
				n++
				return "$" + strconv.Itoa(n)
			})
			sqlStmts[i] = stmt
		}
		// Replace `row` with row
		reg = regexp.MustCompile("`row`")
		for i, stmt := range sqlStmts {
			stmt := reg.ReplaceAllStringFunc(stmt, func(string) string {
				return "row"
			})
			sqlStmts[i] = stmt
		}
		// OVER (PARTITION ...) is not supported in older MySQL servers.
		// So the default SQL statement is specific to MySQL and uses variables.
		// For Postgres, replace with this statement:
		sqlStmts[sqlRecoverGetSeqFloorForMaxBytes] = "SELECT COALESCE(MIN(seq), 0) FROM (SELECT seq, SUM(size) OVER (PARTITION BY id ORDER BY seq DESC) AS total FROM Messages WHERE id=$1)t WHERE t.total<=$2"
	}
}

// Init implements the Store interface
func (s *SQLStore) Init(info *spb.ServerInfo) error {
	s.Lock()
	defer s.Unlock()
	count := 0
	r := s.db.QueryRow(sqlStmts[sqlHasServerInfoRow])
	if err := r.Scan(&count); err != nil && err != sql.ErrNoRows {
		return sqlStmtError(sqlHasServerInfoRow, err)
	}
	infoBytes, _ := info.Marshal()
	if count == 0 {
		if _, err := s.db.Exec(sqlStmts[sqlAddServerInfo], info.ClusterID, infoBytes, sqlVersion); err != nil {
			return sqlStmtError(sqlAddServerInfo, err)
		}
	} else {
		if _, err := s.db.Exec(sqlStmts[sqlUpdateServerInfo], info.ClusterID, infoBytes, sqlVersion); err != nil {
			return sqlStmtError(sqlUpdateServerInfo, err)
		}
	}
	return nil
}

// Recover implements the Store interface
func (s *SQLStore) Recover() (*RecoveredState, error) {
	s.Lock()
	defer s.Unlock()
	var (
		clusterID string
		data      []byte
		version   int
		err       error
	)
	r := s.db.QueryRow(sqlStmts[sqlRecoverServerInfo])
	if err := r.Scan(&clusterID, &data, &version); err != nil {
		// If there is no row, that means nothing to recover. Return nil for the
		// state and no error.
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, sqlStmtError(sqlRecoverServerInfo, err)
	}
	if version != sqlVersion {
		return nil, fmt.Errorf("sql: unsupported version: %v (supports [1..%v])", version, sqlVersion)
	}
	info := &spb.ServerInfo{}
	if err := info.Unmarshal(data); err != nil {
		return nil, err
	}
	if info.ClusterID != clusterID {
		return nil, fmt.Errorf("sql: id %q in column does not match cluster ID in data %q", clusterID, info.ClusterID)
	}

	// Create recovered state structure and fill it with server info.
	rs := &RecoveredState{
		Info: info,
	}

	var clients []*Client
	cliRows, err := s.db.Query(sqlStmts[sqlRecoverClients])
	if err != nil && err != sql.ErrNoRows {
		return nil, sqlStmtError(sqlRecoverClients, err)
	}
	defer cliRows.Close()
	for cliRows.Next() {
		var (
			clientID string
			hbInbox  string
			proto    []byte
		)
		if err := cliRows.Scan(&clientID, &hbInbox, &proto); err != nil {
			return nil, err
		}
		var client *Client
		if len(proto) == 0 {
			client = &Client{spb.ClientInfo{ID: clientID, HbInbox: hbInbox}}
		} else {
			info := spb.ClientInfo{}
			info.Unmarshal(proto)
			client = &Client{info}
		}
		clients = append(clients, client)
	}
	cliRows.Close()
	// Set clients into recovered state.
	rs.Clients = clients

	// Get the maxChannelID
	r = s.db.QueryRow(sqlStmts[sqlRecoverMaxChannelID])
	err = r.Scan(&s.maxChannelID)
	if err != nil && err != sql.ErrNoRows {
		return nil, sqlStmtError(sqlRecoverMaxChannelID, err)
	}
	// If there was no channel recovered, we are done
	if s.maxChannelID == 0 {
		return rs, nil
	}
	// Get the maxSubID
	r = s.db.QueryRow(sqlStmts[sqlRecoverMaxSubID])
	if err := r.Scan(&s.maxSubID); err != nil && err != sql.ErrNoRows {
		return nil, sqlStmtError(sqlRecoverMaxSubID, err)
	}

	// Recover individual channels
	var channels map[string]*RecoveredChannel
	channelRows, err := s.db.Query(sqlStmts[sqlRecoverChannelsList])
	if err != nil && err != sql.ErrNoRows {
		return nil, sqlStmtError(sqlRecoverChannelsList, err)
	}
	defer channelRows.Close()
	for channelRows.Next() {
		var (
			channelID int64
			name      string
			maxseq    uint64
		)
		if err := channelRows.Scan(&channelID, &name, &maxseq); err != nil {
			return nil, err
		}

		channelLimits := s.genericStore.getChannelLimits(name)

		msgStore := s.newSQLMsgStore(name, channelID, &channelLimits.MsgStoreLimits)

		if err := s.applyLimitsOnRecovery(msgStore); err != nil {
			return nil, err
		}

		r = s.preparedStmts[sqlRecoverChannelMsgs].QueryRow(channelID)
		var (
			totalCount    int
			first         uint64
			last          uint64
			totalBytes    uint64
			lastTimestamp int64
		)
		if err := r.Scan(&totalCount, &first, &last, &totalBytes, &lastTimestamp); err != nil && err != sql.ErrNoRows {
			return nil, sqlStmtError(sqlRecoverChannelMsgs, err)
		}
		msgStore.first = first
		msgStore.last = last
		msgStore.totalCount = totalCount
		msgStore.totalBytes = totalBytes
		// If all messages have expired, the above should all be 0, however,
		// the Channel table may contain a maxseq that we should use as starting
		// point.
		if msgStore.last == 0 {
			msgStore.first = maxseq + 1
			msgStore.last = maxseq
		}

		subStore := s.newSQLSubStore(channelID, &channelLimits.SubStoreLimits)
		// Prevent scheduling to flusher while we are recovering
		if !s.opts.NoCaching {
			// By setting this to true, we prevent scheduling since
			// scheduling would occur only if needsFlush is false.
			subStore.cache.needsFlush = true
		}

		var subscriptions []*RecoveredSubscription

		subRows, err := s.preparedStmts[sqlRecoverChannelSubs].Query(channelID)
		if err != nil {
			return nil, sqlStmtError(sqlRecoverChannelSubs, err)
		}
		defer subRows.Close()
		for subRows.Next() {
			var (
				lastSent   uint64
				protoBytes []byte
				ap         *sqlSubAcksPending
			)
			if err := subRows.Scan(&lastSent, &protoBytes); err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if protoBytes != nil {
				sub := &spb.SubState{}
				if err := sub.Unmarshal(protoBytes); err != nil {
					return nil, err
				}
				// We need to use the max of lastSent column or the one in the proto
				if lastSent > sub.LastSent {
					sub.LastSent = lastSent
				}
				if s.opts.NoCaching {
					// We can remove entries for sequence that are below the smallest
					// sequence that was found in Messages.
					if _, err := s.preparedStmts[sqlRecoverDoPurgeSubsPending].Exec(sub.ID, msgStore.first); err != nil {
						return nil, sqlStmtError(sqlRecoverDoPurgeSubsPending, err)
					}
				} else {
					ap = subStore.getOrCreateAcksPending(sub.ID, 0)
				}
				rows, err := s.preparedStmts[sqlRecoverSubPending].Query(sub.ID)
				if err != nil {
					return nil, sqlStmtError(sqlRecoverSubPending, err)
				}
				defer rows.Close()
				pendingAcks := make(PendingAcks)
				var gcedRows map[uint64]struct{}
				if !s.opts.NoCaching {
					gcedRows = make(map[uint64]struct{})
				}
				for rows.Next() {
					if err := subStore.recoverPendingRow(rows, sub, ap, pendingAcks, gcedRows); err != nil {
						return nil, err
					}
				}
				rows.Close()

				if s.opts.NoCaching {
					// Update the in-memory map tracking last sent
					subStore.subLastSent[sub.ID] = sub.LastSent
				} else {
					// Go over garbage collected rows and delete them
					for rowID := range gcedRows {
						if err := subStore.deleteSubPendingRow(sub.ID, rowID); err != nil {
							return nil, err
						}
					}
				}

				// Add to the recovered subscriptions
				subscriptions = append(subscriptions, &RecoveredSubscription{Sub: sub, Pending: pendingAcks})
			}
		}
		subRows.Close()

		if !s.opts.NoCaching {
			// Clear but also allow scheduling now that the recovery is complete.
			subStore.cache.needsFlush = false
		}

		rc := &RecoveredChannel{
			Channel: &Channel{
				Msgs: msgStore,
				Subs: subStore,
			},
			Subscriptions: subscriptions,
		}
		if channels == nil {
			channels = make(map[string]*RecoveredChannel)
		}
		channels[name] = rc
		s.channels[name] = rc.Channel
	}
	channelRows.Close()

	// Set channels into recovered state
	rs.Channels = channels

	return rs, nil
}

func (s *SQLStore) applyLimitsOnRecovery(ms *SQLMsgStore) error {
	// These are the current limits set on restart.
	limits := &ms.limits
	maxAge := int64(limits.MaxAge)
	// We need to check the ones that were stored in the DB.
	var (
		storedMsgsLimit  int
		storedBytesLimit int64
		storedAgeLimit   int64
	)
	r := s.preparedStmts[sqlRecoverGetChannelLimits].QueryRow(ms.channelID)
	if err := r.Scan(&storedMsgsLimit, &storedBytesLimit, &storedAgeLimit); err != nil {
		return sqlStmtError(sqlRecoverGetChannelLimits, err)
	}
	// If any of the limits is different than what was stored, we will
	// need to update the channel at the end of this function.
	needUpdate := storedMsgsLimit != limits.MaxMsgs || storedBytesLimit != limits.MaxBytes || storedAgeLimit != maxAge

	// Let's reduce the number of messages if there is an age limit and messages
	// should have expired.
	if maxAge > 0 {
		expiredTimestamp := time.Now().UnixNano() - int64(limits.MaxAge)
		if _, err := s.preparedStmts[sqlRecoverDoExpireMsgs].Exec(ms.channelID, expiredTimestamp); err != nil {
			return sqlStmtError(sqlRecoverDoExpireMsgs, err)
		}
	}
	// For MaxMsgs and MaxBytes we are interested only the new limit is
	// lower than the old one (since messages are removed during runtime,
	// if the limit has not been lowered, we should be good).
	if limits.MaxMsgs > 0 && limits.MaxMsgs < storedMsgsLimit {
		count := 0
		r := s.preparedStmts[sqlRecoverGetMessagesCount].QueryRow(ms.channelID)
		if err := r.Scan(&count); err != nil {
			return sqlStmtError(sqlRecoverGetMessagesCount, err)
		}
		// We leave at least 1 message
		if count > 1 && count > limits.MaxMsgs {
			seq := uint64(0)
			r = s.preparedStmts[sqlRecoverGetSeqFloorForMaxMsgs].QueryRow(ms.channelID, limits.MaxMsgs)
			if err := r.Scan(&seq); err != nil {
				return sqlStmtError(sqlRecoverGetSeqFloorForMaxMsgs, err)
			}
			if _, err := s.preparedStmts[sqlDeletedMsgsWithSeqLowerThan].Exec(ms.channelID, seq-1); err != nil {
				return sqlStmtError(sqlDeletedMsgsWithSeqLowerThan, err)
			}
		}
	}
	if limits.MaxBytes > 0 && limits.MaxBytes < storedBytesLimit {
		currentBytes := uint64(0)
		r := s.preparedStmts[sqlRecoverGetChannelTotalSize].QueryRow(ms.channelID)
		if err := r.Scan(&currentBytes); err != nil {
			return sqlStmtError(sqlRecoverGetChannelTotalSize, err)
		}
		if currentBytes > uint64(limits.MaxBytes) {
			seq := 0
			// This query finds the first seq (inclusive) for which the running total
			// size is <= max bytes.
			r := s.preparedStmts[sqlRecoverGetSeqFloorForMaxBytes].QueryRow(ms.channelID, uint64(limits.MaxBytes))
			if err := r.Scan(&seq); err != nil {
				return sqlStmtError(sqlRecoverGetSeqFloorForMaxBytes, err)
			}
			// If 0, it could mean that the very last message is bigger than maxBytes,
			// but then we should try to delete anything before the last (keep at least
			// one).
			if seq == 0 {
				r = s.preparedStmts[sqlGetLastSeq].QueryRow(ms.channelID)
				if err := r.Scan(&seq); err != nil {
					return sqlStmtError(sqlGetLastSeq, err)
				}
			}
			// Delete at seq-1
			if seq > 0 {
				seq--
			}
			if seq > 0 {
				if _, err := s.preparedStmts[sqlDeletedMsgsWithSeqLowerThan].Exec(ms.channelID, seq); err != nil {
					return sqlStmtError(sqlDeletedMsgsWithSeqLowerThan, err)
				}
			}
		}
	}
	// If limits were changed compared to last run, we need to update the
	// Channels table.
	if needUpdate {
		if _, err := s.preparedStmts[sqlRecoverUpdateChannelLimits].Exec(
			limits.MaxMsgs, limits.MaxBytes, maxAge, ms.channelID); err != nil {
			return sqlStmtError(sqlRecoverUpdateChannelLimits, err)
		}
	}
	return nil
}

// CreateChannel implements the Store interface
func (s *SQLStore) CreateChannel(channel string) (*Channel, error) {
	s.Lock()
	defer s.Unlock()

	// Verify that it does not already exist or that we did not hit the limits
	if err := s.canAddChannel(channel); err != nil {
		return nil, err
	}

	channelLimits := s.genericStore.getChannelLimits(channel)

	cid := s.maxChannelID + 1
	if _, err := s.preparedStmts[sqlAddChannel].Exec(cid, channel,
		channelLimits.MaxMsgs, channelLimits.MaxBytes, int64(channelLimits.MaxAge)); err != nil {
		return nil, sqlStmtError(sqlAddChannel, err)
	}
	s.maxChannelID = cid

	msgStore := s.newSQLMsgStore(channel, cid, &channelLimits.MsgStoreLimits)
	subStore := s.newSQLSubStore(cid, &channelLimits.SubStoreLimits)

	c := &Channel{
		Subs: subStore,
		Msgs: msgStore,
	}
	s.channels[channel] = c

	return c, nil
}

// DeleteChannel implements the Store interface
func (s *SQLStore) DeleteChannel(channel string) error {
	s.Lock()
	defer s.Unlock()
	c := s.channels[channel]
	if c == nil {
		return ErrNotFound
	}
	// Get the channel ID from Msgs store
	cid := c.Msgs.(*SQLMsgStore).channelID
	// Fast delete just marks the channel row as deleted
	if _, err := s.preparedStmts[sqlDeleteChannelFast].Exec(cid); err != nil {
		return err
	}

	// If that succeeds, proceed with deletion of channel
	delete(s.channels, channel)

	// Close the messages and subs stores
	c.Msgs.Close()
	c.Subs.Close()

	// Now trigger in a go routine the longer deletion of entries
	// in all other tables.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := s.deepChannelDelete(cid); err != nil {
			s.log.Errorf("Unable to completely delete channel %q: %v", channel, err)
		}
	}()

	return nil
}

// This function is called after a channel has been marked
// as deleted. It will do a "deep" delete of the channel,
// which means removing all rows from any table that has
// a reference to the deleted channel. It is executed in
// a separate go-routine (as to not block DeleteChannel()
// call). It will run to completion possibly delaying
// the closing of the store.
func (s *SQLStore) deepChannelDelete(channelID int64) error {
	// On Store.Close(), the prepared statements and DB
	// won't be closed until after this call returns,
	// so we don't need explicit store locking.

	// We start by removing from SubsPending.
	limit := 1000
	for {
		// This will get us a set of subscription ids. We need
		// to repeat since we have a limit in the query
		rows, err := s.preparedStmts[sqlDeleteChannelGetSubIds].Query(channelID, limit)

		// If no more row, we are done, continue with other tables.
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var subid uint64
			if err := rows.Scan(&subid); err != nil {
				return err
			}
			_, err := s.preparedStmts[sqlDeleteChannelDelSubsPending].Exec(subid)
			if err != nil {
				return err
			}
			count++
		}
		rows.Close()
		if count < limit {
			break
		}
	}
	// Same for messages, we will get a certain number of messages
	// to delete and repeat the operation.
	for {
		var maxSeq uint64

		row := s.preparedStmts[sqlDeleteChannelGetSomeMessagesSeq].QueryRow(channelID, limit)
		if err := row.Scan(&maxSeq); err != nil {
			return err
		}
		if maxSeq == 0 {
			break
		}
		_, err := s.preparedStmts[sqlDeleteChannelDelSomeMessages].Exec(channelID, maxSeq)
		if err != nil {
			return err
		}
	}
	// Now with the subscriptions and channel
	_, err := s.preparedStmts[sqlDeleteChannelDelSubscriptions].Exec(channelID)
	if err == nil {
		_, err = s.preparedStmts[sqlDeleteChannelDelChannel].Exec(channelID)
	}
	return err
}

// AddClient implements the Store interface
func (s *SQLStore) AddClient(info *spb.ClientInfo) (*Client, error) {
	s.Lock()
	defer s.Unlock()
	var (
		protoBytes []byte
		err        error
	)
	protoBytes, err = info.Marshal()
	if err != nil {
		return nil, err
	}
	client := &Client{*info}
	for i := 0; i < 2; i++ {
		_, err = s.preparedStmts[sqlAddClient].Exec(client.ID, client.HbInbox, protoBytes)
		if err == nil {
			break
		}
		// We stop if this is the second AddClient failed attempt.
		if i > 0 {
			err = sqlStmtError(sqlAddClient, err)
			break
		}
		// This is the first AddClient failed attempt. It could be because
		// client was already in db, so delete now and try again.
		_, err = s.preparedStmts[sqlDeleteClient].Exec(client.ID)
		if err != nil {
			err = sqlStmtError(sqlDeleteClient, err)
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return client, nil
}

// DeleteClient implements the Store interface
func (s *SQLStore) DeleteClient(clientID string) error {
	s.Lock()
	_, err := s.preparedStmts[sqlDeleteClient].Exec(clientID)
	if err != nil {
		err = sqlStmtError(sqlDeleteClient, err)
	}
	s.Unlock()
	return err
}

// timeTick updates the store's time in nanosecond at regular
// interval. The time is used in Lookup() to compensate for possible
// delay in expiring messages. The Lookup() will check the message's
// expiration time against the time captured here. If it is expired
// even though it is still in the database, Lookup() will return nil.
func (s *SQLStore) timeTick() {
	defer s.wg.Done()
	timer := time.NewTicker(sqlTimeTickInterval)
	for {
		select {
		case <-s.doneCh:
			timer.Stop()
			return
		case <-timer.C:
			atomic.StoreInt64(&s.nowInNano, time.Now().UnixNano())
		}
	}
}

// Close implements the Store interface
func (s *SQLStore) Close() error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return nil
	}
	s.closed = true
	// This will cause MsgStore's and SubStore's to be closed.
	err := s.close()
	db := s.db
	wg := &s.wg
	// Signal background go-routines to quit
	if s.doneCh != nil {
		close(s.doneCh)
	}
	s.Unlock()

	// Wait for go routine(s) to finish
	wg.Wait()

	s.Lock()
	for _, ps := range s.preparedStmts {
		if lerr := ps.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	if db != nil {
		if s.dbLock != nil {
			s.releaseDBLockIfOwner()
		}
		if lerr := db.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	s.Unlock()
	return err
}

////////////////////////////////////////////////////////////////////////////
// SQLMsgStore methods
////////////////////////////////////////////////////////////////////////////

func (mc *sqlMsgsCache) add(msg *pb.MsgProto, data []byte) {
	cachedMsg := mc.free
	if cachedMsg != nil {
		mc.free = cachedMsg.next
		cachedMsg.next = nil
		// Remove old message from the map
		delete(mc.msgs, cachedMsg.msg.Sequence)
	} else {
		cachedMsg = &sqlCachedMsg{}
	}
	cachedMsg.msg = msg
	cachedMsg.data = data
	mc.msgs[msg.Sequence] = cachedMsg
	if mc.head == nil {
		mc.head = cachedMsg
	} else {
		mc.tail.next = cachedMsg
	}
	mc.tail = cachedMsg
}

func (mc *sqlMsgsCache) transferToFreeList() {
	if mc.tail != nil {
		mc.tail.next = mc.free
		mc.free = mc.head
	}
	mc.head = nil
	mc.tail = nil
}

func (mc *sqlMsgsCache) pop() *sqlCachedMsg {
	cm := mc.head
	if cm != nil {
		delete(mc.msgs, cm.msg.Sequence)
		mc.head = cm.next
		if mc.head == nil {
			mc.tail = nil
		}
	}
	return cm
}

// Store implements the MsgStore interface
func (ms *SQLMsgStore) Store(m *pb.MsgProto) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	if m.Sequence <= ms.last {
		// We've already seen this message.
		return m.Sequence, nil
	}

	seq := m.Sequence
	msgBytes, _ := m.Marshal()

	dataLen := uint64(len(msgBytes))

	useCache := !ms.sqlStore.opts.NoCaching
	if useCache {
		ms.writeCache.add(m, msgBytes)
	} else {
		if _, err := ms.sqlStore.preparedStmts[sqlStoreMsg].Exec(ms.channelID, seq, m.Timestamp, dataLen, msgBytes); err != nil {
			return 0, sqlStmtError(sqlStoreMsg, err)
		}
	}
	if ms.first == 0 {
		ms.first = seq
	}
	ms.last = seq
	ms.totalCount++
	ms.totalBytes += dataLen

	// Check if we need to remove any (but leave at least the last added)
	maxMsgs := ms.limits.MaxMsgs
	maxBytes := ms.limits.MaxBytes
	if maxMsgs > 0 || maxBytes > 0 {
		for ms.totalCount > 1 &&
			((maxMsgs > 0 && ms.totalCount > maxMsgs) ||
				(maxBytes > 0 && (ms.totalBytes > uint64(maxBytes)))) {

			didSQL := false
			delBytes := uint64(0)

			if useCache && ms.writeCache.head.msg.Sequence == ms.first {
				firstCachedMsg := ms.writeCache.pop()
				delBytes = uint64(len(firstCachedMsg.data))
			} else {
				r := ms.sqlStore.preparedStmts[sqlGetSizeOfMessage].QueryRow(ms.channelID, ms.first)
				if err := r.Scan(&delBytes); err != nil && err != sql.ErrNoRows {
					return 0, sqlStmtError(sqlGetSizeOfMessage, err)
				}
				didSQL = true
			}
			if delBytes > 0 {
				if didSQL {
					if _, err := ms.sqlStore.preparedStmts[sqlDeleteMessage].Exec(ms.channelID, ms.first); err != nil {
						return 0, sqlStmtError(sqlDeleteMessage, err)
					}
				}
				ms.totalCount--
				ms.totalBytes -= delBytes
				ms.first++
			}
			if !ms.hitLimit {
				ms.hitLimit = true
				ms.log.Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxMsgs,
					util.FriendlyBytes(int64(ms.totalBytes)), util.FriendlyBytes(ms.limits.MaxBytes))
			}
		}
	}

	if !useCache && ms.limits.MaxAge > 0 && ms.expireTimer == nil {
		ms.createExpireTimer()
	}
	return seq, nil
}

func (ms *SQLMsgStore) createExpireTimer() {
	ms.wg.Add(1)
	ms.expireTimer = time.AfterFunc(ms.limits.MaxAge, ms.expireMsgs)
}

// Lookup implements the MsgStore interface
func (ms *SQLMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookup(seq)
	ms.Unlock()
	return msg, err
}

func (ms *SQLMsgStore) lookup(seq uint64) (*pb.MsgProto, error) {
	var (
		timestamp int64
		data      []byte
		msg       *pb.MsgProto
	)
	if seq < ms.first || seq > ms.last {
		return nil, nil
	}
	if !ms.sqlStore.opts.NoCaching {
		cm := ms.writeCache.msgs[seq]
		if cm != nil {
			msg = cm.msg
			timestamp = msg.Timestamp
		}
	}
	if msg == nil {
		r := ms.sqlStore.preparedStmts[sqlLookupMsg].QueryRow(ms.channelID, seq)
		err := r.Scan(&timestamp, &data)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, sqlStmtError(sqlLookupMsg, err)
		}
	}
	if maxAge := int64(ms.limits.MaxAge); maxAge > 0 && atomic.LoadInt64(&ms.sqlStore.nowInNano) > timestamp+maxAge {
		return nil, nil
	}
	if msg == nil {
		msg = &pb.MsgProto{}
		msg.Unmarshal(data)
	}
	return msg, nil
}

// GetSequenceFromTimestamp implements the MsgStore interface
func (ms *SQLMsgStore) GetSequenceFromTimestamp(timestamp int64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	// No message ever stored
	if ms.first == 0 {
		return 0, nil
	}
	// All messages have expired
	if ms.first > ms.last {
		return ms.last + 1, nil
	}
	r := ms.sqlStore.preparedStmts[sqlGetSequenceFromTimestamp].QueryRow(ms.channelID, timestamp)
	seq := uint64(0)
	err := r.Scan(&seq)
	if err == sql.ErrNoRows {
		return ms.last + 1, nil
	}
	if err != nil {
		return 0, sqlStmtError(sqlGetSequenceFromTimestamp, err)
	}
	return seq, nil
}

// FirstMsg implements the MsgStore interface
func (ms *SQLMsgStore) FirstMsg() (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookup(ms.first)
	ms.Unlock()
	return msg, err
}

// LastMsg implements the MsgStore interface
func (ms *SQLMsgStore) LastMsg() (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookup(ms.last)
	ms.Unlock()
	return msg, err
}

// expireMsgsLocked removes all messages that have expired in this channel.
// Store lock is assumed held on entry
func (ms *SQLMsgStore) expireMsgs() {
	ms.Lock()
	defer ms.Unlock()

	if ms.closed {
		ms.wg.Done()
		return
	}

	var (
		count     int
		maxSeq    uint64
		totalSize uint64
		timestamp int64
	)
	processErr := func(errCode int, err error) {
		ms.log.Errorf("Unable to perform expiration for channel %q: %v", ms.subject, sqlStmtError(errCode, err))
		ms.expireTimer.Reset(sqlExpirationIntervalOnError)
	}
	for {
		expiredTimestamp := time.Now().UnixNano() - int64(ms.limits.MaxAge)
		r := ms.sqlStore.preparedStmts[sqlGetExpiredMessages].QueryRow(ms.channelID, expiredTimestamp)
		if err := r.Scan(&count, &maxSeq, &totalSize); err != nil {
			processErr(sqlGetExpiredMessages, err)
			return
		}
		// It could be that messages that should have expired have been
		// removed due to count/size limit. We still need to adjust the
		// expiration timer based on the first message that need to expire.
		if count > 0 {
			if maxSeq == ms.last {
				if _, err := ms.sqlStore.preparedStmts[sqlUpdateChannelMaxSeq].Exec(maxSeq, ms.channelID); err != nil {
					processErr(sqlUpdateChannelMaxSeq, err)
					return
				}
			}
			if _, err := ms.sqlStore.preparedStmts[sqlDeletedMsgsWithSeqLowerThan].Exec(ms.channelID, maxSeq); err != nil {
				processErr(sqlDeletedMsgsWithSeqLowerThan, err)
				return
			}
			ms.first = maxSeq + 1
			ms.totalCount -= count
			ms.totalBytes -= totalSize
		}
		// Reset since we are in a loop
		timestamp = 0
		// If there is any message left in the channel, find out what the expiration
		// timer needs to be set to.
		if ms.totalCount > 0 {
			r = ms.sqlStore.preparedStmts[sqlGetFirstMsgTimestamp].QueryRow(ms.channelID, ms.first)
			if err := r.Scan(&timestamp); err != nil {
				processErr(sqlGetFirstMsgTimestamp, err)
				return
			}
		}
		// No message left or no message to expire. The timer will be recreated when
		// a new message is added to the channel.
		if timestamp == 0 {
			ms.wg.Done()
			ms.expireTimer = nil
			return
		}
		elapsed := time.Duration(time.Now().UnixNano() - timestamp)
		if elapsed < ms.limits.MaxAge {
			ms.expireTimer.Reset(ms.limits.MaxAge - elapsed)
			// Done with the for loop
			return
		}
	}
}

func (ms *SQLMsgStore) flush() error {
	if ms.sqlStore.opts.NoCaching {
		return nil
	}
	if ms.writeCache.head == nil {
		return nil
	}
	var (
		tx *sql.Tx
		ps *sql.Stmt
	)
	defer func() {
		ms.writeCache.transferToFreeList()
		if ps != nil {
			ps.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err := ms.sqlStore.db.Begin()
	if err != nil {
		return err
	}
	ps, err = tx.Prepare(sqlStmts[sqlStoreMsg])
	if err != nil {
		return err
	}
	// Iterate through the cache, but do not remove elements from the list.
	// They are needed in transferToFreeList().
	for cm := ms.writeCache.head; cm != nil; cm = cm.next {
		if _, err := ps.Exec(ms.channelID, cm.msg.Sequence, cm.msg.Timestamp, len(cm.data), cm.data); err != nil {
			return err
		}
	}
	if err := ps.Close(); err != nil {
		return err
	}
	ps = nil
	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	if ms.limits.MaxAge > 0 && ms.expireTimer == nil {
		ms.createExpireTimer()
	}
	return nil
}

// Empty implements the MsgStore interface
func (ms *SQLMsgStore) Empty() error {
	ms.Lock()
	tx, err := ms.sqlStore.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(sqlStmts[sqlDeletedMsgsWithSeqLowerThan], ms.channelID, ms.last); err != nil {
		return err
	}
	if _, err := tx.Exec(sqlStmts[sqlUpdateChannelMaxSeq], 0, ms.channelID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	ms.empty()
	if ms.expireTimer != nil {
		if ms.expireTimer.Stop() {
			ms.wg.Done()
		}
		ms.expireTimer = nil
	}
	if ms.writeCache != nil {
		ms.writeCache.transferToFreeList()
	}
	ms.Unlock()
	return err
}

// Flush implements the MsgStore interface
func (ms *SQLMsgStore) Flush() error {
	ms.Lock()
	err := ms.flush()
	ms.Unlock()
	return err
}

// Close implements the MsgStore interface
func (ms *SQLMsgStore) Close() error {
	ms.Lock()
	if ms.closed {
		ms.Unlock()
		return nil
	}
	// Flush before switching the state to closed
	err := ms.flush()
	ms.closed = true
	if ms.expireTimer != nil {
		if ms.expireTimer.Stop() {
			ms.wg.Done()
		}
	}
	ms.Unlock()

	ms.wg.Wait()
	return err
}

////////////////////////////////////////////////////////////////////////////
// SQLSubStore methods
////////////////////////////////////////////////////////////////////////////

// CreateSub implements the SubStore interface
func (ss *SQLSubStore) CreateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()
	// Check limits only if needed
	if ss.limits.MaxSubscriptions > 0 {
		r := ss.sqlStore.preparedStmts[sqlCheckMaxSubs].QueryRow(ss.channelID)
		count := 0
		if err := r.Scan(&count); err != nil {
			return sqlStmtError(sqlCheckMaxSubs, err)
		}
		if count >= ss.limits.MaxSubscriptions {
			return ErrTooManySubs
		}
	}
	sub.ID = atomic.AddUint64(ss.maxSubID, 1)
	subBytes, _ := sub.Marshal()
	if _, err := ss.sqlStore.preparedStmts[sqlCreateSub].Exec(ss.channelID, sub.ID, subBytes); err != nil {
		sub.ID = 0
		return sqlStmtError(sqlCreateSub, err)
	}
	if ss.hasMarkedAsDel {
		if _, err := ss.sqlStore.preparedStmts[sqlDeleteSubMarkedAsDeleted].Exec(ss.channelID); err != nil {
			return sqlStmtError(sqlDeleteSubMarkedAsDeleted, err)
		}
		ss.hasMarkedAsDel = false
	}
	return nil
}

// UpdateSub implements the SubStore interface
func (ss *SQLSubStore) UpdateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()
	subBytes, _ := sub.Marshal()
	r, err := ss.sqlStore.preparedStmts[sqlUpdateSub].Exec(subBytes, ss.channelID, sub.ID)
	if err != nil {
		return sqlStmtError(sqlUpdateSub, err)
	}
	// FileSubStoe supports updating a subscription for which there was no CreateSub.
	// Not sure if this is necessary, since I think server would never do that.
	// Stay consistent.
	c, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if c == 0 {
		if _, err := ss.sqlStore.preparedStmts[sqlCreateSub].Exec(ss.channelID, sub.ID, subBytes); err != nil {
			return sqlStmtError(sqlCreateSub, err)
		}
	}
	return nil
}

// DeleteSub implements the SubStore interface
func (ss *SQLSubStore) DeleteSub(subid uint64) error {
	ss.Lock()
	defer ss.Unlock()
	if subid == atomic.LoadUint64(ss.maxSubID) {
		if _, err := ss.sqlStore.preparedStmts[sqlMarkSubscriptionAsDeleted].Exec(ss.channelID, subid); err != nil {
			return sqlStmtError(sqlMarkSubscriptionAsDeleted, err)
		}
		ss.hasMarkedAsDel = true
	} else {
		if _, err := ss.sqlStore.preparedStmts[sqlDeleteSubscription].Exec(ss.channelID, subid); err != nil {
			return sqlStmtError(sqlDeleteSubscription, err)
		}
	}
	if ss.cache != nil {
		delete(ss.cache.subs, subid)
	} else {
		delete(ss.subLastSent, subid)
	}
	// Ignore error on this since subscription would not be recovered
	// if above executed ok.
	ss.sqlStore.preparedStmts[sqlDeleteSubPendingMessages].Exec(subid)
	return nil
}

// This returns the structure responsible to keep track of
// pending messages and acks for a given subscription ID.
func (ss *SQLSubStore) getOrCreateAcksPending(subid, seqno uint64) *sqlSubAcksPending {
	if !ss.cache.needsFlush {
		ss.cache.needsFlush = true
		ss.sqlStore.scheduleSubStoreFlush(ss)
	}
	ap := ss.cache.subs[subid]
	if ap == nil {
		ap = &sqlSubAcksPending{
			msgToRow: make(map[uint64]*sqlSubsPendingRow),
			ackToRow: make(map[uint64]*sqlSubsPendingRow),
			msgs:     make(map[uint64]struct{}),
			acks:     make(map[uint64]struct{}),
		}
		ss.cache.subs[subid] = ap
	}
	if seqno > ap.lastSent {
		ap.lastSent = seqno
	}
	return ap
}

// Adds the given sequence to the list of pending messages.
// Returns true if the number of pending messages has
// reached a certain threshold, indicating that the
// store should be flushed.
func (ss *SQLSubStore) addSeq(subid, seqno uint64) bool {
	ap := ss.getOrCreateAcksPending(subid, seqno)
	ap.msgs[seqno] = struct{}{}
	return len(ap.msgs) >= sqlMaxPendingAcks
}

// Adds the given sequence to the list of acks and possibly
// delete rows that have all their pending messages acknowledged.
// Returns true if the number of acks has reached a certain threshold,
// indicating that the store should be flushed.
func (ss *SQLSubStore) ackSeq(subid, seqno uint64) (bool, error) {
	ap := ss.getOrCreateAcksPending(subid, seqno)
	// If still in cache and not persisted into a row,
	// then simply remove from map and do not persist the ack.
	if _, exists := ap.msgs[seqno]; exists {
		delete(ap.msgs, seqno)
	} else if row := ap.msgToRow[seqno]; row != nil {
		ap.acks[seqno] = struct{}{}
		// This is an ack for a pending msg that was persisted
		// in a row. Update the row's msgRef count.
		delete(ap.msgToRow, seqno)
		row.msgsRefs--
		// If all pending messages in that row have been ack'ed
		if row.msgsRefs == 0 {
			// and if all acks on that row are no longer needed
			// (or there was none)
			if row.acksRefs == 0 {
				// then this row can be deleted.
				if err := ss.deleteSubPendingRow(subid, row.ID); err != nil {
					return false, err
				}
				// If there is no error, we don't even need
				// to persist this ack.
				delete(ap.acks, seqno)
			}
			// Since there is no pending message left in this
			// row, let's find all the corresponding acks' rows
			// for these sequences and update their acksRefs
			for seq := range row.msgs {
				delete(row.msgs, seq)
				ackRow := ap.ackToRow[seq]
				if ackRow != nil {
					// We found the row for the ack of this sequence,
					// remove from map and update reference count.
					// delete(ap.ackToRow, seq)
					ackRow.acksRefs--
					// If all acks for that row are no longer needed and
					// that row has also no pending messages, then ok to
					// delete.
					if ackRow.acksRefs == 0 && ackRow.msgsRefs == 0 {
						if err := ss.deleteSubPendingRow(subid, ackRow.ID); err != nil {
							return false, err
						}
					}
				} else {
					// That means the ack is in current cache so we won't
					// need to persist it.
					delete(ap.acks, seq)
				}
			}
			sqlSeqMapPool.Put(row.msgs)
			row.msgs = nil
		}
	}
	return len(ap.acks) >= sqlMaxPendingAcks, nil
}

// AddSeqPending implements the SubStore interface
func (ss *SQLSubStore) AddSeqPending(subid, seqno uint64) error {
	var err error
	ss.Lock()
	if !ss.closed {
		if ss.cache != nil {
			if isFull := ss.addSeq(subid, seqno); isFull {
				err = ss.flush()
			}
		} else {
			ls := ss.subLastSent[subid]
			if seqno > ls {
				ss.subLastSent[subid] = seqno
			}
			ss.curRow++
			_, err = ss.sqlStore.preparedStmts[sqlSubAddPending].Exec(subid, ss.curRow, seqno)
			if err != nil {
				err = sqlStmtError(sqlSubAddPending, err)
			}
		}
	}
	ss.Unlock()
	return err
}

// AckSeqPending implements the SubStore interface
func (ss *SQLSubStore) AckSeqPending(subid, seqno uint64) error {
	var err error
	ss.Lock()
	if !ss.closed {
		if ss.cache != nil {
			var isFull bool
			isFull, err = ss.ackSeq(subid, seqno)
			if err == nil && isFull {
				err = ss.flush()
			}
		} else {
			updateLastSent := false
			ls := ss.subLastSent[subid]
			if seqno >= ls {
				if seqno > ls {
					ss.subLastSent[subid] = seqno
				}
				updateLastSent = true
			}
			if updateLastSent {
				if _, err := ss.sqlStore.preparedStmts[sqlSubUpdateLastSent].Exec(seqno, ss.channelID, subid); err != nil {
					ss.Unlock()
					return sqlStmtError(sqlSubUpdateLastSent, err)
				}
			}
			_, err = ss.sqlStore.preparedStmts[sqlSubDeletePending].Exec(subid, seqno)
			if err != nil {
				err = sqlStmtError(sqlSubDeletePending, err)
			}
		}
	}
	ss.Unlock()
	return err
}

func (ss *SQLSubStore) deleteSubPendingRow(subid, rowid uint64) error {
	if _, err := ss.sqlStore.preparedStmts[sqlSubDeletePendingRow].Exec(subid, rowid); err != nil {
		return sqlStmtError(sqlSubDeletePendingRow, err)
	}
	return nil
}

func (ss *SQLSubStore) recoverPendingRow(rows *sql.Rows, sub *spb.SubState, ap *sqlSubAcksPending, pendingAcks PendingAcks,
	gcedRows map[uint64]struct{}) error {
	var (
		seq, lastSent           uint64
		pendingBytes, acksBytes []byte
	)
	if err := rows.Scan(&ss.curRow, &seq, &lastSent, &pendingBytes, &acksBytes); err != nil && err != sql.ErrNoRows {
		return err
	}
	// If seq is non zero, this was created from a non-buffered run.
	if seq > 0 {
		if seq > sub.LastSent {
			sub.LastSent = seq
		}
		pendingAcks[seq] = struct{}{}
	} else {
		row := &sqlSubsPendingRow{
			ID:   ss.curRow,
			msgs: sqlSeqMapPool.Get().(map[uint64]struct{}),
		}
		ap.lastSent = lastSent
		ap.prevLastSent = lastSent

		if lastSent > sub.LastSent {
			sub.LastSent = lastSent
		}
		if len(pendingBytes) > 0 {
			if err := sqlDecodeSeqs(pendingBytes, func(seq uint64) {
				pendingAcks[seq] = struct{}{}
				row.msgsRefs++
				row.msgs[seq] = struct{}{}
				ap.msgToRow[seq] = row
			}); err != nil {
				return err
			}
		}
		if len(acksBytes) > 0 {
			if err := sqlDecodeSeqs(acksBytes, func(seq uint64) {
				if _, exists := pendingAcks[seq]; exists {
					delete(pendingAcks, seq)
					row.acksRefs++
					ap.ackToRow[seq] = row

					seqRow := ap.msgToRow[seq]
					if seqRow != nil {
						delete(ap.msgToRow, seq)
						seqRow.msgsRefs--
						if seqRow.msgsRefs == 0 && seqRow.acksRefs == 0 {
							gcedRows[seqRow.ID] = struct{}{}
						}
					}
				}
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush implements the SubStore interface
func (ss *SQLSubStore) Flush() error {
	ss.Lock()
	err := ss.flush()
	ss.Unlock()
	return err
}

func (ss *SQLSubStore) flush() error {
	if ss.cache == nil || !ss.cache.needsFlush || ss.closed {
		return nil
	}
	var (
		tx  *sql.Tx
		ps  *sql.Stmt
		err error
	)
	defer func() {
		if ps != nil {
			ps.Close()
		}
		if tx != nil {
			tx.Rollback()
		}
	}()
	tx, err = ss.sqlStore.db.Begin()
	if err != nil {
		return err
	}
	ps, err = tx.Prepare(sqlStmts[sqlSubAddPendingRow])
	if err != nil {
		return err
	}
	for subid, ap := range ss.cache.subs {
		prevLastSent := ap.prevLastSent
		ap.prevLastSent = ap.lastSent
		if len(ap.msgs) == 0 && len(ap.acks) == 0 {
			// Update subscription's lastSent column if it has changed.
			if ap.lastSent != prevLastSent {
				if _, err := tx.Exec(sqlStmts[sqlSubUpdateLastSent], ap.lastSent, ss.channelID, subid); err != nil {
					return err
				}
			}
			// Since there was no pending nor ack for this sub, simply continue
			// with the next subscription.
			continue
		}
		var (
			pendingBytes []byte
			acksBytes    []byte
		)
		ss.curRow++
		row := &sqlSubsPendingRow{ID: ss.curRow}
		if len(ap.msgs) > 0 {
			pendingBytes, err = sqlEncodeSeqs(ap.msgs, func(seqno uint64) {
				row.msgsRefs++
				ap.msgToRow[seqno] = row
			})
			if err != nil {
				return err
			}
			row.msgs = ap.msgs
			ap.msgs = sqlSeqMapPool.Get().(map[uint64]struct{})
		}
		if len(ap.acks) > 0 {
			acksBytes, err = sqlEncodeSeqs(ap.acks, func(seqno uint64) {
				delete(ap.acks, seqno)
				row.acksRefs++
				ap.ackToRow[seqno] = row
			})
			if err != nil {
				return err
			}
		}
		if _, err := ps.Exec(subid, ss.curRow, ap.lastSent, pendingBytes, acksBytes); err != nil {
			return err
		}
	}
	if err := ps.Close(); err != nil {
		return err
	}
	ps = nil
	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	ss.cache.needsFlush = false
	return nil
}

func sqlEncodeSeqs(m map[uint64]struct{}, f func(seq uint64)) ([]byte, error) {
	// We store as a pointer in the sync pool.
	pseqarray := sqlSeqArrayPool.Get().(*[]uint64)
	seqarray := *pseqarray
	for seqno := range m {
		f(seqno)
		seqarray = append(seqarray, seqno)
	}
	b, err := json.Marshal(seqarray)
	if err != nil {
		return nil, err
	}
	seqarray = seqarray[:0]
	sqlSeqArrayPool.Put(&seqarray)
	return b, nil
}

func sqlDecodeSeqs(data []byte, f func(seq uint64)) error {
	var seqarray []uint64
	if err := json.Unmarshal(data, &seqarray); err != nil {
		return err
	}
	if seqarray != nil {
		for _, seq := range seqarray {
			f(seq)
		}
		seqarray = seqarray[:0]
		sqlSeqArrayPool.Put(&seqarray)
	}
	return nil
}

// Close implements the SubStore interface
func (ss *SQLSubStore) Close() error {
	ss.Lock()
	if ss.closed {
		ss.Unlock()
		return nil
	}
	// Flush before switching the state to closed.
	err := ss.flush()
	ss.closed = true
	ss.Unlock()
	return err
}
