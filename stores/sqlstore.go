// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	_ "github.com/lib/pq"              // postgres driver

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

const (
	sqlUpdateServerInfo = iota
	sqlAddServerInfo
	sqlAddClient
	sqlDeleteClient
	sqlAddChannel
	sqlStoreMsg
	sqlLookupMsg
	sqlGetSequenceFromTimestamp
	sqlGetFirstMsgToExpire
	sqlGetChannelNamesWithExpiredMessages
	sqlGetExpiredMessagesForChannel
	sqlDeletedMsgsWithSeqLowerThan
	sqlGetSizeOfMessage
	sqlDeleteMessage
	sqlCheckMaxSubs
	sqlCreateSub
	sqlUpdateSub
	sqlMarkSubscriptionAsDeleted
	sqlDeleteSubscription
	sqlDeleteSubPendingMessages
	sqlSubAddPending
	sqlSubDeletePending
)

var sqlStmts = []string{
	"UPDATE ServerInfo SET id=?, data=?, version=? WHERE uniquerow=1",                                                                   // sqlUpdateServerInfo
	"INSERT INTO ServerInfo (id, data, version) VALUES (?, ?, ?)",                                                                       // sqlAddServerInfo
	"INSERT INTO Clients (id, hbinbox) VALUES (?, ?)",                                                                                   // sqlAddClient
	"DELETE FROM Clients WHERE id=?",                                                                                                    // sqlDeleteClient
	"INSERT INTO Channels (id, name) VALUES (?, ?)",                                                                                     // sqlAddChannel
	"INSERT INTO Messages VALUES (?, ?, ?, ?, ?, ?)",                                                                                    // sqlStoreMsg
	"SELECT expiration, data FROM Messages WHERE id=? AND seq=?",                                                                        // sqlLookupMsg
	"SELECT seq FROM Messages WHERE id=? AND timestamp>=? LIMIT 1",                                                                      // sqlGetSequenceFromTimestamp
	"SELECT expiration FROM Messages WHERE expiration < ? ORDER BY expiration LIMIT 1",                                                  // sqlGetFirstMsgToExpire
	"SELECT Channels.name FROM Channels INNER JOIN Messages WHERE Channels.id = Messages.id AND expiration <= ? GROUP BY Channels.name", // sqlGetChannelNamesWithExpiredMessages
	"SELECT COUNT(seq), IFNULL(MAX(seq), 0), IFNULL(SUM(size), 0) FROM Messages WHERE id=? AND expiration<=?",                           // sqlGetExpiredMessagesForChannel
	"DELETE FROM Messages WHERE id=? AND seq<=?",                                                                                        // sqlDeletedMsgsWithSeqLowerThan
	"SELECT size FROM Messages WHERE id=? AND seq=?",                                                                                    // sqlGetSizeOfMessage
	"DELETE FROM Messages WHERE id=? AND seq=?",                                                                                         // sqlDeleteMessage
	"SELECT COUNT(subid) FROM Subscriptions WHERE id=? AND deleted=FALSE",                                                               // sqlCheckMaxSubs
	"INSERT INTO Subscriptions (id, subid, proto) VALUES (?, ?, ?)",                                                                     // sqlCreateSub
	"UPDATE Subscriptions SET proto=? WHERE id=? AND subid=?",                                                                           // sqlUpdateSub
	"UPDATE Subscriptions SET deleted=TRUE WHERE id=? AND subid=?",                                                                      // sqlMarkSubscriptionAsDeleted
	"DELETE FROM Subscriptions WHERE id=? AND subid=?",                                                                                  // sqlDeleteSubscription
	"DELETE FROM SubsPending WHERE subid=?",                                                                                             // sqlDeleteSubPendingMessages
	"INSERT IGNORE INTO SubsPending (subid, seq) SELECT ?, ? FROM Subscriptions WHERE subid=?",                                          // sqlSubAddPending
	"DELETE FROM SubsPending WHERE subid=? AND seq=?",                                                                                   // sqlSubDeletePending
}

const (
	// This is to detect changes in the tables, etc...
	sqlVersion = 1

	// This is the max int64 value, which we use to say that a message has no expiration set.
	sqlNoExpiration = 0x7FFFFFFFFFFFFFFF

	// When finding out what is the first message to expire, if the SQL query fails,
	// this is the default amount of time the background go routine will wait before
	// attempting to expire messages.
	sqlDefaultExpirationWaitTimeOnError = time.Second

	// Interval at which time is captured.
	sqlDefaultTimeTickInterval = time.Second
)

// These are initialized based on the constants that have reasonable values.
// But for tests, it is often interesting to be able to lower values to
// make tests finish faster.
var (
	sqlExpirationWaitTimeOnError = sqlDefaultExpirationWaitTimeOnError
	sqlTimeTickInterval          = sqlDefaultTimeTickInterval
)

// SQLStore is a factory for message and subscription stores backed by
// a SQL Database.
type SQLStore struct {
	// These are used with atomic operations and need to be 64-bit aligned.
	// Position them at the begining of the structure.
	maxSubID      uint64
	noMsgToExpire int64
	nowInNano     int64

	genericStore
	db           *sql.DB
	maxChannelID int64
	expireTimer  *time.Timer
	doneCh       chan struct{}
	wg           sync.WaitGroup
}

// SQLSubStore is a subscription store backed by an SQL Database
type SQLSubStore struct {
	commonStore
	maxSubID  *uint64 // Points to the uint64 stored in SQLStore and is used with atomic operations
	channelID int64
	db        *sql.DB
	limits    SubStoreLimits
}

// SQLMsgStore is a per channel message store backed by an SQL Database
type SQLMsgStore struct {
	genericMsgStore
	channelID int64
	db        *sql.DB
	sqlStore  *SQLStore // Reference to "parent" store
}

////////////////////////////////////////////////////////////////////////////
// SQLStore methods
////////////////////////////////////////////////////////////////////////////

// NewSQLStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewSQLStore(log logger.Logger, driver, source string, limits *StoreLimits) (Store, error) {
	db, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	s := &SQLStore{
		db:          db,
		doneCh:      make(chan struct{}),
		expireTimer: time.NewTimer(time.Hour),
	}
	if err := s.init(TypeSQL, log, limits); err != nil {
		s.Close()
		return nil, err
	}
	s.wg.Add(2)
	go s.timeTick()
	go s.backgroundTasks()
	return s, nil
}

// Init implements the Store interface
func (s *SQLStore) Init(info *spb.ServerInfo) error {
	s.Lock()
	defer s.Unlock()
	infoBytes, _ := info.Marshal()
	r, err := s.db.Exec(sqlStmts[sqlUpdateServerInfo], info.ClusterID, infoBytes, sqlVersion)
	if err != nil {
		return err
	}
	c, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if c == 0 {
		_, err = s.db.Exec(sqlStmts[sqlAddServerInfo], info.ClusterID, infoBytes, sqlVersion)
	}
	return err
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
	if _, err := s.db.Exec(sqlStmts[sqlAddChannel], cid, channel); err != nil {
		return nil, err
	}
	s.maxChannelID = cid

	msgStore := &SQLMsgStore{db: s.db, channelID: cid, sqlStore: s}
	msgStore.init(channel, s.log, &channelLimits.MsgStoreLimits)

	subStore := &SQLSubStore{db: s.db, channelID: cid, limits: channelLimits.SubStoreLimits}
	subStore.log = s.log
	subStore.maxSubID = &s.maxSubID

	c := &Channel{
		Subs: subStore,
		Msgs: msgStore,
	}
	s.channels[channel] = c

	return c, nil
}

// AddClient implements the Store interface
func (s *SQLStore) AddClient(clientID, hbInbox string) (*Client, error) {
	s.Lock()
	defer s.Unlock()
	var err error
	for i := 0; i < 2; i++ {
		_, err = s.db.Exec(sqlStmts[sqlAddClient], clientID, hbInbox)
		if err == nil {
			break
		}
		// Client may already be in db, so delete now and try again.
		_, err = s.db.Exec(sqlStmts[sqlDeleteClient], clientID)
		if err != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &Client{spb.ClientInfo{ID: clientID, HbInbox: hbInbox}}, nil
}

// DeleteClient implements the Store interface
func (s *SQLStore) DeleteClient(clientID string) error {
	s.Lock()
	_, err := s.db.Exec(sqlStmts[sqlDeleteClient], clientID)
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

// backgroundTasks performs some background tasks such as expiration
// and getting time.Now() every second.
func (s *SQLStore) backgroundTasks() {
	defer s.wg.Done()

	s.RLock()
	timer := s.expireTimer
	s.RUnlock()

	waitTime := s.getWaitTimeBeforeFirstExpiration()
	notify := false

	for {
		// If waitTime is 0, make sure we notify the message stores
		// that we are waiting on an arbitrary time and they should
		// reset our timer when inserting the first message that needs
		// to expire
		if waitTime == 0 {
			waitTime = time.Hour
			notify = true
		}
		timer.Reset(waitTime)
		if notify {
			atomic.CompareAndSwapInt64(&s.noMsgToExpire, 0, 1)
		}
		select {
		case <-s.doneCh:
			return
		case <-timer.C:
			s.expireMsgs()
			waitTime = s.getWaitTimeBeforeFirstExpiration()
		}
	}
}

// getWaitTimeBeforeFirstExpiration returns the amount of time before
// the first message of any message is set to expire.
// Returns 0 if there is no message set to expire, and an arbitrary
// duration if there was an error executing the SQL query.
func (s *SQLStore) getWaitTimeBeforeFirstExpiration() time.Duration {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return 0
	}
	var (
		expiration int64
		waitTime   time.Duration
	)
	r := s.db.QueryRow(sqlStmts[sqlGetFirstMsgToExpire], sqlNoExpiration)
	if err := r.Scan(&expiration); err != nil {
		if err == sql.ErrNoRows {
			return 0
		}
		return sqlExpirationWaitTimeOnError
	}
	// If there is no stored message or no messages is set to expire,
	// return 0, otherwise...
	if expiration > 0 {
		nowInNano := time.Now().UnixNano()
		// Compute wait time.
		if expiration <= nowInNano {
			// Expire asap
			waitTime = time.Nanosecond
		} else {
			// This is the amount of time before some messages are no longer valid
			waitTime = time.Duration(expiration - nowInNano)
		}
	}
	return waitTime
}

// expireMsgs ensures that messages don't stay in the log longer than the
// limit's MaxAge.
func (s *SQLStore) expireMsgs() {
	s.RLock()
	// Refresh view of now
	nowInNano := time.Now().UnixNano()
	// Get all channels that have messages that have expired
	rows, err := s.db.Query(sqlStmts[sqlGetChannelNamesWithExpiredMessages], nowInNano)
	if err != nil {
		s.log.Errorf("Unable get list of channles for message expiration: %v", err)
		s.RUnlock()
		return
	}
	defer rows.Close()
	s.RUnlock()

	var (
		channel string
		ms      *SQLMsgStore
	)

	// Go over all channels
	for rows.Next() {
		channel = ""
		ms = nil
		rows.Scan(&channel)
		s.RLock()
		if s.closed {
			s.RUnlock()
			return
		}
		c := s.channels[channel]
		if c != nil {
			ms = c.Msgs.(*SQLMsgStore)
			ms.Lock()
		}
		s.RUnlock()
		if ms != nil {
			ms.expireMsgsLocked()
			ms.Unlock()
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
	err := s.close()
	db := s.db
	wg := &s.wg
	if s.doneCh != nil {
		// Signal background go-routine to quit
		close(s.doneCh)
	}
	s.Unlock()

	// Wait for go routine(s) to finish
	wg.Wait()

	if db != nil {
		if lerr := db.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// SQLMsgStore methods
////////////////////////////////////////////////////////////////////////////

// Store implements the MsgStore interface
func (ms *SQLMsgStore) Store(data []byte) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	seq := ms.last + 1
	msg := ms.createMsg(seq, data)
	msgBytes, _ := msg.Marshal()

	var (
		maxAge     time.Duration
		expiration int64
	)
	maxAge = ms.limits.MaxAge
	if maxAge > 0 {
		expiration = msg.Timestamp + int64(maxAge)
	} else {
		expiration = sqlNoExpiration
	}
	tx, err := ms.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	dataLen := uint64(len(msgBytes))
	if _, err := tx.Exec(sqlStmts[sqlStoreMsg], ms.channelID, seq, msg.Timestamp, expiration, dataLen, msgBytes); err != nil {
		return 0, err
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

			r := tx.QueryRow(sqlStmts[sqlGetSizeOfMessage], ms.channelID, ms.first)
			delBytes := uint64(0)
			if err := r.Scan(&delBytes); err != nil && err != sql.ErrNoRows {
				return 0, err
			}
			if delBytes > 0 {
				if _, err := tx.Exec(sqlStmts[sqlDeleteMessage], ms.channelID, ms.first); err != nil {
					return 0, err
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

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	if maxAge > 0 && atomic.CompareAndSwapInt64(&ms.sqlStore.noMsgToExpire, 1, 0) {
		ms.sqlStore.expireTimer.Reset(maxAge)
	}

	return seq, nil
}

// Lookup implements the MsgStore interface
func (ms *SQLMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookupLocked(seq)
	ms.Unlock()
	return msg, err
}

func (ms *SQLMsgStore) lookupLocked(seq uint64) (*pb.MsgProto, error) {
	var (
		expiration int64
		data       []byte
	)
	r := ms.db.QueryRow(sqlStmts[sqlLookupMsg], ms.channelID, seq)
	err := r.Scan(&expiration, &data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expiration != sqlNoExpiration && atomic.LoadInt64(&ms.sqlStore.nowInNano) > expiration {
		return nil, nil
	}
	msg := &pb.MsgProto{}
	msg.Unmarshal(data)
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
	r := ms.db.QueryRow(sqlStmts[sqlGetSequenceFromTimestamp], ms.channelID, timestamp)
	seq := uint64(0)
	err := r.Scan(&seq)
	if err == sql.ErrNoRows {
		return ms.last + 1, nil
	}
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// FirstMsg implements the MsgStore interface
func (ms *SQLMsgStore) FirstMsg() (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookupLocked(ms.first)
	ms.Unlock()
	return msg, err
}

// LastMsg implements the MsgStore interface
func (ms *SQLMsgStore) LastMsg() (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookupLocked(ms.last)
	ms.Unlock()
	return msg, err
}

// expireMsgsLocked removes all messages that have expired in this channel.
// Store lock is assumed held on entry
func (ms *SQLMsgStore) expireMsgsLocked() {
	var (
		count     int
		maxSeq    uint64
		totalSize uint64
		err       error
	)
	defer func() {
		if err != nil {
			ms.log.Errorf("Error performing message expiration for channel %q: %v", ms.subject, err)
		}
	}()
	r := ms.db.QueryRow(sqlStmts[sqlGetExpiredMessagesForChannel], ms.channelID, time.Now().UnixNano())
	err = r.Scan(&count, &maxSeq, &totalSize)
	if err != nil {
		return
	}
	// It is possible that target message has been removed due to
	// other limits. So if count is 0, we need to look for the
	// first message to expire. This will be done in the defer
	// function.
	if count == 0 {
		return
	}
	_, err = ms.db.Exec(sqlStmts[sqlDeletedMsgsWithSeqLowerThan], ms.channelID, maxSeq)
	if err != nil {
		return
	}
	ms.first = maxSeq + 1
	ms.totalCount -= count
	ms.totalBytes -= totalSize
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
		r := ss.db.QueryRow(sqlStmts[sqlCheckMaxSubs], ss.channelID)
		count := 0
		if err := r.Scan(&count); err != nil {
			return err
		}
		if count >= ss.limits.MaxSubscriptions {
			return ErrTooManySubs
		}
	}
	subID := atomic.AddUint64(ss.maxSubID, 1)
	subBytes, _ := sub.Marshal()
	if _, err := ss.db.Exec(sqlStmts[sqlCreateSub], ss.channelID, subID, subBytes); err != nil {
		return err
	}
	sub.ID = subID
	return nil
}

// UpdateSub implements the SubStore interface
func (ss *SQLSubStore) UpdateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()
	subBytes, _ := sub.Marshal()
	_, err := ss.db.Exec(sqlStmts[sqlUpdateSub], subBytes, ss.channelID, sub.ID)
	return err
}

// DeleteSub implements the SubStore interface
func (ss *SQLSubStore) DeleteSub(subid uint64) error {
	ss.Lock()
	defer ss.Unlock()
	if subid == atomic.LoadUint64(ss.maxSubID) {
		if _, err := ss.db.Exec(sqlStmts[sqlMarkSubscriptionAsDeleted], ss.channelID, subid); err != nil {
			return err
		}
	} else {
		if _, err := ss.db.Exec(sqlStmts[sqlDeleteSubscription], ss.channelID, subid); err != nil {
			return err
		}
	}
	// Ignore error on this since subscription would not be recovered
	// if above executed ok.
	ss.db.Exec(sqlStmts[sqlDeleteSubPendingMessages], subid)
	return nil
}

// AddSeqPending implements the SubStore interface
func (ss *SQLSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	_, err := ss.db.Exec(sqlStmts[sqlSubAddPending], subid, seqno, subid)
	ss.Unlock()
	return err
}

// AckSeqPending implements the SubStore interface
func (ss *SQLSubStore) AckSeqPending(subid, seqno uint64) error {
	ss.Lock()
	_, err := ss.db.Exec(sqlStmts[sqlSubDeletePending], subid, seqno)
	ss.Unlock()
	return err
}

// Flush implements the SubStore interface
func (ss *SQLSubStore) Flush() error {
	return nil
}

// Close implements the SubStore interface
func (ss *SQLSubStore) Close() error {
	return nil
}
