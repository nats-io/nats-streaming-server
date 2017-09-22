// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
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
	driverMySQL    = "mysql"
	driverPostgres = "postgres"
)

const (
	sqlHasServerInfoRow = iota
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
	sqlSubAddPending
	sqlSubDeletePending
	sqlRecoverServerInfo
	sqlRecoverClients
	sqlRecoverMaxChannelID
	sqlRecoverMaxSubID
	sqlRecoverChannelsList
	sqlRecoverChannelMsgs
	sqlRecoverChannelSubs
	sqlRecoverDoPurgeSubsPending
	sqlRecoverSubPendingSeqs
	sqlRecoverGetChannelLimits
	sqlRecoverDoExpireMsgs
	sqlRecoverGetMessagesCount
	sqlRecoverGetSeqFloorForMaxMsgs
	sqlRecoverGetChannelTotalSize
	sqlRecoverGetSeqFloorForMaxBytes
	sqlRecoverUpdateChannelLimits
)

var sqlStmts = []string{
	"SELECT COUNT(uniquerow) FROM ServerInfo",                                                                                                                                        // sqlHasServerInfoRow
	"UPDATE ServerInfo SET id=?, proto=?, version=? WHERE uniquerow=1",                                                                                                               // sqlUpdateServerInfo
	"INSERT INTO ServerInfo (id, proto, version) VALUES (?, ?, ?)",                                                                                                                   // sqlAddServerInfo
	"INSERT INTO Clients (id, hbinbox) VALUES (?, ?)",                                                                                                                                // sqlAddClient
	"DELETE FROM Clients WHERE id=?",                                                                                                                                                 // sqlDeleteClient
	"INSERT INTO Channels (id, name, maxmsgs, maxbytes, maxage) VALUES (?, ?, ?, ?, ?)",                                                                                              // sqlAddChannel
	"INSERT INTO Messages VALUES (?, ?, ?, ?, ?)",                                                                                                                                    // sqlStoreMsg
	"SELECT timestamp, data FROM Messages WHERE id=? AND seq=?",                                                                                                                      // sqlLookupMsg
	"SELECT seq FROM Messages WHERE id=? AND timestamp>=? LIMIT 1",                                                                                                                   // sqlGetSequenceFromTimestamp
	"UPDATE Channels SET maxseq=? WHERE id=?",                                                                                                                                        // sqlUpdateChannelMaxSeq
	"SELECT COUNT(seq), COALESCE(MAX(seq), 0), COALESCE(SUM(size), 0) FROM Messages WHERE id=? AND timestamp<=?",                                                                     // sqlGetExpiredMessages
	"SELECT timestamp FROM Messages WHERE id=? AND seq>=? LIMIT 1",                                                                                                                   // sqlGetFirstMsgTimestamp
	"DELETE FROM Messages WHERE id=? AND seq<=?",                                                                                                                                     // sqlDeletedMsgsWithSeqLowerThan
	"SELECT size FROM Messages WHERE id=? AND seq=?",                                                                                                                                 // sqlGetSizeOfMessage
	"DELETE FROM Messages WHERE id=? AND seq=?",                                                                                                                                      // sqlDeleteMessage
	"SELECT COUNT(subid) FROM Subscriptions WHERE id=? AND deleted=FALSE",                                                                                                            // sqlCheckMaxSubs
	"INSERT INTO Subscriptions (id, subid, proto) VALUES (?, ?, ?)",                                                                                                                  // sqlCreateSub
	"UPDATE Subscriptions SET proto=? WHERE id=? AND subid=?",                                                                                                                        // sqlUpdateSub
	"UPDATE Subscriptions SET deleted=TRUE WHERE id=? AND subid=?",                                                                                                                   // sqlMarkSubscriptionAsDeleted
	"DELETE FROM Subscriptions WHERE id=? AND subid=?",                                                                                                                               // sqlDeleteSubscription
	"DELETE FROM Subscriptions WHERE id=? AND deleted=TRUE",                                                                                                                          // sqlDeleteSubMarkedAsDeleted
	"DELETE FROM SubsPending WHERE subid=?",                                                                                                                                          // sqlDeleteSubPendingMessages
	"INSERT IGNORE INTO SubsPending (subid, seq) SELECT ?, ? FROM Subscriptions WHERE subid=?",                                                                                       // sqlSubAddPending
	"DELETE FROM SubsPending WHERE subid=? AND seq=?",                                                                                                                                // sqlSubDeletePending
	"SELECT id, proto, version FROM ServerInfo WHERE uniquerow=1",                                                                                                                    // sqlRecoverServerInfo
	"SELECT id, hbinbox FROM Clients",                                                                                                                                                // sqlRecoverClients
	"SELECT COALESCE(MAX(id), 0) FROM Channels",                                                                                                                                      // sqlRecoverMaxChannelID
	"SELECT COALESCE(MAX(subid), 0) FROM Subscriptions",                                                                                                                              // sqlRecoverMaxSubID
	"SELECT id, name, maxseq FROM Channels WHERE deleted=FALSE",                                                                                                                      // sqlRecoverChannelsList
	"SELECT COUNT(seq), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0), COALESCE(SUM(size), 0), COALESCE(MAX(timestamp), 0) FROM Messages WHERE id=?",                                  // sqlRecoverChannelMsgs
	"SELECT proto FROM Subscriptions WHERE id=? AND deleted=FALSE",                                                                                                                   // sqlRecoverChannelSubs
	"DELETE FROM SubsPending WHERE subid=? AND seq<?",                                                                                                                                // sqlRecoverDoPurgeSubsPending
	"SELECT seq FROM SubsPending WHERE subid=?",                                                                                                                                      // sqlRecoverSubPendingSeqs
	"SELECT maxmsgs, maxbytes, maxage FROM Channels WHERE id=?",                                                                                                                      // sqlRecoverGetChannelLimits
	"DELETE FROM Messages WHERE id=? AND timestamp<=?",                                                                                                                               // sqlRecoverDoExpireMsgs
	"SELECT COUNT(seq) FROM Messages WHERE id=?",                                                                                                                                     // sqlRecoverGetMessagesCount
	"SELECT MIN(t.seq) FROM (SELECT seq FROM Messages WHERE id=? ORDER BY seq DESC LIMIT ?)t",                                                                                        // sqlRecoverGetSeqFloorForMaxMsgs
	"SELECT COALESCE(SUM(size), 0) FROM Messages WHERE id=?",                                                                                                                         // sqlRecoverGetChannelTotalSize
	"SELECT COALESCE(MIN(seq), 0) FROM (SELECT seq, (SELECT SUM(size) FROM Messages WHERE id=? AND seq<=t.seq) AS total FROM Messages t WHERE id=? ORDER BY seq)t2 WHERE t2.total>?", // sqlRecoverGetSeqFloorForMaxBytes
	"UPDATE Channels SET maxmsgs=?, maxbytes=?, maxage=? WHERE id=?",                                                                                                                 // sqlRecoverUpdateChannelLimits
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
)

// These are initialized based on the constants that have reasonable values.
// But for tests, it is often interesting to be able to lower values to
// make tests finish faster.
var (
	sqlExpirationIntervalOnError = sqlDefaultExpirationIntervalOnError
	sqlTimeTickInterval          = sqlDefaultTimeTickInterval
)

// SQLStoreOptions are used to configure the SQL Store.
type SQLStoreOptions struct {
	Driver string
	Source string
}

// SQLStore is a factory for message and subscription stores backed by
// a SQL Database.
type SQLStore struct {
	// These are used with atomic operations and need to be 64-bit aligned.
	// Position them at the beginning of the structure.
	maxSubID  uint64
	nowInNano int64

	genericStore
	db           *sql.DB
	maxChannelID int64
	doneCh       chan struct{}
	wg           sync.WaitGroup
}

// SQLSubStore is a subscription store backed by an SQL Database
type SQLSubStore struct {
	commonStore
	maxSubID       *uint64 // Points to the uint64 stored in SQLStore and is used with atomic operations
	channelID      int64
	db             *sql.DB
	limits         SubStoreLimits
	hasMarkedAsDel bool
}

// SQLMsgStore is a per channel message store backed by an SQL Database
type SQLMsgStore struct {
	genericMsgStore
	channelID   int64
	db          *sql.DB
	sqlStore    *SQLStore // Reference to "parent" store
	expireTimer *time.Timer
	wg          sync.WaitGroup
}

// sqlStmtError returns an error including the text of the offending SQL statement.
func sqlStmtError(code int, err error) error {
	return fmt.Errorf("sql: error executing %q: %v", sqlStmts[code], err)
}

////////////////////////////////////////////////////////////////////////////
// SQLStore methods
////////////////////////////////////////////////////////////////////////////

// NewSQLStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewSQLStore(log logger.Logger, driver, source string, limits *StoreLimits) (*SQLStore, error) {
	initSQLStmts.Do(func() { initSQLStmtsTable(driver) })
	db, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	s := &SQLStore{
		db:     db,
		doneCh: make(chan struct{}),
	}
	if err := s.init(TypeSQL, log, limits); err != nil {
		s.Close()
		return nil, err
	}
	s.wg.Add(1)
	go s.timeTick()
	return s, nil
}

// initialize the global sqlStmts table to driver's one.
func initSQLStmtsTable(driver string) {
	// The sqlStmts table is initialized with MySQL statements.
	// Update the statements for the selected driver.
	switch driver {
	case driverPostgres:
		// Support for INSERT IGNORE is for Postgres 9.5+
		sqlStmts[sqlSubAddPending] = "INSERT INTO SubsPending (subid, seq) SELECT $1, $2 FROM Subscriptions WHERE subid=$3 AND NOT EXISTS (SELECT 1 FROM SubsPending WHERE subid=$1 AND seq=$2)"
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
		)
		if err := cliRows.Scan(&clientID, &hbInbox); err != nil {
			return nil, err
		}
		clients = append(clients, &Client{spb.ClientInfo{ID: clientID, HbInbox: hbInbox}})
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

		msgStore := &SQLMsgStore{db: s.db, channelID: channelID, sqlStore: s}
		msgStore.init(name, s.log, &channelLimits.MsgStoreLimits)

		if err := s.applyLimitsOnRecovery(msgStore); err != nil {
			return nil, err
		}

		r = s.db.QueryRow(sqlStmts[sqlRecoverChannelMsgs], channelID)
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
		msgStore.lTimestamp = lastTimestamp
		// If all messages have expired, the above should all be 0, however,
		// the Channel table may contain a maxseq that we should use as starting
		// point.
		if msgStore.last == 0 {
			msgStore.first = maxseq
			msgStore.last = maxseq
		}

		subStore := &SQLSubStore{db: s.db, channelID: channelID, limits: channelLimits.SubStoreLimits}
		subStore.log = s.log
		subStore.maxSubID = &s.maxSubID

		var subscriptions []*RecoveredSubscription

		subRows, err := s.db.Query(sqlStmts[sqlRecoverChannelSubs], channelID)
		if err != nil {
			return nil, sqlStmtError(sqlRecoverChannelSubs, err)
		}
		defer subRows.Close()
		for subRows.Next() {
			var protoBytes []byte
			if err := subRows.Scan(&protoBytes); err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if protoBytes != nil {
				sub := &spb.SubState{}
				if err := sub.Unmarshal(protoBytes); err != nil {
					return nil, err
				}

				// We can remove entries for sequence that are below the smallest
				// sequence that was found in Messages.
				if _, err := s.db.Exec(sqlStmts[sqlRecoverDoPurgeSubsPending], sub.ID, msgStore.first); err != nil {
					return nil, sqlStmtError(sqlRecoverDoPurgeSubsPending, err)
				}

				var pendingAcks PendingAcks
				pendingSeqRows, err := s.db.Query(sqlStmts[sqlRecoverSubPendingSeqs], sub.ID)
				if err != nil {
					return nil, sqlStmtError(sqlRecoverSubPendingSeqs, err)
				}
				defer pendingSeqRows.Close()
				for pendingSeqRows.Next() {
					var seq uint64
					if err := pendingSeqRows.Scan(&seq); err != nil && err != sql.ErrNoRows {
						return nil, err
					}
					if seq > 0 {
						// Update subscription's LastSent based on the highest recoveverd sequence number.
						if seq > sub.LastSent {
							sub.LastSent = seq
						}
						if pendingAcks == nil {
							pendingAcks = make(PendingAcks)
						}
						pendingAcks[seq] = struct{}{}
					}
				}
				pendingSeqRows.Close()

				// Add to the recovered subscriptions
				subscriptions = append(subscriptions, &RecoveredSubscription{Sub: sub, Pending: pendingAcks})
			}
		}
		subRows.Close()

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
	r := s.db.QueryRow(sqlStmts[sqlRecoverGetChannelLimits], ms.channelID)
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
		if _, err := s.db.Exec(sqlStmts[sqlRecoverDoExpireMsgs], ms.channelID, expiredTimestamp); err != nil {
			return sqlStmtError(sqlRecoverDoExpireMsgs, err)
		}
	}
	// For MaxMsgs and MaxBytes we are interested only the new limit is
	// lower than the old one (since messages are removed during runtime,
	// if the limit has not been lowered, we should be good).
	if limits.MaxMsgs > 0 && limits.MaxMsgs < storedMsgsLimit {
		count := 0
		r := s.db.QueryRow(sqlStmts[sqlRecoverGetMessagesCount], ms.channelID)
		if err := r.Scan(&count); err != nil {
			return sqlStmtError(sqlRecoverGetMessagesCount, err)
		}
		// We leave at least 1 message
		if count > 1 && count > limits.MaxMsgs {
			seq := uint64(0)
			r = s.db.QueryRow(sqlStmts[sqlRecoverGetSeqFloorForMaxMsgs], ms.channelID, limits.MaxMsgs)
			if err := r.Scan(&seq); err != nil {
				return sqlStmtError(sqlRecoverGetSeqFloorForMaxMsgs, err)
			}
			if _, err := s.db.Exec(sqlStmts[sqlDeletedMsgsWithSeqLowerThan], ms.channelID, seq-1); err != nil {
				return sqlStmtError(sqlDeletedMsgsWithSeqLowerThan, err)
			}
		}
	}
	if limits.MaxBytes > 0 && limits.MaxBytes < storedBytesLimit {
		currentBytes := uint64(0)
		r := s.db.QueryRow(sqlStmts[sqlRecoverGetChannelTotalSize], ms.channelID)
		if err := r.Scan(&currentBytes); err != nil {
			return sqlStmtError(sqlRecoverGetChannelTotalSize, err)
		}
		if currentBytes > uint64(limits.MaxBytes) {
			// How much do we need to get rid off
			removeBytes := currentBytes - uint64(limits.MaxBytes)
			seq := 0
			r := s.db.QueryRow(sqlStmts[sqlRecoverGetSeqFloorForMaxBytes], ms.channelID, ms.channelID, removeBytes)
			if err := r.Scan(&seq); err != nil {
				return sqlStmtError(sqlRecoverGetSeqFloorForMaxBytes, err)
			}
			// Leave at least 1 record
			if seq > 0 {
				if _, err := s.db.Exec(sqlStmts[sqlDeletedMsgsWithSeqLowerThan], ms.channelID, seq); err != nil {
					return sqlStmtError(sqlDeletedMsgsWithSeqLowerThan, err)
				}
			}
		}
	}
	// If limits were changed compared to last run, we need to update the
	// Channels table.
	if needUpdate {
		if _, err := s.db.Exec(sqlStmts[sqlRecoverUpdateChannelLimits],
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
	if _, err := s.db.Exec(sqlStmts[sqlAddChannel], cid, channel,
		channelLimits.MaxMsgs, channelLimits.MaxBytes, int64(channelLimits.MaxAge)); err != nil {
		return nil, sqlStmtError(sqlAddChannel, err)
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
		// We stop if this is the second AddClient failed attempt.
		if i > 0 {
			err = sqlStmtError(sqlAddClient, err)
			break
		}
		// This is the first AddClient failed attempt. It could be because
		// client was already in db, so delete now and try again.
		_, err = s.db.Exec(sqlStmts[sqlDeleteClient], clientID)
		if err != nil {
			err = sqlStmtError(sqlDeleteClient, err)
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

	dataLen := uint64(len(msgBytes))
	if _, err := ms.db.Exec(sqlStmts[sqlStoreMsg], ms.channelID, seq, msg.Timestamp, dataLen, msgBytes); err != nil {
		return 0, sqlStmtError(sqlStoreMsg, err)
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

			r := ms.db.QueryRow(sqlStmts[sqlGetSizeOfMessage], ms.channelID, ms.first)
			delBytes := uint64(0)
			if err := r.Scan(&delBytes); err != nil && err != sql.ErrNoRows {
				return 0, sqlStmtError(sqlGetSizeOfMessage, err)
			}
			if delBytes > 0 {
				if _, err := ms.db.Exec(sqlStmts[sqlDeleteMessage], ms.channelID, ms.first); err != nil {
					return 0, sqlStmtError(sqlDeleteMessage, err)
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

	if ms.limits.MaxAge > 0 && ms.expireTimer == nil {
		ms.wg.Add(1)
		ms.expireTimer = time.AfterFunc(ms.limits.MaxAge, ms.expireMsgs)
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
		timestamp int64
		data      []byte
	)
	r := ms.db.QueryRow(sqlStmts[sqlLookupMsg], ms.channelID, seq)
	err := r.Scan(&timestamp, &data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, sqlStmtError(sqlLookupMsg, err)
	}
	if maxAge := int64(ms.limits.MaxAge); maxAge > 0 && atomic.LoadInt64(&ms.sqlStore.nowInNano) > timestamp+maxAge {
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
		return 0, sqlStmtError(sqlGetSequenceFromTimestamp, err)
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
		r := ms.db.QueryRow(sqlStmts[sqlGetExpiredMessages], ms.channelID, expiredTimestamp)
		if err := r.Scan(&count, &maxSeq, &totalSize); err != nil {
			processErr(sqlGetExpiredMessages, err)
			return
		}
		// It could be that messages that should have expired have been
		// removed due to count/size limit. We still need to adjust the
		// expiration timer based on the first message that need to expire.
		if count > 0 {
			if maxSeq == ms.last {
				if _, err := ms.db.Exec(sqlStmts[sqlUpdateChannelMaxSeq], maxSeq, ms.channelID); err != nil {
					processErr(sqlUpdateChannelMaxSeq, err)
					return
				}
			}
			if _, err := ms.db.Exec(sqlStmts[sqlDeletedMsgsWithSeqLowerThan], ms.channelID, maxSeq); err != nil {
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
			r = ms.db.QueryRow(sqlStmts[sqlGetFirstMsgTimestamp], ms.channelID, ms.first)
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

// Close implements the MsgStore interface
func (ms *SQLMsgStore) Close() error {
	ms.Lock()
	if ms.closed {
		ms.Unlock()
		return nil
	}
	ms.closed = true
	if ms.expireTimer != nil {
		if ms.expireTimer.Stop() {
			ms.wg.Done()
		}
	}
	ms.Unlock()

	ms.wg.Wait()
	return nil
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
			return sqlStmtError(sqlCheckMaxSubs, err)
		}
		if count >= ss.limits.MaxSubscriptions {
			return ErrTooManySubs
		}
	}
	sub.ID = atomic.AddUint64(ss.maxSubID, 1)
	subBytes, _ := sub.Marshal()
	if _, err := ss.db.Exec(sqlStmts[sqlCreateSub], ss.channelID, sub.ID, subBytes); err != nil {
		sub.ID = 0
		return sqlStmtError(sqlCreateSub, err)
	}
	if ss.hasMarkedAsDel {
		if _, err := ss.db.Exec(sqlStmts[sqlDeleteSubMarkedAsDeleted], ss.channelID); err != nil {
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
	r, err := ss.db.Exec(sqlStmts[sqlUpdateSub], subBytes, ss.channelID, sub.ID)
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
		if _, err := ss.db.Exec(sqlStmts[sqlCreateSub], ss.channelID, sub.ID, subBytes); err != nil {
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
		if _, err := ss.db.Exec(sqlStmts[sqlMarkSubscriptionAsDeleted], ss.channelID, subid); err != nil {
			return sqlStmtError(sqlMarkSubscriptionAsDeleted, err)
		}
		ss.hasMarkedAsDel = true
	} else {
		if _, err := ss.db.Exec(sqlStmts[sqlDeleteSubscription], ss.channelID, subid); err != nil {
			return sqlStmtError(sqlDeleteSubscription, err)
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
	if err != nil {
		err = sqlStmtError(sqlSubAddPending, err)
	}
	ss.Unlock()
	return err
}

// AckSeqPending implements the SubStore interface
func (ss *SQLSubStore) AckSeqPending(subid, seqno uint64) error {
	ss.Lock()
	_, err := ss.db.Exec(sqlStmts[sqlSubDeletePending], subid, seqno)
	if err != nil {
		err = sqlStmtError(sqlSubDeletePending, err)
	}
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
