// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"bufio"
	"github.com/nats-io/go-stan/pb"
	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan-server/util"
)

const (
	// Our file version.
	fileVersion = 1

	// Number of files for a MsgStore on a given channel.
	numFiles = 5

	// Name of the subscriptions file.
	subsFileName = "subs.dat"

	// Name of the clients file.
	clientsFileName = "clients.dat"

	// record header size of a subscription:
	// 4 bytes: 1 byte for type, 3 bytes for buffer size
	subRecordHeaderSize = 4
)

// Type for the records in the subscriptions file
type subRecordType byte

// Subscription protobuf do not share a common interface, yet, when saving a
// subscription on disk, we have to get the size and marshal the record in
// a buffer. These methods are available in all the protobuf.
// So we create this interface with those two methods to be used by the
// writeRecord method.
type subRecord interface {
	Size() int
	MarshalTo([]byte) (int, error)
}

// Various record types
const (
	subRecNew = subRecordType(iota)
	subRecDel
	subRecAck
	subRecMsg
)

// Actions for client store
const (
	addClient = "A"
	delClient = "D"
)

// FileStore is the storage interface for STAN servers, backed by files.
type FileStore struct {
	genericStore
	rootDir     string
	clientsFile *os.File
}

type recoveredSub struct {
	sub    *spb.SubState
	seqnos map[uint64]struct{}
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	genericSubStore
	tmpSubBuf []byte
	file      *os.File
}

// fileSlice represents one of the message store file (there are a number
// of files for a MsgStore on a given channel).
type fileSlice struct {
	fileName  string
	firstMsg  *pb.MsgProto
	lastMsg   *pb.MsgProto
	msgsCount int
	msgsSize  uint64
}

// FileMsgStore is a per channel message file store.
type FileMsgStore struct {
	genericMsgStore
	tmpMsgBuf    []byte
	file         *os.File
	files        [numFiles]*fileSlice
	currSliceIdx int
}

// openFile opens the file (and create it if needed). If the file exists,
// it checks that the version is supported.
func openFile(fileName string) (*os.File, error) {
	checkVersion := false

	// Check if file already exists
	if s, err := os.Stat(fileName); s != nil && err == nil {
		checkVersion = true
	}
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	if checkVersion {
		err = checkFileVersion(file)
	} else {
		// This is a new file, write our file version
		err = util.WriteInt(file, fileVersion)
	}
	if err != nil {
		file.Close()
		file = nil
	}
	return file, err
}

// check that the version of the file is understood by this interface
func checkFileVersion(r io.Reader) error {
	fv, err := util.ReadInt(r)
	if err != nil {
		return fmt.Errorf("unable to verify file version: %v", err)
	}
	if fv == 0 || fv > fileVersion {
		return fmt.Errorf("unsupported file version: %v (supports [1..%v])", fv, fileVersion)
	}
	return nil
}

// write the header in the given buffer.
func writeHeader(buf []byte, recType subRecordType, recordSize int) {
	if recordSize > 0xFFFFFF {
		panic("record size too big")
	}
	header := int(recType)<<24 | recordSize
	util.ByteOrder.PutUint32(buf, uint32(header))
}

////////////////////////////////////////////////////////////////////////////
// FileStore methods
////////////////////////////////////////////////////////////////////////////

// NewFileStore returns a factory for stores backed by files, and recovers
// any state present.
// If not limits are provided, the store will be created with
// DefaultChannelLimits.
func NewFileStore(rootDir string, limits *ChannelLimits) (*FileStore, *RecoveredState, error) {
	fs := &FileStore{
		rootDir: rootDir,
	}
	fs.init(TypeFile, limits)

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, nil, fmt.Errorf("unable to create the root directory [%s]: %v", rootDir, err)
	}

	var err error
	var msgStore *FileMsgStore
	var subStore *FileSubStore
	var subs map[uint64]*recoveredSub

	recoveredState := &RecoveredState{
		Clients: make([]*RecoveredClient, 0, 16),
		Subs:    make(RecoveredSubscriptions),
	}

	// Recover the clients file
	err = fs.recoverClients(&recoveredState.Clients)
	if err != nil {
		// If the file was opened/created, make sure we close it here.
		if fs.clientsFile != nil {
			fs.clientsFile.Close()
		}
		return nil, nil, err
	}

	// Get the channels (there are subdirectories of rootDir)
	var channels []os.FileInfo
	channels, err = ioutil.ReadDir(rootDir)
	if err != nil {
		return nil, nil, err
	}
	// Go through the list
	for _, c := range channels {
		// Channels are directories. Ignore simple files
		if !c.IsDir() {
			continue
		}

		channel := c.Name()
		channelDirName := filepath.Join(rootDir, channel)

		// Recover messages for this channel
		msgStore, err = newFileMsgStore(channelDirName, channel, fs.limits, true)
		if err != nil {
			break
		}
		subStore, subs, err = newFileSubStore(channelDirName, channel, fs.limits, true)
		if err != nil {
			msgStore.Close()
			break
		}

		// For this channel, construct an array of RecoveredSubState
		rssArray := make([]*RecoveredSubState, 0, len(subs))

		// Fill that array with what we got from newFileSubStore.
		for _, sub := range subs {
			rss := &RecoveredSubState{
				Sub:     sub.sub,
				Pending: make(PendingAcks),
			}
			// If we recovered any seqno...
			if len(sub.seqnos) > 0 {
				// Lookup messages, and if we find those, update the
				// Pending map.
				for seq := range sub.seqnos {
					// Access directly 'msgs' here. If we have a
					// different implementation where we don't
					// keep messages around, we would still have
					// a cache of messages per channel that will
					// then be cleared after this loop when we
					// are done restoring the subscriptions.
					if m := msgStore.msgs[seq]; m != nil {
						rss.Pending[seq] = m
					}
				}
			}
			// Add to the array of recovered subscriptions
			rssArray = append(rssArray, rss)
		}

		// This is the recovered subscription state for this channel
		recoveredState.Subs[channel] = rssArray

		fs.channels[channel] = &ChannelStore{
			Subs: subStore,
			Msgs: msgStore,
		}
	}
	if err != nil {
		// Make sure we close to ensure that all files that have been
		// successfully opened are properly closed.
		fs.Close()
		fs = nil
		recoveredState = nil
		return nil, nil, err
	}

	return fs, recoveredState, nil
}

// recoverClients reads the client files and fills the given array with RecoveredClient
func (fs *FileStore) recoverClients(clients *[]*RecoveredClient) error {
	clientsMap := make(map[string]*RecoveredClient)

	var err error

	fileName := filepath.Join(fs.rootDir, clientsFileName)
	fs.clientsFile, err = openFile(fileName)
	if err != nil {
		return err
	}

	var action, clientID, hbInbox string

	scanner := bufio.NewScanner(fs.clientsFile)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Sscanf(line, "%s %s %s", &action, &clientID, &hbInbox)
		switch action {
		case addClient:
			if clientID == "" {
				return fmt.Errorf("missing client ID in ADD client instruction")
			}
			if hbInbox == "" {
				return fmt.Errorf("missing client heartbeat inbox in ADD client instruction")
			}
			c := &RecoveredClient{ClientID: clientID, HbInbox: hbInbox}
			// Add to the map. Note that if one already exists, which should
			// not, just replace with this most recent one.
			clientsMap[clientID] = c
		case delClient:
			if clientID == "" {
				return fmt.Errorf("missing client ID in DELETE client instruction")
			}
			delete(clientsMap, clientID)
		default:
			return fmt.Errorf("invalid client action %q", action)
		}
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	// Convert the map into an array
	for _, c := range clientsMap {
		*clients = append(*clients, c)
	}
	return nil
}

// LookupOrCreateChannel returns a ChannelStore for the given channel,
// creates one if no such channel exists. In this case, the returned
// boolean will be true.
func (fs *FileStore) LookupOrCreateChannel(channel string) (*ChannelStore, bool, error) {
	channelStore := fs.LookupChannel(channel)
	if channelStore != nil {
		return channelStore, false, nil
	}

	// Two "threads" could make this call and end-up deciding to create the
	// channel. So we need to test again, this time under the write lock.
	fs.Lock()
	defer fs.Unlock()
	channelStore = fs.channels[channel]
	if channelStore != nil {
		return channelStore, false, nil
	}

	// Check for limits
	if err := fs.canAddChannel(); err != nil {
		return nil, false, err
	}

	// We create the channel here...

	channelDirName := filepath.Join(fs.rootDir, channel)
	if err := os.MkdirAll(channelDirName, os.ModeDir+os.ModePerm); err != nil {
		return nil, false, err
	}

	var err error
	var msgStore MsgStore
	var subStore SubStore

	msgStore, err = newFileMsgStore(channelDirName, channel, fs.limits, false)
	if err != nil {
		return nil, false, err
	}
	subStore, _, err = newFileSubStore(channelDirName, channel, fs.limits, false)
	if err != nil {
		msgStore.Close()
		return nil, false, err
	}

	channelStore = &ChannelStore{
		Subs: subStore,
		Msgs: msgStore,
	}

	fs.channels[channel] = channelStore

	return channelStore, true, nil
}

// AddClient stores information about the client identified by `clientID`.
func (fs *FileStore) AddClient(clientID, hbInbox string) error {
	line := fmt.Sprintf("%s %s %s\r\n", addClient, clientID, hbInbox)
	fs.Lock()
	defer fs.Unlock()
	if _, err := fs.clientsFile.WriteString(line); err != nil {
		return err
	}
	return nil
}

// DeleteClient invalidates the client identified by `clientID`.
func (fs *FileStore) DeleteClient(clientID string) {
	line := fmt.Sprintf("%s %s\r\n", delClient, clientID)
	fs.Lock()
	defer fs.Unlock()
	fs.clientsFile.WriteString(line)
}

// Close closes all stores.
func (fs *FileStore) Close() error {
	fs.Lock()
	defer fs.Unlock()
	if fs.closed {
		return nil
	}
	fs.closed = true

	var err error

	err = fs.genericStore.close()
	if fs.clientsFile != nil {
		if lerr := fs.clientsFile.Close(); lerr != nil {
			if err == nil {
				err = lerr
			}
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileMsgStore methods
////////////////////////////////////////////////////////////////////////////

// newFileMsgStore returns a new instace of a file MsgStore.
func newFileMsgStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileMsgStore, error) {
	var err error
	var file *os.File

	// Create an instance and initialize
	ms := &FileMsgStore{}
	ms.init(channel, limits)

	// Open/create all the files
	for i := 0; i < numFiles; i++ {
		// Fully qualified file name.
		fileName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.dat", (i+1)))

		// Open the file.
		file, err = openFile(fileName)
		if err != nil {
			break
		}
		// Save slice
		ms.files[i] = &fileSlice{fileName: fileName}

		// Should we try to recover (startup case)
		if doRecover {
			err = ms.recoverOneMsgFile(file, i)
		} else if i == 0 {
			// Otherwise, keep the first file opened...
			ms.file = file
		} else {
			// and close all others.
			err = file.Close()
		}
		if err != nil {
			break
		}
	}
	// Cleanup on error
	if err != nil {
		ms.Close()
		ms = nil
		action := "create"
		if doRecover {
			action = "recover"
		}
		err = fmt.Errorf("unable to %s message store for [%s]: %v", action, channel, err)
		return nil, err
	}
	return ms, nil
}

// recovers one of the file
func (ms *FileMsgStore) recoverOneMsgFile(file *os.File, numFile int) error {
	var err error

	msgSize := 0
	var msg *pb.MsgProto

	fslice := ms.files[numFile]

	for {
		// Read the message size as an int32
		msgSize, err = util.ReadInt(file)
		if err != nil {
			if err == io.EOF {
				// We are done, reset err
				err = nil
				break
			} else {
				return err
			}
		}

		// Expand buffer if necessary
		ms.tmpMsgBuf = util.EnsureBufBigEnough(ms.tmpMsgBuf, msgSize)

		// Read fully the expected number of bytes for this message
		_, err = io.ReadFull(file, ms.tmpMsgBuf[:msgSize])
		if err != nil {
			return err
		}
		// Recover this message
		msg = &pb.MsgProto{}
		err = msg.Unmarshal(ms.tmpMsgBuf[:msgSize])
		if err != nil {
			return err
		}

		// Mark this message as redelivered.
		msg.Redelivered = true

		if fslice.firstMsg == nil {
			fslice.firstMsg = msg
		}
		fslice.lastMsg = msg
		fslice.msgsCount++
		fslice.msgsSize += uint64(len(msg.Data))

		if ms.first == 0 {
			ms.first = msg.Sequence
		}
		ms.msgs[msg.Sequence] = msg
	}

	// Do more accounting and bump the current slice index if we recovered
	// at least one message on that file.
	if fslice.msgsCount > 0 {
		ms.last = fslice.lastMsg.Sequence
		ms.totalCount += fslice.msgsCount
		ms.totalBytes += fslice.msgsSize
		ms.currSliceIdx = numFile

		// Close the previous file
		if numFile > 0 {
			err = ms.file.Close()
			ms.file = nil
		}
	}
	// Keep the file opened if this is the first or messages were recovered
	if err == nil && (fslice.msgsCount > 0 || numFile == 0) {
		ms.file = file
	} else {
		// Close otherwise...
		err = file.Close()
	}
	return err
}

// Store a given message.
func (ms *FileMsgStore) Store(reply string, data []byte) (*pb.MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	fslice := ms.files[ms.currSliceIdx]

	seq := ms.last + 1
	m := &pb.MsgProto{
		Sequence:  seq,
		Subject:   ms.subject,
		Reply:     reply,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}

	// This is the size needed to store the marshalled message.
	bufSize := m.Size()

	// This is the size needed to store the size + buffer
	totalSize := 4 + bufSize

	// Alloc or realloc if needed
	ms.tmpMsgBuf = util.EnsureBufBigEnough(ms.tmpMsgBuf, totalSize)

	// Write the size in the buffer
	util.ByteOrder.PutUint32(ms.tmpMsgBuf[0:4], uint32(bufSize))

	// Marshal into the given buffer
	if _, err := m.MarshalTo(ms.tmpMsgBuf[4:totalSize]); err != nil {
		return nil, err
	}
	// Write the buffer
	if _, err := ms.file.Write(ms.tmpMsgBuf[:totalSize]); err != nil {
		return nil, err
	}

	if ms.first == 0 {
		ms.first = 1
	}
	ms.last = seq
	ms.msgs[ms.last] = m

	msgSize := uint64(len(data))

	// Total stats
	ms.totalCount++
	ms.totalBytes += msgSize

	// Stats per file slice
	fslice.msgsCount++
	fslice.msgsSize += msgSize

	// Save references to first and last message for this slice
	if fslice.firstMsg == nil {
		fslice.firstMsg = m
	}
	fslice.lastMsg = m

	// Check to see if we should move to next slice.
	// FIXME(ik): Need to check total counts first? Not sure about
	// what to do here.
	if (fslice.msgsCount >= ms.limits.MaxNumMsgs/(numFiles-1)) ||
		(fslice.msgsSize >= ms.limits.MaxMsgBytes/(numFiles-1)) {

		nextSlice := ms.currSliceIdx + 1

		if nextSlice == numFiles {
			// Delete first file and shift remaining. We will
			// keep currSliceIdx to the current value.
			Noticef("WARNING: Limit reached, discarding messages for subject %s", ms.subject)
			if err := ms.removeAndShiftFiles(); err != nil {
				return nil, err
			}
		} else {
			// Close the file and open the next slice
			if err := ms.file.Close(); err != nil {
				return nil, err
			}
			file, err := openFile(ms.files[nextSlice].fileName)
			if err != nil {
				return nil, err
			}
			ms.file = file
			ms.currSliceIdx = nextSlice
		}
	}
	return m, nil
}

func (ms *FileMsgStore) removeAndShiftFiles() error {
	// Close the currently opened file (should be the numFile's file)
	if err := ms.file.Close(); err != nil {
		return err
	}

	// Rename msgs.2.dat to msgs.1.dat, refresh state, etc...
	for i := 0; i < numFiles-1; i++ {
		file1 := ms.files[i]
		file2 := ms.files[i+1]

		if err := os.Rename(file2.fileName, file1.fileName); err != nil {
			return err
		}
		// Update total stats for the first store being removed
		if i == 0 {
			ms.totalCount -= file1.msgsCount
			ms.totalBytes -= file1.msgsSize

			// Remove all messages from first file from our cache
			seqStart := file1.firstMsg.Sequence
			seqEnd := file1.lastMsg.Sequence
			for i := seqStart; i <= seqEnd; i++ {
				delete(ms.msgs, i)
			}
			// Update sequence of first available message
			ms.first = file2.firstMsg.Sequence
		}

		// Copy over values from the next slice
		file1.firstMsg = file2.firstMsg
		file1.lastMsg = file2.lastMsg
		file1.msgsCount = file2.msgsCount
		file1.msgsSize = file2.msgsSize
	}

	var err error

	// Reset the last
	fslice := ms.files[numFiles-1]
	ms.file, err = openFile(fslice.fileName)
	if err != nil {
		return err
	}
	// Move to the end of the file (offset 0 relative to end)
	if _, err := ms.file.Seek(0, 2); err != nil {
		return err
	}
	fslice.firstMsg = nil
	fslice.lastMsg = nil
	fslice.msgsCount = 0
	fslice.msgsSize = uint64(0)
	return nil
}

// Close closes the store.
func (ms *FileMsgStore) Close() error {
	ms.Lock()
	defer ms.Unlock()

	if ms.closed {
		return nil
	}

	ms.closed = true

	var err error
	if ms.file != nil {
		err = ms.file.Close()
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileSubStore methods
////////////////////////////////////////////////////////////////////////////

// newFileSubStore returns a new instace of a file SubStore.
func newFileSubStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileSubStore, map[uint64]*recoveredSub, error) {
	ss := &FileSubStore{}
	ss.init(channel, limits)

	var err error
	var subs map[uint64]*recoveredSub

	fileName := filepath.Join(channelDirName, subsFileName)
	ss.file, err = openFile(fileName)
	if err == nil && doRecover {
		subs, err = ss.recoverSubscriptions()
	}
	if err != nil {
		ss.Close()
		ss = nil
		subs = nil
		return nil, nil, fmt.Errorf("unable to create subscription store for [%s]: %v", channel, err)
	}
	return ss, subs, nil
}

// recoverSubscriptions recovers subscriptions state for this store.
func (ss *FileSubStore) recoverSubscriptions() (map[uint64]*recoveredSub, error) {
	// We will store the recovered subscriptions in there.
	recoveredSubs := make(map[uint64]*recoveredSub)

	var err error
	var recType subRecordType
	var newSub *spb.SubState
	var delSub *spb.SubStateDelete
	var updateSub *spb.SubStateUpdate

	recHeader := 0
	recSize := 0
	file := ss.file

	for {
		// Read the message size as an int32
		recHeader, err = util.ReadInt(file)
		if err != nil {
			if err == io.EOF {
				// We are done, reset err
				err = nil
				break
			} else {
				return nil, err
			}
		}
		// Get type and size from the header:
		// type is first byte
		recType = subRecordType(recHeader >> 24 & 0xFF)
		// size is following 3 bytes.
		recSize = recHeader & 0xFFFFFF

		// Expand buffer if necessary
		ss.tmpSubBuf = util.EnsureBufBigEnough(ss.tmpSubBuf, recSize)

		// Read fully the expected number of bytes for this record
		if _, err := io.ReadFull(file, ss.tmpSubBuf[:recSize]); err != nil {
			return nil, err
		}

		// Based on record type...
		switch recType {
		case subRecNew:
			newSub = &spb.SubState{}
			if err := newSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return nil, err
			}
			subAndPending := &recoveredSub{
				sub:    newSub,
				seqnos: make(map[uint64]struct{}),
			}
			recoveredSubs[newSub.ID] = subAndPending
			// Keep track of the subscriptions count
			ss.subsCount++
			// Keep track of max subscription ID found.
			if newSub.ID > ss.maxSubID {
				ss.maxSubID = newSub.ID
			}
			break
		case subRecDel:
			delSub = &spb.SubStateDelete{}
			if err := delSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return nil, err
			}
			if _, exists := recoveredSubs[delSub.ID]; exists {
				delete(recoveredSubs, delSub.ID)
				// Keep track of the subscriptions count
				ss.subsCount--
			}
			// Keep track of max subscription ID found.
			if delSub.ID > ss.maxSubID {
				ss.maxSubID = delSub.ID
			}
			break
		case subRecMsg:
			updateSub = &spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return nil, err
			}
			if subAndPending, exists := recoveredSubs[updateSub.ID]; exists {
				subAndPending.seqnos[updateSub.Seqno] = struct{}{}
			}
			break
		case subRecAck:
			updateSub = &spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return nil, err
			}
			if subAndPending, exists := recoveredSubs[updateSub.ID]; exists {
				delete(subAndPending.seqnos, updateSub.Seqno)
			}
			break
		default:
			return nil, fmt.Errorf("unexpected record type: %v", recType)
		}
	}
	return recoveredSubs, nil
}

// CreateSub records a new subscription represented by SubState. On success,
// it returns an id that is used by the other methods.
func (ss *FileSubStore) CreateSub(sub *spb.SubState) error {
	// Check if we can create the subscription (check limits and update
	// subscription count)
	ss.Lock()
	defer ss.Unlock()
	if err := ss.createSub(sub); err != nil {
		return err
	}
	if err := ss.writeRecord(subRecNew, sub); err != nil {
		return err
	}
	return nil
}

// DeleteSub invalidates this subscription.
func (ss *FileSubStore) DeleteSub(subid uint64) {
	ss.Lock()
	del := &spb.SubStateDelete{
		ID: subid,
	}
	ss.writeRecord(subRecDel, del)
	ss.Unlock()
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	update := &spb.SubStateUpdate{
		ID:    subid,
		Seqno: seqno,
	}
	err := ss.writeRecord(subRecMsg, update)
	ss.Unlock()
	return err
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (ss *FileSubStore) AckSeqPending(subid, seqno uint64) error {
	ss.Lock()
	update := &spb.SubStateUpdate{
		ID:    subid,
		Seqno: seqno,
	}
	err := ss.writeRecord(subRecAck, update)
	ss.Unlock()
	return err
}

// writes a record in the subscriptions file.
// store's lock is held on entry.
func (ss *FileSubStore) writeRecord(recType subRecordType, rec subRecord) error {

	bufSize := rec.Size()

	// Add record header size
	totalSize := bufSize + subRecordHeaderSize

	// Alloc or realloc as needed
	ss.tmpSubBuf = util.EnsureBufBigEnough(ss.tmpSubBuf, totalSize)

	// Prepare header
	writeHeader(ss.tmpSubBuf[0:subRecordHeaderSize], recType, bufSize)

	// Marshal into the given buffer (offset with header size)
	_, err := rec.MarshalTo(ss.tmpSubBuf[subRecordHeaderSize:totalSize])
	if err == nil {
		// Write the header and record
		_, err = ss.file.Write(ss.tmpSubBuf[:totalSize])
	}
	return err
}

// Close closes this store
func (ss *FileSubStore) Close() error {
	ss.RLock()
	defer ss.RUnlock()

	if ss.closed {
		return nil
	}

	ss.closed = true

	var err error
	if ss.file != nil {
		err = ss.file.Close()
	}
	return err
}
