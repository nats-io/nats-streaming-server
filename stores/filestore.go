// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan-server/util"
	"github.com/nats-io/stan/pb"
)

const (
	// Our file version.
	fileVersion = uint8(1)

	// Number of files for a MsgStore on a given channel.
	numFiles = 5

	// Name of the subscriptions file.
	subsFileName = "subs.dat"

	// record header size of a subscription:
	// 4 bytes: 1 byte for type, 3 bytes for buffer size
	subRecordHeaderSize = 4
)

// Type for the records in the subscriptions file
type subRecordType byte

// Various record types
const (
	subRecNew = subRecordType(iota)
	subRecDel
	subRecAck
	subRecMsg
)

// FileStore is a factory for message and subscription stores.
type FileStore struct {
	genericStore
	rootDir string
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	genericSubStore
	tmpSubBuf     []byte
	file          *os.File
	recoveredSubs map[uint64]*RecoveredSubState
}

// fileSlice represents one of the message store file (there are a number
// of files for a MsgStore on a given channel).
type fileSlice struct {
	fileName  string
	file      *os.File
	firstMsg  *pb.MsgProto
	lastMsg   *pb.MsgProto
	msgsCount int
	msgsSize  uint64
}

// FileMsgStore is a per channel message file store.
type FileMsgStore struct {
	genericMsgStore
	tmpMsgBuf    []byte
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
	if err == nil {
		if checkVersion {
			err = checkFileVersion(file)
		} else {
			// This is a new file, write our file version
			var b [1]byte

			b[0] = fileVersion
			_, err = file.Write(b[:1])
		}
		if err != nil {
			file.Close()
			file = nil
		}
	}
	return file, err
}

// check that the version of the file is understood by this interface
func checkFileVersion(r io.Reader) error {
	var b [1]byte

	_, err := r.Read(b[:1])
	if err != nil {
		return fmt.Errorf("Unable to verify file version: %v", err)
	}
	fv := b[0]
	if fv > fileVersion {
		return fmt.Errorf("Unsupported file version: %v (support up to %v)", fv, fileVersion)
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

// NewFileStore returns a factory for stores backed by files.
// If not limits are provided, the store will be created with
// DefaultChannelLimits.
func NewFileStore(rootDir string, limits *ChannelLimits) (*FileStore, error) {
	fs := &FileStore{
		rootDir: rootDir,
	}
	fs.init("FILESTORE", limits)

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		Errorf("Unable to create the root directory [%s]: %v", rootDir, err)
		return nil, err
	}

	var err error
	var msgStore *FileMsgStore
	var subStore *FileSubStore

	// Recover channels
	var channels []os.FileInfo
	channels, err = ioutil.ReadDir(rootDir)
	if err == nil {
		for _, c := range channels {
			// Channels are directories. Ignore simple files
			if !c.IsDir() {
				continue
			}

			channel := c.Name()
			channelDirName := filepath.Join(rootDir, channel)

			// Recover messages for this channel
			msgStore, err = newFileMsgStore(channelDirName, channel, fs.limits, true)
			if err == nil {
				subStore, err = newFileSubStore(channelDirName, channel, fs.limits, true)
			}
			if err != nil {
				break
			}

			fs.channels[channel] = &ChannelStore{
				Subs: subStore,
				Msgs: msgStore,
			}
		}
	}
	if err != nil {
		// Make sure we close to make sure that all files that have been
		// successfully opened are properly closed.
		fs.Close()
	}

	return fs, err
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
	var err error
	var msgStore MsgStore
	var subStore SubStore

	channelDirName := filepath.Join(fs.rootDir, channel)
	err = os.MkdirAll(channelDirName, os.ModeDir+os.ModePerm)
	if err == nil {
		msgStore, err = newFileMsgStore(channelDirName, channel, fs.limits, false)
	}
	if err == nil {
		subStore, err = newFileSubStore(channelDirName, channel, fs.limits, false)
	}
	if err != nil {
		return nil, false, err
	}

	channelStore = &ChannelStore{
		Subs: subStore,
		Msgs: msgStore,
	}

	fs.channels[channel] = channelStore

	return channelStore, true, nil
}

////////////////////////////////////////////////////////////////////////////
// FileMsgStore methods
////////////////////////////////////////////////////////////////////////////

// newFileMsgStore returns a new instace of a file MsgStore.
func newFileMsgStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileMsgStore, error) {
	var err error

	// Create an instance and initialize
	ms := &FileMsgStore{}
	ms.init(channel, limits)

	// Open the files
	for i := 0; (err == nil) && (i < numFiles); i++ {
		// Fully qualified file name.
		fileName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.dat", (i+1)))

		// Create a file slice.
		slice := &fileSlice{fileName: fileName}

		// Open the file.
		slice.file, err = openFile(fileName)
		if err == nil {
			// Save slice
			ms.files[i] = slice

			// Should we try to recover (startup case)
			if doRecover {
				err = ms.recoverOneMsgFile(i)
			}
		}
	}
	if err != nil {
		Errorf("Unable to restore message store for [%s]: %v", ms.subject, err)
	}
	return ms, err
}

// recovers one of the file
func (ms *FileMsgStore) recoverOneMsgFile(numFile int) error {
	var err error

	msgSize := 0
	var msg *pb.MsgProto

	fslice := ms.files[numFile]
	file := fslice.file

	hasOne := false

	for err == nil {
		// Read the message size as an int32
		msgSize, err = util.ReadInt(file)
		if err == io.EOF {
			// We are done, reset err
			err = nil
			break
		}

		if err == nil {
			// Expand buffer if necessary
			ms.tmpMsgBuf = util.EnsureBufBigEnough(ms.tmpMsgBuf, msgSize)

			// Read fully the expected number of bytes for this message
			_, err = io.ReadFull(file, ms.tmpMsgBuf[:msgSize])
		}
		if err == nil {
			// Recover this message
			msg = &pb.MsgProto{}
			err = msg.Unmarshal(ms.tmpMsgBuf[:msgSize])
		}
		if err == nil {
			// Some accounting...
			hasOne = true

			if fslice.firstMsg == nil {
				fslice.firstMsg = msg

				if ms.currSliceIdx == -1 {
					ms.first = msg.Sequence
				}
			}
			fslice.lastMsg = msg
			fslice.msgsCount++
			fslice.msgsSize += uint64(len(msg.Data))

			ms.msgs[msg.Sequence] = msg
		}
	}

	// Do more accounting and bump the current slice index if we recovered
	// at least one message on that file.
	if err == nil && hasOne {
		ms.last = fslice.lastMsg.Sequence
		ms.totalCount += fslice.msgsCount
		ms.totalBytes += fslice.msgsSize
		ms.currSliceIdx = numFile
	}

	return err
}

// Store a given message.
func (ms *FileMsgStore) Store(reply string, data []byte) (*pb.MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	var err error

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
	_, err = m.MarshalTo(ms.tmpMsgBuf[4:totalSize])
	if err == nil {
		// Write the buffer
		_, err = fslice.file.Write(ms.tmpMsgBuf[:totalSize])
	}
	if err != nil {
		return nil, err
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

		if ms.currSliceIdx+1 == numFiles {
			// Delete first file and shift remaning. We will
			// keep currSliceIdx to the current value.
			Noticef("WARNING: Limit reached, discarding messages for subject %s", ms.subject)
			err = ms.removeAndShiftFiles()
		} else {
			ms.currSliceIdx++
		}
	}

	return m, err
}

func (ms *FileMsgStore) removeAndShiftFiles() error {
	var err error

	// Rename msgs.2.dat to msgs.1.dat, refresh state, etc...
	for i := 0; i < numFiles-1; i++ {
		file1 := ms.files[i]
		file2 := ms.files[i+1]

		if i == 0 {
			err = file1.file.Close()
		}
		if err == nil {
			err = file2.file.Close()
		}
		if err == nil {
			err = os.Rename(file2.fileName, file1.fileName)
		}
		if err == nil {
			file1.file, err = openFile(file1.fileName)
		}
		if err == nil {
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
	}
	if err == nil {
		// Reset the last
		fslice := ms.files[numFiles-1]
		fslice.file, err = openFile(fslice.fileName)
		if err == nil {
			// Move to the end of the file (offset 0 relative to end)
			_, err = fslice.file.Seek(0, 2)
		}
		if err == nil {
			fslice.firstMsg = nil
			fslice.lastMsg = nil
			fslice.msgsCount = 0
			fslice.msgsSize = uint64(0)
		}
	}

	return err
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
	for i := 0; i < numFiles; i++ {
		file := ms.files[i].file
		if file != nil {
			lerr := file.Close()
			if lerr != nil && err == nil {
				err = lerr
			}
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileSubStore methods
////////////////////////////////////////////////////////////////////////////

// newFileSubStore returns a new instace of a file SubStore.
func newFileSubStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileSubStore, error) {
	ss := &FileSubStore{}
	ss.init(channel, limits)

	var err error

	fileName := filepath.Join(channelDirName, subsFileName)
	ss.file, err = openFile(fileName)
	if err == nil && doRecover {
		err = ss.recoverSubscriptions()
	}
	if err != nil {
		Errorf("Unable to create subscription store for [%s]: %v", channel, err)
	}
	return ss, err
}

// GetRecoveredState returns the restored subscriptions.
// Stores not supporting recovery must return an empty map.
func (ss *FileSubStore) GetRecoveredState() map[uint64]*RecoveredSubState {
	ss.RLock()
	defer ss.RUnlock()

	// We still need to return an empty map.
	if ss.recoveredSubs == nil {
		return make(map[uint64]*RecoveredSubState, 0)
	}

	return ss.recoveredSubs
}

// ClearRecoverdState clears the internal state regarding recoverd subscriptions.
func (ss *FileSubStore) ClearRecoverdState() {
	ss.Lock()
	ss.recoveredSubs = nil
	ss.Unlock()
}

// recoverSubscriptions recovers subscriptions state for this store.
func (ss *FileSubStore) recoverSubscriptions() error {
	// We will store the recovered subscriptions in there.
	ss.recoveredSubs = make(map[uint64]*RecoveredSubState, 16)

	var err error
	var recType subRecordType
	var newSub *spb.SubState
	var delSub *spb.SubStateDelete
	var updateSub *spb.SubStateUpdate

	recHeader := 0
	recSize := 0
	file := ss.file

	for err == nil {
		// Read the message size as an int32
		recHeader, err = util.ReadInt(file)
		if err == io.EOF {
			// We are done, reset err
			err = nil
			break
		}

		if err == nil {
			// Get type and size from the header:
			// type is first byte
			recType = subRecordType(recHeader >> 24 & 0xFF)
			// size is following 3 bytes.
			recSize = recHeader & 0xFFFFFF

			// Expand buffer if necessary
			ss.tmpSubBuf = util.EnsureBufBigEnough(ss.tmpSubBuf, recSize)

			// Read fully the expected number of bytes for this record
			_, err = io.ReadFull(file, ss.tmpSubBuf[:recSize])
		}
		if err == nil {
			// Based on record type...
			switch recType {
			case subRecNew:
				newSub = &spb.SubState{}
				err = newSub.Unmarshal(ss.tmpSubBuf[:recSize])
				if err == nil {
					subAndPending := &RecoveredSubState{
						Sub:    newSub,
						Seqnos: make(map[uint64]struct{}, 16),
					}
					ss.recoveredSubs[newSub.ID] = subAndPending
					// Keep track of the subscriptions count
					ss.subsCount++
				}
				break
			case subRecDel:
				delSub = &spb.SubStateDelete{}
				err = delSub.Unmarshal(ss.tmpSubBuf[:recSize])
				if err == nil {
					if _, exists := ss.recoveredSubs[delSub.ID]; exists {
						delete(ss.recoveredSubs, delSub.ID)
						// Keep track of the subscriptions count
						ss.subsCount--
					}
				}
				break
			case subRecMsg:
				updateSub = &spb.SubStateUpdate{}
				err = updateSub.Unmarshal(ss.tmpSubBuf[:recSize])
				if err == nil {
					if subAndPending, exists := ss.recoveredSubs[updateSub.ID]; exists {
						subAndPending.Seqnos[updateSub.Seqno] = struct{}{}
					}
				}
				break
			case subRecAck:
				updateSub = &spb.SubStateUpdate{}
				err = updateSub.Unmarshal(ss.tmpSubBuf[:recSize])
				if err == nil {
					if subAndPending, exists := ss.recoveredSubs[updateSub.ID]; exists {
						delete(subAndPending.Seqnos, updateSub.Seqno)
					}
				}
				break
			default:
				err = fmt.Errorf("Unexpected record type: %v", recType)
			}
		}
	}
	return err
}

// CreateSub records a new subscription represented by SubState. On success,
// it returns an id that is used by the other methods.
func (ss *FileSubStore) CreateSub(sub *spb.SubState) error {
	// Check if we can create the subscription (check limits and update
	// subscription count)
	ss.Lock()
	err := ss.createSub(sub)
	if err == nil {
		err = ss.writeRecord(subRecNew, sub, nil, nil)
	}
	ss.Unlock()
	return err
}

// DeleteSub invalidates this subscription.
func (ss *FileSubStore) DeleteSub(subid uint64) {
	ss.Lock()
	del := &spb.SubStateDelete{
		ID: subid,
	}
	ss.writeRecord(subRecDel, nil, del, nil)
	ss.Unlock()
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	update := &spb.SubStateUpdate{
		ID:    subid,
		Seqno: seqno,
	}
	err := ss.writeRecord(subRecMsg, nil, nil, update)
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
	err := ss.writeRecord(subRecAck, nil, nil, update)
	ss.Unlock()
	return err
}

// writes a record in the subscriptions file.
// store's lock is held on entry.
func (ss *FileSubStore) writeRecord(recType subRecordType, newSub *spb.SubState,
	delSub *spb.SubStateDelete, updateSub *spb.SubStateUpdate) error {

	var err error
	var bufSize int

	// Size needed for the record
	switch recType {
	case subRecNew:
		bufSize = newSub.Size()
		break
	case subRecDel:
		bufSize = delSub.Size()
		break
	default:
		bufSize = updateSub.Size()
		break
	}

	// Add record header size
	totalSize := bufSize + subRecordHeaderSize

	// Alloc or realloc as needed
	ss.tmpSubBuf = util.EnsureBufBigEnough(ss.tmpSubBuf, totalSize)

	// Prepare header
	writeHeader(ss.tmpSubBuf[0:subRecordHeaderSize], recType, bufSize)

	// Marshal into the given buffer (offset with header size)
	switch recType {
	case subRecNew:
		_, err = newSub.MarshalTo(ss.tmpSubBuf[subRecordHeaderSize:totalSize])
		break
	case subRecDel:
		_, err = delSub.MarshalTo(ss.tmpSubBuf[subRecordHeaderSize:totalSize])
		break
	default:
		_, err = updateSub.MarshalTo(ss.tmpSubBuf[subRecordHeaderSize:totalSize])
		break
	}
	if err == nil {
		// Write the header and record
		_, err = ss.file.Write(ss.tmpSubBuf[:totalSize])
	}
	return nil
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
