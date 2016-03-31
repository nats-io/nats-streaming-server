package stores

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan/pb"
)

const (
	numFiles = 5

	addPending = uint8(1)
	addAck     = uint8(2)
)

// FileStore is a factory for message and subscription stores.
type FileStore struct {
	genericStore
	rootDir string
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	genericSubStore
	tmpSubBuf   []byte
	subsFile    *os.File
	updatesFile *os.File
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

// make this a function in case OpenFile parameters are not right, easier
// to change in a single place.
func openFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
}

////////////////////////////////////////////////////////////////////////////
// FileStore methods
////////////////////////////////////////////////////////////////////////////

// NewFileStore returns a factory for stores backed by files.
func NewFileStore(rootDir string, limits ChannelLimits) (*FileStore, error) {
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
			msgStore, err = NewFileMsgStore(channelDirName, channel, fs.limits, true)
			if err == nil {
				subStore, err = NewFileSubStore(channelDirName, channel, fs.limits, true)
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
		Errorf("Unable to restore state: %v", err)
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
		msgStore, err = NewFileMsgStore(channelDirName, channel, fs.limits, false)
	}
	if err == nil {
		subStore, err = NewFileSubStore(channelDirName, channel, fs.limits, false)
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

// NewFileMsgStore returns a new instace of a file MsgStore.
func NewFileMsgStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileMsgStore, error) {
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

	msgSize := int32(0)
	bufSize := int32(0)
	var msgBuf []byte
	var msg *pb.MsgProto

	fslice := ms.files[numFile]
	file := fslice.file

	hasOne := false

	for err == nil {
		// Read the message size as an int32
		err = binary.Read(file, binary.LittleEndian, &msgSize)
		if err == io.EOF {
			// We are done, reset err
			err = nil
			break
		}

		if err == nil {
			// Expand buffer if necessary
			if msgSize > bufSize {
				bufSize = int32(float32(msgSize) * 1.10)
				msgBuf = make([]byte, bufSize)
			}

			// Read fully the expected number of bytes for this message
			_, err = io.ReadFull(file, msgBuf[:msgSize])
		}
		if err == nil {
			// Recover this message
			msg = &pb.MsgProto{}
			err = msg.Unmarshal(msgBuf[:msgSize])
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

	// This is the size needed to store the marshalled message
	bufSize := m.Size()

	// Alloc or realloc if needed
	if ms.tmpMsgBuf == nil || len(ms.tmpMsgBuf) < bufSize {
		ms.tmpMsgBuf = make([]byte, int(float32(bufSize)*1.1))
	}

	// Marshal into the given buffer
	_, err = m.MarshalTo(ms.tmpMsgBuf)
	if err == nil {
		// Write the size of the buffer ((int does not work, needs to be int32)
		bufSizeToWrite := int32(bufSize)
		err = binary.Write(fslice.file, binary.LittleEndian, bufSizeToWrite)
	}
	if err == nil {
		// Write the buffer
		_, err = fslice.file.Write(ms.tmpMsgBuf[:bufSize])
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
	if (fslice.msgsCount >= ms.limits.MaxNumMsgs/(numFiles-1)) ||
		(fslice.msgsSize >= ms.limits.MaxMsgBytes/(numFiles-1)) {

		if ms.currSliceIdx+1 == numFiles {
			// Delete first file and shift remaning. We will
			// keep currSliceIdx to the current value.
			Errorf("WARNING: Limit reached, discarding messages for subject %s", ms.subject)
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
			if err == nil && lerr != nil {
				err = lerr
			}
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileSubStore methods
////////////////////////////////////////////////////////////////////////////

// NewFileSubStore returns a new instace of a file SubStore.
func NewFileSubStore(channelDirName, channel string, limits ChannelLimits, doRecover bool) (*FileSubStore, error) {
	ss := &FileSubStore{}
	ss.init(channel, limits)

	var err error

	subsFileName := filepath.Join(channelDirName, "subs.dat")
	ss.subsFile, err = openFile(subsFileName)
	if err == nil {
		updatesFileName := filepath.Join(channelDirName, "subsup.dat")
		ss.updatesFile, err = openFile(updatesFileName)
	}
	if err == nil && doRecover {
		// TODO: recovery. Will probably need to have callbacks
		// so that the server can update its structures.
	}
	if err != nil {
		Errorf("Unable to create subscription store for [%s]: %v", channel, err)
	}
	return ss, err
}

// CreateSub records a new subscription represented by SubState. On success,
// it returns an id that is used by the other methods.
func (ss *FileSubStore) CreateSub(sub *spb.SubState) (uint64, error) {
	ss.Lock()
	defer ss.Unlock()

	// Size needed to store the marshalled sub state
	bufSize := sub.Size()

	// Check if we can create the subscription (check limits and update
	// subscription count)
	subID, err := ss.createSub(sub)
	if err == nil {
		// Write the subID
		err = binary.Write(ss.subsFile, binary.LittleEndian, subID)
	}
	if err == nil {
		// Alloc or realloc the sub's buffer if needed
		if ss.tmpSubBuf == nil || bufSize > len(ss.tmpSubBuf) {
			ss.tmpSubBuf = make([]byte, int(float32(bufSize)*1.1))
		}
		// Marshal into the given buffer
		_, err = sub.MarshalTo(ss.tmpSubBuf)
	}
	if err == nil {
		// Write the size of the buffer (int does not work, needs to be int32)
		bufSizeToWrite := int32(bufSize)
		err = binary.Write(ss.subsFile, binary.LittleEndian, bufSizeToWrite)
	}
	if err == nil {
		// Write the buffer
		_, err = ss.subsFile.Write(ss.tmpSubBuf[:bufSize])
	}
	return subID, err
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	err := binary.Write(ss.updatesFile, binary.LittleEndian, addPending)
	if err == nil {
		err = binary.Write(ss.updatesFile, binary.LittleEndian, subid)
	}
	if err == nil {
		err = binary.Write(ss.updatesFile, binary.LittleEndian, seqno)
	}
	return err
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (ss *FileSubStore) AckSeqPending(subid, seqno uint64) error {
	err := binary.Write(ss.updatesFile, binary.LittleEndian, addAck)
	if err == nil {
		err = binary.Write(ss.updatesFile, binary.LittleEndian, subid)
	}
	if err == nil {
		err = binary.Write(ss.updatesFile, binary.LittleEndian, seqno)
	}
	return err
}
