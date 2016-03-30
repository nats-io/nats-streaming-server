package stores

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/stan/pb"
)

const (
	numFiles = 5
)

// FileStore is a factory for message and subscription stores.
type FileStore struct {
	GenericStore
	rootDir string
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	GenericSubStore
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
	GenericMsgStore
	files        [numFiles]*fileSlice
	currSliceIdx int
}

// NewFileStore returns a factory for stores backed by files.
func NewFileStore(rootDir string, limits ChannelLimits) (*FileStore, error) {
	fs := &FileStore{
		rootDir: rootDir,
	}
	fs.Init("FILESTORE", limits)

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		Errorf("Unable to create the root directory [%s]: %v", rootDir, err)
		return nil, err
	}

	var err error
	var msgStore *FileMsgStore

	// Recover channels
	var channels []os.FileInfo
	channels, err = ioutil.ReadDir(rootDir)
	if err == nil {
		for _, c := range channels {
			// Channels are directories. Ignore simple files
			if !c.IsDir() {
				continue
			}

			// Recover messages for this channel
			msgStore, err = fs.recoverMsgs(c)
			if err != nil {
				break
			}

			channelName := c.Name()

			fs.channels[channelName] = &ChannelStore{
				Subs: &FileSubStore{},
				Msgs: msgStore,
			}
		}
	}

	return fs, err
}

func (fs *FileStore) recoverMsgs(channelDir os.FileInfo) (*FileMsgStore, error) {

	// Create a message store
	msgStore := NewFileMsgStore(channelDir.Name(), fs.limits)

	// Go through each file and load messages
	for i := 0; i < numFiles; i++ {
		if err := fs.recoverOneMsgFile(msgStore, i); err != nil {
			Errorf("Unable to restore message store for [%s]: %v", msgStore.subject, err)
			return nil, err
		}
	}

	return msgStore, nil
}

func (fs *FileStore) recoverOneMsgFile(msgStore *FileMsgStore, numFile int) error {
	var err error
	var file *os.File

	fileName := filepath.Join(fs.rootDir, msgStore.subject, fmt.Sprintf("msgs.%d.dat", (numFile+1)))
	file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	fslice := &fileSlice{
		fileName: fileName,
		file:     file,
	}
	msgStore.files[numFile] = fslice

	msgSize := int32(0)
	bufSize := int32(0)
	var msgBuf []byte
	var msg *pb.MsgProto

	hasOne := false

	for {
		// Read the message size as an int32
		err = binary.Read(file, binary.LittleEndian, &msgSize)
		if err == io.EOF {
			// We are done
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
			msg, err = fs.recoverOneMsg(msgBuf[:msgSize])
		}
		if err == nil {
			// Some accounting...
			hasOne = true

			if fslice.firstMsg == nil {
				fslice.firstMsg = msg

				if msgStore.currSliceIdx == -1 {
					msgStore.first = msg.Sequence
				}
			}
			fslice.lastMsg = msg
			fslice.msgsCount++
			fslice.msgsSize += uint64(len(msg.Data))

			msgStore.msgs[msg.Sequence] = msg
		}

		if err != nil {
			break
		}
	}

	// Do more accounting and bump the current slice index if we recovered
	// at least one message on that file.
	if err == nil && hasOne {
		msgStore.last = fslice.lastMsg.Sequence
		msgStore.totalCount += fslice.msgsCount
		msgStore.totalBytes += fslice.msgsSize
		msgStore.currSliceIdx = numFile
	}

	return err
}

func (fs *FileStore) recoverOneMsg(buf []byte) (*pb.MsgProto, error) {
	m := &pb.MsgProto{}
	err := m.Unmarshal(buf)

	return m, err
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

	// We create the channel here...

	msgStore, err := fs.createFileMsgStore(channel)
	if err != nil {
		return nil, false, err
	}

	subStore := &FileSubStore{}

	channelStore = &ChannelStore{
		Subs: subStore,
		Msgs: msgStore,
	}

	fs.channels[channel] = channelStore

	return channelStore, true, nil
}

func (fs *FileStore) createFileMsgStore(channel string) (*FileMsgStore, error) {
	var fileSlices [numFiles]*fileSlice
	var err error

	channelName := filepath.Join(fs.rootDir, channel)
	err = os.Mkdir(channelName, os.ModeDir+os.ModePerm)
	if err == nil {
		var file *os.File

		for i := 0; i < numFiles; i++ {
			fileName := filepath.Join(channelName, fmt.Sprintf("msgs.%d.dat", (i+1)))
			file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err == nil {
				fileSlices[i] = &fileSlice{
					fileName: fileName,
					file:     file,
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	msgStore := NewFileMsgStore(channel, fs.limits)
	msgStore.files = fileSlices

	return msgStore, nil
}

// NewFileMsgStore returns a new instace of a file MsgStore
func NewFileMsgStore(channel string, limits ChannelLimits) *FileMsgStore {
	fs := &FileMsgStore{}
	fs.Init(channel, limits)

	return fs
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

	storedMsgBytes, err := m.Marshal()
	if err == nil {
		encodedMsgSize := int32(len(storedMsgBytes))
		err = binary.Write(fslice.file, binary.LittleEndian, encodedMsgSize)
	}
	if err == nil {
		_, err = fslice.file.Write(storedMsgBytes)
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
			file1.file, err = os.OpenFile(file1.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
		fslice.file, err = os.OpenFile(fslice.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
