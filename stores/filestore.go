package stores

import (
	"encoding/binary"
	"errors"
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

	fileVersion = uint8(1)

	addPending = uint8(1)
	addAck     = uint8(2)
)

var byteOrder binary.ByteOrder

func init() {
	byteOrder = binary.LittleEndian
}

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

// openFile opens the file (and create it if needed). If the file exists,
// it checks that the version is supported.
func openFile(fileName string) (*os.File, error) {
	checkVersion := false

	// Check if file already exists
	if _, err := os.Stat(fileName); err == nil {
		checkVersion = true
	}
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err == nil {
		if checkVersion {
			err = checkFileVersion(file)
		} else {
			// This is a new file, write our file version
			err = writeUInt8(file, fileVersion)
		}
		if err != nil {
			file.Close()
		}
	}
	return file, err
}

// check that the version of the file is understood by this interface
func checkFileVersion(r io.Reader) error {
	fv, err := readUInt8(r)
	if err == nil {
		if fv > fileVersion {
			return errors.New(fmt.Sprintf("Unsupported file version: %v (support up to %v)", fv, fileVersion))
		}
	}
	return err
}

// write a size (as int converted into uint32) to the writer using byteOrder.
func writeSize(w io.Writer, size int) error {
	var b [4]byte
	var bs []byte

	bs = b[:4]

	byteOrder.PutUint32(bs, uint32(size))
	_, err := w.Write(bs)
	return err
}

// read a size (as uint32 converted to int) from the reader using byteOrder.
func readSize(r io.Reader) (int, error) {
	var b [4]byte
	var bs []byte

	bs = b[:4]

	_, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	}
	return int(byteOrder.Uint32(bs)), nil
}

// write a subscription update to the writer using byteOrder
func writeUpdate(w io.Writer, typeUpdate uint8, subid, seqno uint64) error {
	var b [1 + 8 + 8]byte
	var bs []byte

	b[0] = byte(typeUpdate)

	bs = b[1:9]
	byteOrder.PutUint64(bs, subid)

	bs = b[9:17]
	byteOrder.PutUint64(bs, seqno)

	_, err := w.Write(b[0:17])
	return err
}

// read a subscription update from the reader using byteOrder
func readUInt64(r io.Reader) (uint8, uint64, uint64, error) {
	var b [1 + 8 + 8]byte
	var bs []byte

	_, err := io.ReadFull(r, b[0:17])
	if err != nil {
		return 0, 0, 0, err
	}

	typeUpdate := b[0]

	bs = b[1:9]
	subid := byteOrder.Uint64(bs)

	bs = b[9:17]
	seqno := byteOrder.Uint64(bs)

	return typeUpdate, subid, seqno, nil
}

// write an uint8 to the writer using byteOrder.
func writeUInt8(w io.Writer, v uint8) error {
	var b [1]byte
	var bs []byte

	b[0] = byte(v)
	bs = b[:1]

	_, err := w.Write(bs)
	return err
}

// read an uint8 from the reader using byteOrder.
func readUInt8(r io.Reader) (uint8, error) {
	var b [1]byte
	var bs []byte

	bs = b[:1]

	_, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	}
	return bs[0], nil
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

	msgSize := 0
	bufSize := 0
	var msgBuf []byte
	var msg *pb.MsgProto

	fslice := ms.files[numFile]
	file := fslice.file

	hasOne := false

	for err == nil {
		// Read the message size as an int32
		msgSize, err = readSize(file)
		if err == io.EOF {
			// We are done, reset err
			err = nil
			break
		}

		if err == nil {
			// Expand buffer if necessary
			if msgSize > bufSize {
				bufSize = int(float32(msgSize) * 1.10)
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

	// This is the size needed to store the marshalled message.
	bufSize := m.Size()

	// Alloc or realloc if needed
	if ms.tmpMsgBuf == nil || len(ms.tmpMsgBuf) < bufSize {
		ms.tmpMsgBuf = make([]byte, int(float32(bufSize)*1.1))
	}

	// Marshal into the given buffer
	_, err = m.MarshalTo(ms.tmpMsgBuf)
	if err == nil {
		err = writeSize(fslice.file, bufSize)
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
func (ss *FileSubStore) CreateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()

	// Check if we can create the subscription (check limits and update
	// subscription count)
	err := ss.createSub(sub)
	if err == nil {
		// Size need to be checked only after sub is fully initialized,
		// which happens in createSub (where sub.ID is set)
		bufSize := sub.Size()

		// Alloc or realloc the sub's buffer if needed
		if ss.tmpSubBuf == nil || bufSize > len(ss.tmpSubBuf) {
			ss.tmpSubBuf = make([]byte, int(float32(bufSize)*1.1))
		}

		// Marshal into the given buffer
		_, err = sub.MarshalTo(ss.tmpSubBuf)
		if err == nil {
			// Write size of following buffer
			err = writeSize(ss.subsFile, bufSize)
		}
		if err == nil {
			// Write the buffer
			_, err = ss.subsFile.Write(ss.tmpSubBuf[:bufSize])
		}
	}
	return err
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	err := writeUpdate(ss.updatesFile, addPending, subid, seqno)
	ss.Unlock()
	return err
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (ss *FileSubStore) AckSeqPending(subid, seqno uint64) error {
	ss.Lock()
	err := writeUpdate(ss.updatesFile, addAck, subid, seqno)
	ss.Unlock()
	return err
}

func (ss *FileSubStore) Close() error {
	ss.RLock()
	defer ss.RUnlock()

	if ss.closed {
		return nil
	}

	ss.closed = true

	var err error

	if ss.subsFile != nil {
		err = ss.subsFile.Close()
	}
	if ss.updatesFile != nil {
		lerr := ss.updatesFile.Close()
		if lerr != nil && err == nil {
			err = lerr
		}
	}
	return err
}
