// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"bufio"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
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

	// Name of the server file.
	serverFileName = "server.dat"

	// Number of bytes required to store a CRC-32 checksum
	crcSize = crc32.Size

	// Size of a record header.
	//  4 bytes: For typed records: 1 byte for type, 3 bytes for buffer size
	// 	         For non typed rec: buffer size
	// +4 bytes for CRC-32
	recordHeaderSize = 4 + crcSize

	// defaultBufSize is used for various buffered IO operations
	defaultBufSize = 10 * 1024 * 1024
)

// FileStoreOption is a function on the options for a File Store
type FileStoreOption func(*FileStoreOptions) error

// FileStoreOptions can be used to customize a File Store
type FileStoreOptions struct {
	// BufferSize is the size of the buffer used during store operations.
	BufferSize int

	// CompactEnabled allows to enable/disable files compaction.
	CompactEnabled bool

	// CompactInterval indicates the minimum interval (in seconds) between compactions.
	CompactInterval int

	// CompactFragmentation indicates the minimum ratio of fragmentation
	// to trigger compaction. For instance, 50 means that compaction
	// would not happen until fragmentation is more than 50%.
	CompactFragmentation int

	// CompactMinFileSize indicates the minimum file size before compaction
	// can be performed, regardless of the current file fragmentation.
	CompactMinFileSize int64

	// DoCRC enables (or disables) CRC checksum verification on read operations.
	DoCRC bool

	// CRCPoly is a polynomial used to make the table used in CRC computation.
	CRCPolynomial int64

	// DoSync indicates if `File.Sync()`` is called during a flush.
	DoSync bool
}

// DefaultFileStoreOptions defines the default options for a File Store.
var DefaultFileStoreOptions = FileStoreOptions{
	BufferSize:           2 * 1024 * 1024, // 2MB
	CompactEnabled:       true,
	CompactInterval:      5 * 60, // 5 minutes
	CompactFragmentation: 50,
	CompactMinFileSize:   1024 * 1024,
	DoCRC:                true,
	CRCPolynomial:        int64(crc32.IEEE),
	DoSync:               true,
}

// BufferSize is a FileStore option that sets the size of the buffer used
// during store writes. This can help improve write performance.
func BufferSize(size int) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.BufferSize = size
		return nil
	}
}

// CompactEnabled is a FileStore option that enables or disables file compaction.
// The value false will disable compaction.
func CompactEnabled(enabled bool) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CompactEnabled = enabled
		return nil
	}
}

// CompactInterval is a FileStore option that defines the minimum compaction interval.
// Compaction is not timer based, but instead when things get "deleted". This value
// prevents compaction to happen too often.
func CompactInterval(seconds int) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CompactInterval = seconds
		return nil
	}
}

// CompactFragmentation is a FileStore option that defines the fragmentation ratio
// below which compaction would not occur. For instance, specifying 50 means that
// if other variables would allow for compaction, the compaction would occur only
// after 50% of the file has data that is no longer valid.
func CompactFragmentation(fragmentation int) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CompactFragmentation = fragmentation
		return nil
	}
}

// CompactMinFileSize is a FileStore option that defines the minimum file size below
// which compaction would not occur. Specify `-1` if you don't want any minimum.
func CompactMinFileSize(fileSize int64) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CompactMinFileSize = fileSize
		return nil
	}
}

// DoCRC is a FileStore option that defines if a CRC checksum verification should
// be performed when records are read from disk.
func DoCRC(enableCRC bool) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.DoCRC = enableCRC
		return nil
	}
}

// CRCPolynomial is a FileStore option that defines the polynomial to use to create
// the table used for CRC-32 Checksum.
// See https://golang.org/pkg/hash/crc32/#MakeTable
func CRCPolynomial(polynomial int64) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CRCPolynomial = polynomial
		return nil
	}
}

// DoSync is a FileStore option that defines if `File.Sync()` should be called
// during a `Flush()` call.
func DoSync(enableFileSync bool) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.DoSync = enableFileSync
		return nil
	}
}

// AllOptions is a convenient option to pass all options from a FileStoreOptions
// structure to the constructor.
func AllOptions(opts *FileStoreOptions) FileStoreOption {
	return func(o *FileStoreOptions) error {
		// Make a copy
		*o = *opts
		return nil
	}
}

// Type for the records in the subscriptions file
type recordType byte

// Protobufs do not share a common interface, yet, when saving a
// record on disk, we have to get the size and marshal the record in
// a buffer. These methods are available in all the protobuf.
// So we create this interface with those two methods to be used by the
// writeRecord method.
type record interface {
	Size() int
	MarshalTo([]byte) (int, error)
}

// This is use for cases when the record is not typed
const recNoType = recordType(0)

// Record types for subscription file
const (
	subRecNew = recordType(iota) + 1
	subRecUpdate
	subRecDel
	subRecAck
	subRecMsg
)

// Record types for client store
const (
	addClient = recordType(iota) + 1
	delClient
)

// FileStore is the storage interface for STAN servers, backed by files.
type FileStore struct {
	genericStore
	rootDir       string
	serverFile    *os.File
	clientsFile   *os.File
	opts          FileStoreOptions
	compactItvl   time.Duration
	addClientRec  spb.ClientInfo
	delClientRec  spb.ClientDelete
	cliFileSize   int64
	cliDeleteRecs int // Number of deleted client records
	cliCompactTS  time.Time
	crcTable      *crc32.Table
}

type subscription struct {
	sub    *spb.SubState
	seqnos map[uint64]struct{}
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	genericSubStore
	tmpSubBuf   []byte
	file        *os.File
	bw          *bufio.Writer
	delSub      spb.SubStateDelete
	updateSub   spb.SubStateUpdate
	subs        map[uint64]*subscription
	opts        *FileStoreOptions // points to options from FileStore
	compactItvl time.Duration
	fileSize    int64
	numRecs     int // Number of records (sub and msgs)
	delRecs     int // Number of delete (or ack) records
	rootDir     string
	compactTS   time.Time
	crcTable    *crc32.Table // reference to the one from FileStore
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
	bw           *bufio.Writer
	files        [numFiles]*fileSlice
	currSliceIdx int
	opts         *FileStoreOptions // points to FileStore options
	crcTable     *crc32.Table      // reference to the one from FileStore
}

// openFile opens the file specified by `filename`.
// If the file exists, it checks that the version is supported.
// If no file mode is provided, the file is created if not present,
// opened in Read/Write and Append mode.
func openFile(fileName string, modes ...int) (*os.File, error) {
	checkVersion := false

	mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	if len(modes) > 0 {
		// Use the provided modes instead
		mode = 0
		for _, m := range modes {
			mode |= m
		}
	}

	// Check if file already exists
	if s, err := os.Stat(fileName); s != nil && err == nil {
		checkVersion = true
	}
	file, err := os.OpenFile(fileName, mode, 0666)
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

// writeRecord writes a record to `w`.
// The record layout is as follows:
// 8 bytes: 4 bytes for type and/or size combined
//          4 bytes for CRC-32
// variable bytes: payload.
// If a buffer is provided, this function uses it and expands it if necessary.
// The function returns the buffer (possibly changed due to expansion) and the
// number of bytes written into that buffer.
func writeRecord(w io.Writer, buf []byte, recType recordType, rec record, crcTable *crc32.Table) ([]byte, int, error) {
	// Size of record itself
	recSize := rec.Size()
	// This is the header + payload size
	totalSize := recordHeaderSize + recSize
	// Alloc or realloc as needed
	buf = util.EnsureBufBigEnough(buf, totalSize)
	// If there is a record type, encode it
	headerFirstInt := 0
	if recType != recNoType {
		if recSize > 0xFFFFFF {
			panic("record size too big")
		}
		// Encode the type in the high byte of the header
		headerFirstInt = int(recType)<<24 | recSize
	} else {
		// The header is the size of the record
		headerFirstInt = recSize
	}
	// Write the first part of the header at the beginning of the buffer
	util.ByteOrder.PutUint32(buf[:4], uint32(headerFirstInt))
	// Marshal the record into the given buffer, after the header offset
	if _, err := rec.MarshalTo(buf[recordHeaderSize:totalSize]); err != nil {
		// Return the buffer because the caller may have provided one
		return buf, 0, err
	}
	// Compute CRC
	crc := crc32.Checksum(buf[recordHeaderSize:totalSize], crcTable)
	// Write it in the buffer
	util.ByteOrder.PutUint32(buf[4:recordHeaderSize], crc)
	// Are we dealing with a buffered writer?
	bw, isBuffered := w.(*bufio.Writer)
	// if so, make sure that if what we are about to "write" is more
	// than what's available, then first flush the buffer.
	// This is to reduce the risk of partial writes.
	if isBuffered && (bw.Buffered() > 0) && (bw.Available() < totalSize) {
		if err := bw.Flush(); err != nil {
			return buf, 0, err
		}
	}
	// Write the content of our slice into the writer `w`
	if _, err := w.Write(buf[:totalSize]); err != nil {
		// Return the tmpBuf because the caller may have provided one
		return buf, 0, err
	}
	return buf, totalSize, nil
}

// readRecord reads a record from `r`, possibly checking the CRC-32 checksum.
// When `buf`` is not nil, this function ensures the buffer is big enough to
// hold the payload (expanding if necessary). Therefore, this call always
// return `buf`, regardless if there is an error or not.
// The caller is indicating if the record is supposed to be typed or not.
func readRecord(r io.Reader, buf []byte, recTyped bool, crcTable *crc32.Table, checkCRC bool) ([]byte, int, recordType, error) {
	_header := [recordHeaderSize]byte{}
	header := _header[:]
	if _, err := io.ReadFull(r, header); err != nil {
		return buf, 0, recNoType, err
	}
	recType := recNoType
	recSize := 0
	firstInt := int(util.ByteOrder.Uint32(header[:4]))
	if recTyped {
		recType = recordType(firstInt >> 24 & 0xFF)
		recSize = firstInt & 0xFFFFFF
	} else {
		recSize = firstInt
	}
	crc := util.ByteOrder.Uint32(header[4:recordHeaderSize])
	// Now we are going to read the payload
	buf = util.EnsureBufBigEnough(buf, recSize)
	if _, err := io.ReadFull(r, buf[:recSize]); err != nil {
		return buf, 0, recNoType, err
	}
	if checkCRC {
		// check CRC against what was stored
		if c := crc32.Checksum(buf[:recSize], crcTable); c != crc {
			return buf, 0, recNoType, fmt.Errorf("corrupted data, expected crc to be 0x%08x, got 0x%08x", crc, c)
		}
	}
	return buf, recSize, recType, nil
}

////////////////////////////////////////////////////////////////////////////
// FileStore methods
////////////////////////////////////////////////////////////////////////////

// NewFileStore returns a factory for stores backed by files, and recovers
// any state present.
// If not limits are provided, the store will be created with
// DefaultChannelLimits.
func NewFileStore(rootDir string, limits *ChannelLimits, options ...FileStoreOption) (*FileStore, *RecoveredState, error) {
	fs := &FileStore{
		rootDir: rootDir,
		opts:    DefaultFileStoreOptions,
	}
	fs.init(TypeFile, limits)

	for _, opt := range options {
		if err := opt(&fs.opts); err != nil {
			return nil, nil, err
		}
	}
	// Convert the compact interval in time.Duration
	fs.compactItvl = time.Duration(fs.opts.CompactInterval) * time.Second
	// Create the table using polynomial in options
	if fs.opts.CRCPolynomial == int64(crc32.IEEE) {
		fs.crcTable = crc32.IEEETable
	} else {
		fs.crcTable = crc32.MakeTable(uint32(fs.opts.CRCPolynomial))
	}

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, nil, fmt.Errorf("unable to create the root directory [%s]: %v", rootDir, err)
	}

	var err error
	var recoveredState *RecoveredState
	var serverInfo *spb.ServerInfo
	var recoveredClients []*Client
	var recoveredSubs = make(RecoveredSubscriptions)
	var channels []os.FileInfo
	var msgStore *FileMsgStore
	var subStore *FileSubStore

	// Ensure store is closed in case of return with error
	defer func() {
		if err != nil {
			fs.Close()
		}
	}()

	// Open/Create the server file (note that this file must not be opened,
	// in APPEND mode to allow truncate to work).
	fileName := filepath.Join(fs.rootDir, serverFileName)
	fs.serverFile, err = openFile(fileName, os.O_RDWR, os.O_CREATE)
	if err != nil {
		return nil, nil, err
	}

	// Open/Create the client file.
	fileName = filepath.Join(fs.rootDir, clientsFileName)
	fs.clientsFile, err = openFile(fileName)
	if err != nil {
		return nil, nil, err
	}

	// Recover the server file.
	serverInfo, err = fs.recoverServerInfo()
	if err != nil {
		return nil, nil, err
	}
	// If the server file is empty, then we are done
	if serverInfo == nil {
		// We return the file store instance, but no recovered state.
		return fs, nil, nil
	}

	// Recover the clients file
	recoveredClients, err = fs.recoverClients()
	if err != nil {
		return nil, nil, err
	}

	// Get the channels (there are subdirectories of rootDir)
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
		msgStore, err = fs.newFileMsgStore(channelDirName, channel, true)
		if err != nil {
			break
		}
		subStore, err = fs.newFileSubStore(channelDirName, channel, true)
		if err != nil {
			msgStore.Close()
			break
		}

		// For this channel, construct an array of RecoveredSubState
		rssArray := make([]*RecoveredSubState, 0, len(subStore.subs))

		// Fill that array with what we got from newFileSubStore.
		for _, sub := range subStore.subs {
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
		recoveredSubs[channel] = rssArray

		fs.channels[channel] = &ChannelStore{
			Subs: subStore,
			Msgs: msgStore,
		}
	}
	if err != nil {
		return nil, nil, err
	}
	// Create the recovered state to return
	recoveredState = &RecoveredState{
		Info:    serverInfo,
		Clients: recoveredClients,
		Subs:    recoveredSubs,
	}
	return fs, recoveredState, nil
}

// Init is used to persist server's information after the first start
func (fs *FileStore) Init(info *spb.ServerInfo) error {
	fs.Lock()
	defer fs.Unlock()

	f := fs.serverFile
	// Truncate the file (4 is the size of the fileVersion record)
	if err := f.Truncate(4); err != nil {
		return err
	}
	// Move offset to 4 (truncate does not do that)
	if _, err := f.Seek(4, 0); err != nil {
		return err
	}
	// ServerInfo record is not typed. We also don't pass a reusable buffer.
	if _, _, err := writeRecord(f, nil, recNoType, info, fs.crcTable); err != nil {
		return err
	}
	return nil
}

// recoverClients reads the client files and returns an array of RecoveredClient
func (fs *FileStore) recoverClients() ([]*Client, error) {
	var err error
	var recType recordType
	var recSize int

	_buf := [256]byte{}
	buf := _buf[:]

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(fs.clientsFile, defaultBufSize)

	for {
		buf, recSize, recType, err = readRecord(br, buf, true, fs.crcTable, fs.opts.DoCRC)
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return nil, err
		}
		fs.cliFileSize += int64(recSize + recordHeaderSize)
		switch recType {
		case addClient:
			c := &Client{}
			if err := c.ClientInfo.Unmarshal(buf[:recSize]); err != nil {
				return nil, err
			}
			// Add to the map. Note that if one already exists, which should
			// not, just replace with this most recent one.
			fs.clients[c.ID] = c
		case delClient:
			c := spb.ClientDelete{}
			if err := c.Unmarshal(buf[:recSize]); err != nil {
				return nil, err
			}
			delete(fs.clients, c.ID)
			fs.cliDeleteRecs++
		default:
			return nil, fmt.Errorf("invalid client record type: %v", recType)
		}
	}
	clients := make([]*Client, len(fs.clients))
	i := 0
	// Convert the map into an array
	for _, c := range fs.clients {
		clients[i] = c
		i++
	}
	return clients, nil
}

// recoverServerInfo reads the server file and returns a ServerInfo structure
func (fs *FileStore) recoverServerInfo() (*spb.ServerInfo, error) {
	file := fs.serverFile
	info := &spb.ServerInfo{}
	buf, size, _, err := readRecord(file, nil, false, fs.crcTable, fs.opts.DoCRC)
	if err != nil {
		if err == io.EOF {
			// We are done, no state recovered
			return nil, nil
		}
		return nil, err
	}
	// Check that the size of the file is consistent with the size
	// of the record we are supposed to recover. Account for the
	// 12 bytes (4 + recordHeaderSize) corresponding to the fileVersion and
	// record header.
	fstat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	expectedSize := int64(size + 4 + recordHeaderSize)
	if fstat.Size() != expectedSize {
		return nil, fmt.Errorf("incorrect file size, expected %v bytes, got %v bytes",
			expectedSize, fstat.Size())
	}
	// Reconstruct now
	if err := info.Unmarshal(buf[:size]); err != nil {
		return nil, err
	}
	return info, nil
}

// CreateChannel creates a ChannelStore for the given channel, and returns
// `true` to indicate that the channel is new, false if it already exists.
func (fs *FileStore) CreateChannel(channel string, userData interface{}) (*ChannelStore, bool, error) {
	fs.Lock()
	defer fs.Unlock()
	channelStore := fs.channels[channel]
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

	msgStore, err = fs.newFileMsgStore(channelDirName, channel, false)
	if err != nil {
		return nil, false, err
	}
	subStore, err = fs.newFileSubStore(channelDirName, channel, false)
	if err != nil {
		msgStore.Close()
		return nil, false, err
	}

	channelStore = &ChannelStore{
		Subs:     subStore,
		Msgs:     msgStore,
		UserData: userData,
	}

	fs.channels[channel] = channelStore

	return channelStore, true, nil
}

// AddClient stores information about the client identified by `clientID`.
func (fs *FileStore) AddClient(clientID, hbInbox string, userData interface{}) (*Client, bool, error) {
	sc, isNew, err := fs.genericStore.AddClient(clientID, hbInbox, userData)
	if err != nil {
		return nil, false, err
	}
	if !isNew {
		return sc, false, nil
	}
	fs.Lock()
	fs.addClientRec = spb.ClientInfo{ID: clientID, HbInbox: hbInbox}
	_, size, err := writeRecord(fs.clientsFile, nil, addClient, &fs.addClientRec, fs.crcTable)
	if err != nil {
		delete(fs.clients, clientID)
		fs.Unlock()
		return nil, false, err
	}
	fs.cliFileSize += int64(size)
	fs.Unlock()
	return sc, true, nil
}

// DeleteClient invalidates the client identified by `clientID`.
func (fs *FileStore) DeleteClient(clientID string) *Client {
	sc := fs.genericStore.DeleteClient(clientID)
	if sc != nil {
		fs.Lock()
		fs.delClientRec = spb.ClientDelete{ID: clientID}
		_, size, _ := writeRecord(fs.clientsFile, nil, delClient, &fs.delClientRec, fs.crcTable)
		fs.cliDeleteRecs++
		fs.cliFileSize += int64(size)
		// Check if this triggers a need for compaction
		if fs.shouldCompactClientFile() {
			fs.compactClientFile()
		}
		fs.Unlock()
	}
	return sc
}

// shouldCompactClientFile returns true if the client file should be compacted
// Lock is held by caller
func (fs *FileStore) shouldCompactClientFile() bool {
	// Global switch
	if !fs.opts.CompactEnabled {
		return false
	}
	// Check that if minimum file size is set, the client file
	// is at least at the minimum.
	if fs.opts.CompactMinFileSize > 0 && fs.cliFileSize < fs.opts.CompactMinFileSize {
		return false
	}
	// Check fragmentation
	frag := fs.cliDeleteRecs * 100 / (fs.cliDeleteRecs + len(fs.clients))
	if frag < fs.opts.CompactFragmentation {
		return false
	}
	// Check that we don't do too often
	if time.Now().Sub(fs.cliCompactTS) < fs.compactItvl {
		return false
	}
	return true
}

// Rewrite the content of the clients map into a temporary file,
// then swap back to active file.
// Store lock held on entry
func (fs *FileStore) compactClientFile() error {
	// Open a temporary file
	tmpFile, err := getTempFile(fs.rootDir, clientsFileName)
	if err != nil {
		return err
	}
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
		}
	}()
	bw := bufio.NewWriterSize(tmpFile, defaultBufSize)
	fileSize := int64(0)
	size := 0
	_buf := [256]byte{}
	buf := _buf[:]
	// Dump the content of active clients into the temporary file.
	for _, c := range fs.clients {
		fs.addClientRec = spb.ClientInfo{ID: c.ID, HbInbox: c.HbInbox}
		buf, size, err = writeRecord(bw, buf, addClient, &fs.addClientRec, fs.crcTable)
		if err != nil {
			return err
		}
		fileSize += int64(size)
	}
	// Flush the buffer on disk
	if err := bw.Flush(); err != nil {
		return err
	}
	// Switch the temporary file with the original one.
	fs.clientsFile, err = swapFiles(tmpFile, fs.clientsFile)
	if err != nil {
		return err
	}
	// Avoid unnecesary attempt to cleanup
	tmpFile = nil

	fs.cliDeleteRecs = 0
	fs.cliFileSize = fileSize
	fs.cliCompactTS = time.Now()
	return nil
}

// Return a temporary file (including file version)
func getTempFile(rootDir, prefix string) (*os.File, error) {
	tmpFile, err := ioutil.TempFile(rootDir, prefix)
	if err != nil {
		return nil, err
	}
	if err := util.WriteInt(tmpFile, fileVersion); err != nil {
		return nil, err
	}
	return tmpFile, nil
}

// When a store file is compacted, the content is rewritten into a
// temporary file. When this is done, the temporary file replaces
// the original file.
func swapFiles(tempFile *os.File, activeFile *os.File) (*os.File, error) {
	activeFileName := activeFile.Name()
	tempFileName := tempFile.Name()

	// Lots of things we do here is because Windows would not accept working
	// on files that are currently opened.

	// On exit, ensure temporary file is removed.
	defer func() {
		os.Remove(tempFileName)
	}()
	// Start by closing the temporary file.
	if err := tempFile.Close(); err != nil {
		return activeFile, err
	}
	// Close original file before trying to rename it.
	if err := activeFile.Close(); err != nil {
		return activeFile, err
	}
	// Rename the tmp file to original file name
	err := os.Rename(tempFileName, activeFileName)
	// Need to re-open the active file anyway
	file, lerr := openFile(activeFileName)
	if lerr != nil && err == nil {
		err = lerr
	}
	return file, err
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
	closeFile := func(f *os.File) {
		if f == nil {
			return
		}
		if lerr := f.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	err = fs.genericStore.close()
	closeFile(fs.serverFile)
	closeFile(fs.clientsFile)
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileMsgStore methods
////////////////////////////////////////////////////////////////////////////

// newFileMsgStore returns a new instace of a file MsgStore.
func (fs *FileStore) newFileMsgStore(channelDirName, channel string, doRecover bool) (*FileMsgStore, error) {
	var err error
	var file *os.File

	// Create an instance and initialize
	ms := &FileMsgStore{
		opts:     &fs.opts,
		crcTable: fs.crcTable,
	}
	ms.init(channel, fs.limits)

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
			ms.setFile(file)
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

func (ms *FileMsgStore) setFile(f *os.File) {
	ms.bw = nil
	ms.file = f
	if ms.file != nil {
		ms.bw = bufio.NewWriterSize(ms.file, ms.opts.BufferSize)
	}
}

// recovers one of the file
func (ms *FileMsgStore) recoverOneMsgFile(file *os.File, numFile int) error {
	var err error

	msgSize := 0
	var msg *pb.MsgProto

	fslice := ms.files[numFile]

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(file, defaultBufSize)

	for {
		ms.tmpMsgBuf, msgSize, _, err = readRecord(br, ms.tmpMsgBuf, false, ms.crcTable, ms.opts.DoCRC)
		if err != nil {
			if err == io.EOF {
				// We are done, reset err
				err = nil
			}
			break
		}

		// Recover this message
		msg = &pb.MsgProto{}
		err = msg.Unmarshal(ms.tmpMsgBuf[:msgSize])
		if err != nil {
			break
		}

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
	if err == nil && fslice.msgsCount > 0 {
		ms.last = fslice.lastMsg.Sequence
		ms.totalCount += fslice.msgsCount
		ms.totalBytes += fslice.msgsSize
		ms.currSliceIdx = numFile

		// Close the previous file
		if numFile > 0 {
			err = ms.file.Close()
			ms.setFile(nil)
		}
	}
	// Keep the file opened if this is the first or messages were recovered
	if err == nil && (fslice.msgsCount > 0 || numFile == 0) {
		ms.setFile(file)
	} else {
		// Close otherwise...
		if lerr := file.Close(); lerr != nil {
			if err == nil {
				err = lerr
			}
		}
	}
	return err
}

// Store a given message.
func (ms *FileMsgStore) Store(reply string, data []byte) (*pb.MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	fslice := ms.files[ms.currSliceIdx]

	// Check if we need to move to next file slice
	if (ms.currSliceIdx < numFiles-1) &&
		((fslice.msgsCount >= ms.limits.MaxNumMsgs/(numFiles-1)) ||
			(fslice.msgsSize >= ms.limits.MaxMsgBytes/(numFiles-1))) {

		// Don't change store variable until success...
		nextSlice := ms.currSliceIdx + 1

		// Close the file and open the next slice
		if err := ms.flush(); err != nil {
			return nil, err
		}
		if err := ms.file.Close(); err != nil {
			return nil, err
		}
		file, err := openFile(ms.files[nextSlice].fileName)
		if err != nil {
			return nil, err
		}
		// Success, update the store's variables
		ms.setFile(file)
		ms.currSliceIdx = nextSlice

		fslice = ms.files[ms.currSliceIdx]
	}

	seq := ms.last + 1
	m := &pb.MsgProto{
		Sequence:  seq,
		Subject:   ms.subject,
		Reply:     reply,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}

	var err error
	ms.tmpMsgBuf, _, err = writeRecord(ms.bw, ms.tmpMsgBuf, recNoType, m, ms.crcTable)
	if err != nil {
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

	// Enfore limits and update file slice if needed.
	if err := ms.enforceLimits(); err != nil {
		return nil, err
	}
	return m, nil
}

// enforceLimits checks total counts with current msg store's limits,
// removing a file slice and/or updating slices' count as necessary.
func (ms *FileMsgStore) enforceLimits() error {
	// We may inspect several slices, start with the first at index 0.
	idx := 0
	// Check if we need to remove any (but leave at least the last added).
	// Note that we may have to remove more than one msg if we are here
	// after a restart with smaller limits than originally set.
	for ms.totalCount > 1 &&
		((ms.totalCount > ms.limits.MaxNumMsgs) ||
			(ms.totalBytes > ms.limits.MaxMsgBytes)) {

		// slice we are inspecting
		slice := ms.files[idx]
		// Size of the first message in this slice
		firstMsgSize := uint64(len(slice.firstMsg.Data))
		// Update slice and total counts
		slice.msgsCount--
		slice.msgsSize -= firstMsgSize
		ms.totalCount--
		ms.totalBytes -= firstMsgSize

		// Remove the first message from our cache
		if !ms.hitLimit {
			ms.hitLimit = true
			Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxNumMsgs, ms.totalBytes, ms.limits.MaxMsgBytes)
		}
		delete(ms.msgs, ms.first)

		// Messages sequence is incremental with no gap on a given msgstore.
		ms.first++
		// Is file slice "empty"
		if slice.msgsCount == 0 {
			// If we are at the last file slice, remove the first.
			if ms.currSliceIdx == numFiles-1 {
				if err := ms.removeAndShiftFiles(); err != nil {
					return err
				}
				// Decrement the current slice. It will be bumped if needed
				// before storing the next message.
				ms.currSliceIdx--
				// The first slice is gone, go back to 0.
				idx = 0
			} else {
				// No more message...
				slice.firstMsg = nil
				slice.lastMsg = nil

				// We move the index to check the other slices if needed.
				idx++
				// This should not happen, but just in case...
				if idx > ms.currSliceIdx {
					break
				}
			}
		} else {
			// This is the new first message in this slice.
			slice.firstMsg = ms.msgs[ms.first]
		}
	}
	return nil
}

// removeAndShiftFiles
func (ms *FileMsgStore) removeAndShiftFiles() error {
	// Close the currently opened file since it is going to be renamed.
	if err := ms.flush(); err != nil {
		return err
	}
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

	// Create a new file for the last slice.
	fslice := ms.files[numFiles-1]
	file, err := openFile(fslice.fileName)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Reset the last slice's counts.
	fslice.firstMsg = nil
	fslice.lastMsg = nil
	fslice.msgsCount = 0
	fslice.msgsSize = uint64(0)

	// Now re-open the file we closed at the beginning, which is the one
	// before last.
	file, err = openFile(ms.files[numFiles-2].fileName)
	if err != nil {
		return err
	}
	ms.setFile(file)
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
		err = ms.flush()
		if lerr := ms.file.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	return err
}

func (ms *FileMsgStore) flush() error {
	if ms.bw == nil {
		return nil
	}
	if err := ms.bw.Flush(); err != nil {
		return err
	}
	if ms.opts.DoSync {
		return ms.file.Sync()
	}
	return nil
}

// Flush flushes outstanding data into the store.
func (ms *FileMsgStore) Flush() error {
	ms.Lock()
	err := ms.flush()
	ms.Unlock()
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileSubStore methods
////////////////////////////////////////////////////////////////////////////

// newFileSubStore returns a new instace of a file SubStore.
func (fs *FileStore) newFileSubStore(channelDirName, channel string, doRecover bool) (*FileSubStore, error) {
	ss := &FileSubStore{
		rootDir:  channelDirName,
		subs:     make(map[uint64]*subscription),
		opts:     &fs.opts,
		crcTable: fs.crcTable,
	}
	ss.init(channel, fs.limits)
	// Convert the CompactInterval in time.Duration
	ss.compactItvl = time.Duration(ss.opts.CompactInterval) * time.Second

	var err error

	fileName := filepath.Join(channelDirName, subsFileName)
	ss.file, err = openFile(fileName)
	if err != nil {
		return nil, err
	}
	ss.bw = bufio.NewWriterSize(ss.file, ss.opts.BufferSize)
	if doRecover {
		if err := ss.recoverSubscriptions(); err != nil {
			ss.Close()
			return nil, fmt.Errorf("unable to create subscription store for [%s]: %v", channel, err)
		}
	}
	return ss, nil
}

// recoverSubscriptions recovers subscriptions state for this store.
func (ss *FileSubStore) recoverSubscriptions() error {
	var err error
	var recType recordType

	recSize := 0
	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(ss.file, defaultBufSize)

	for {
		ss.tmpSubBuf, recSize, recType, err = readRecord(br, ss.tmpSubBuf, true, ss.crcTable, ss.opts.DoCRC)
		if err != nil {
			if err == io.EOF {
				// We are done, reset err
				err = nil
				break
			} else {
				return err
			}
		}
		ss.fileSize += int64(recSize + recordHeaderSize)
		// Based on record type...
		switch recType {
		case subRecNew:
			newSub := &spb.SubState{}
			if err := newSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			sub := &subscription{
				sub:    newSub,
				seqnos: make(map[uint64]struct{}),
			}
			ss.subs[newSub.ID] = sub
			// Keep track of the subscriptions count
			ss.subsCount++
			// Keep track of max subscription ID found.
			if newSub.ID > ss.maxSubID {
				ss.maxSubID = newSub.ID
			}
			ss.numRecs++
			break
		case subRecUpdate:
			modifiedSub := &spb.SubState{}
			if err := modifiedSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			// Search if the create has been recovered.
			sub, exists := ss.subs[modifiedSub.ID]
			if exists {
				sub.sub = modifiedSub
				// An update means that the previous version is free space.
				ss.delRecs++
			} else {
				sub := &subscription{
					sub:    modifiedSub,
					seqnos: make(map[uint64]struct{}),
				}
				ss.subs[modifiedSub.ID] = sub
			}
			// Keep track of max subscription ID found.
			if modifiedSub.ID > ss.maxSubID {
				ss.maxSubID = modifiedSub.ID
			}
			ss.numRecs++
			break
		case subRecDel:
			delSub := spb.SubStateDelete{}
			if err := delSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if s, exists := ss.subs[delSub.ID]; exists {
				delete(ss.subs, delSub.ID)
				// Keep track of the subscriptions count
				ss.subsCount--
				// Delete and count all non-ack'ed messages free space.
				ss.delRecs++
				ss.delRecs += len(s.seqnos)
			}
			// Keep track of max subscription ID found.
			if delSub.ID > ss.maxSubID {
				ss.maxSubID = delSub.ID
			}
			break
		case subRecMsg:
			updateSub := spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if sub, exists := ss.subs[updateSub.ID]; exists {
				seqno := updateSub.Seqno
				// Same seqno/ack can appear several times for the same sub.
				// See queue subscribers redelivery.
				if seqno > sub.sub.LastSent {
					sub.sub.LastSent = seqno
				}
				sub.seqnos[seqno] = struct{}{}
				ss.numRecs++
			}
			break
		case subRecAck:
			updateSub := spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if sub, exists := ss.subs[updateSub.ID]; exists {
				delete(sub.seqnos, updateSub.Seqno)
				// A message is ack'ed
				ss.delRecs++
			}
			break
		default:
			return fmt.Errorf("unexpected record type: %v", recType)
		}
	}
	return nil
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
	if err := ss.writeRecord(ss.bw, subRecNew, sub); err != nil {
		return err
	}
	s := &subscription{sub: sub, seqnos: make(map[uint64]struct{})}
	ss.subs[sub.ID] = s
	return nil
}

// UpdateSub updates a given subscription represented by SubState.
func (ss *FileSubStore) UpdateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()
	if err := ss.writeRecord(ss.bw, subRecUpdate, sub); err != nil {
		return err
	}
	s := ss.subs[sub.ID]
	if s != nil {
		s.sub = sub
	} else {
		s := &subscription{sub: sub, seqnos: make(map[uint64]struct{})}
		ss.subs[sub.ID] = s
	}
	return nil
}

// DeleteSub invalidates this subscription.
func (ss *FileSubStore) DeleteSub(subid uint64) {
	ss.Lock()
	ss.delSub.ID = subid
	ss.writeRecord(ss.bw, subRecDel, &ss.delSub)
	if s, exists := ss.subs[subid]; exists {
		delete(ss.subs, subid)
		// writeRecord has already accounted for the count of the
		// delete record. We add to this the number of pending messages
		ss.delRecs += len(s.seqnos)
		// Check if this triggers a need for compaction
		if ss.shouldCompact() {
			ss.compact()
		}
	}
	ss.Unlock()
}

// shouldCompact returns a boolean indicating if we should compact
// Lock is held by caller
func (ss *FileSubStore) shouldCompact() bool {
	// Gobal switch
	if !ss.opts.CompactEnabled {
		return false
	}
	// Check that if minimum file size is set, the client file
	// is at least at the minimum.
	if ss.opts.CompactMinFileSize > 0 && ss.fileSize < ss.opts.CompactMinFileSize {
		return false
	}
	// Check fragmentation
	frag := 0
	if ss.numRecs == 0 {
		frag = 100
	} else {
		frag = ss.delRecs * 100 / ss.numRecs
	}
	if frag < ss.opts.CompactFragmentation {
		return false
	}
	// Check that we don't compact too often
	if time.Now().Sub(ss.compactTS) < ss.compactItvl {
		return false
	}
	return true
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	ss.updateSub.ID, ss.updateSub.Seqno = subid, seqno
	if err := ss.writeRecord(ss.bw, subRecMsg, &ss.updateSub); err != nil {
		ss.Unlock()
		return err
	}
	s := ss.subs[subid]
	if s != nil {
		s.seqnos[seqno] = struct{}{}
	}
	ss.Unlock()
	return nil
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (ss *FileSubStore) AckSeqPending(subid, seqno uint64) error {
	ss.Lock()
	ss.updateSub.ID, ss.updateSub.Seqno = subid, seqno
	if err := ss.writeRecord(ss.bw, subRecAck, &ss.updateSub); err != nil {
		ss.Unlock()
		return err
	}
	s := ss.subs[subid]
	if s != nil {
		delete(s.seqnos, seqno)
		// Test if we should compact
		if ss.shouldCompact() {
			ss.compact()
		}
	}
	ss.Unlock()
	return nil
}

// compact rewrites all subscriptions on a temporary file, reducing the size
// since we get rid of deleted subscriptions and message sequences that have
// been acknowledged. On success, the subscriptions file is replaced by this
// temporary file.
// Lock is held by caller
func (ss *FileSubStore) compact() error {
	tmpFile, err := getTempFile(ss.rootDir, "subs")
	if err != nil {
		return err
	}
	tmpBW := bufio.NewWriterSize(tmpFile, defaultBufSize)
	// Save values in case of failed compaction
	savedNumRecs := ss.numRecs
	savedDelRecs := ss.delRecs
	savedFileSize := ss.fileSize
	// Cleanup in case of error during compact
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			// Since we failed compaction, restore values
			ss.numRecs = savedNumRecs
			ss.delRecs = savedDelRecs
			ss.fileSize = savedFileSize
		}
	}()
	// Reset to 0 since writeRecord() is updating the values.
	ss.numRecs = 0
	ss.delRecs = 0
	ss.fileSize = 0
	for _, sub := range ss.subs {
		err = ss.writeRecord(tmpBW, subRecNew, sub.sub)
		if err != nil {
			return err
		}
		ss.updateSub.ID = sub.sub.ID
		for seqno := range sub.seqnos {
			ss.updateSub.Seqno = seqno
			err = ss.writeRecord(tmpBW, subRecMsg, &ss.updateSub)
			if err != nil {
				return err
			}
		}
	}
	// Flush and sync the temporary file
	err = tmpBW.Flush()
	if err != nil {
		return err
	}
	err = tmpFile.Sync()
	if err != nil {
		return err
	}
	// Switch the temporary file with the original one.
	ss.file, err = swapFiles(tmpFile, ss.file)
	if err != nil {
		return err
	}
	// Prevent cleanup on success
	tmpFile = nil

	ss.bw = bufio.NewWriterSize(ss.file, ss.opts.BufferSize)
	// Update the timestamp of this last successful compact
	ss.compactTS = time.Now()
	return nil
}

// writes a record in the subscriptions file.
// store's lock is held on entry.
func (ss *FileSubStore) writeRecord(w io.Writer, recType recordType, rec record) error {
	var err error
	totalSize := 0
	ss.tmpSubBuf, totalSize, err = writeRecord(w, ss.tmpSubBuf, recType, rec, ss.crcTable)
	if err != nil {
		return err
	}
	switch recType {
	case subRecNew:
		ss.numRecs++
	case subRecMsg:
		ss.numRecs++
	case subRecAck:
		// An ack makes the message record free space
		ss.delRecs++
	case subRecUpdate:
		ss.numRecs++
		// An update makes the old record free space
		ss.delRecs++
	case subRecDel:
		ss.delRecs++
	default:
		panic(fmt.Errorf("Record type %v unknown", recType))
	}
	ss.fileSize += int64(totalSize)
	return nil
}

func (ss *FileSubStore) flush() error {
	if ss.bw == nil {
		return nil
	}
	if err := ss.bw.Flush(); err != nil {
		return err
	}
	if ss.opts.DoSync {
		return ss.file.Sync()
	}
	return nil
}

// Flush persists buffered operations to disk.
func (ss *FileSubStore) Flush() error {
	ss.Lock()
	err := ss.flush()
	ss.Unlock()
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
		err = ss.flush()
		if lerr := ss.file.Close(); lerr != nil && err == nil {
			err = lerr
		}
	}
	return err
}
