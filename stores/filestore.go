// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
	"sort"
	"sync"
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

	// Size of an message index record
	// Seq - Offset - Timestamp - Size - CRC
	msgIndexRecSize = 8 + 8 + 8 + 4 + crcSize

	// Percentage of buffer usage to decide if the buffer should shrink
	bufShrinkThreshold = 50

	// If FileStoreOption's BufferSize is > 0, the buffer writer is initially
	// created with this size (unless this is > than BufferSize, in which case
	// BufferSize is used). When possible, the buffer will shrink but not lower
	// than this value. This is for FileSubStore's
	subBufMinShrinkSize = 128

	// Interval for the FileSubStore's shrink buffer timer callback
	subShrinkTimerInterval = 5 * time.Second
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

	// CacheMsgs allows MsgStore objects to keep messages in memory after being
	// written to disk. This allows fast lookups at the expense of memory usage.
	CacheMsgs bool
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
	CacheMsgs:            true,
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

// CacheMsgs is a FileStore option that allows channels' message stores to
// keep messages in memory after being stored for fast lookup performance
// (at the expense of memory).
func CacheMsgs(cache bool) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.CacheMsgs = cache
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
	bufSize     int          // current buffer size of the buffer writer if one is used
	shrinkReq   bool         // used to decide if the buffer should shrink
	activity    bool         // was there any write between two flush calls
	writer      io.Writer    // this is either `bw` or `file` depending if buffer writer is used or not
	shrinkTimer *time.Timer  // timer associated with callback shrinking buffer when possible
	allDone     sync.WaitGroup
}

// fileSlice represents one of the message store file (there are a number
// of files for a MsgStore on a given channel).
type fileSlice struct {
	fileName  string
	idxFName  string
	firstSeq  uint64
	lastSeq   uint64
	msgsCount int
	msgsSize  uint64
	file      *os.File // Used during lookups.
	lastUsed  int64
}

// msgRecord contains data regarding a message that the FileMsgStore needs to
// keep in memory for performance reasons.
type msgRecord struct {
	offset    int64
	timestamp int64
	msg       *pb.MsgProto // will be nil when message flushed on disk and/or no caching allowed.
	msgSize   uint32
}

// FileMsgStore is a per channel message file store.
type FileMsgStore struct {
	genericMsgStore
	tmpMsgBuf    []byte
	file         *os.File
	idxFile      *os.File
	bw           *bufio.Writer
	files        [numFiles]*fileSlice
	currSliceIdx int
	fstore       *FileStore // pointers to file store object
	msgs         map[uint64]*msgRecord
	wOffset      int64
	firstMsg     *pb.MsgProto
	lastMsg      *pb.MsgProto
	bufferedMsgs []uint64
	tasksTimer   *time.Timer // close file slices not recently used, etc...
	timeTick     int64       // time captured in background task
	allDone      sync.WaitGroup
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
			// The server is making a copy of rss.Sub, still it is not
			// a good idea to return a pointer to an object that belong
			// to the store. So make a copy and return the pointer to
			// that copy.
			csub := *sub.sub
			rss := &RecoveredSubState{
				Sub:     &csub,
				Pending: make(PendingAcks),
			}
			// If we recovered any seqno...
			if len(sub.seqnos) > 0 {
				// Lookup messages, and if we find those, update the
				// Pending map.
				for seq := range sub.seqnos {
					rss.Pending[seq] = struct{}{}
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
	var useIdxFile bool

	// Create an instance and initialize
	ms := &FileMsgStore{
		fstore:       fs,
		msgs:         make(map[uint64]*msgRecord, 64),
		wOffset:      int64(4), // The very first record starts after the file version record
		bufferedMsgs: make([]uint64, 0, 1),
	}
	ms.init(channel, fs.limits)

	done := false
	for i := 0; err == nil && i < numFiles; i++ {
		// Fully qualified file name.
		fileName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.dat", (i+1)))
		idxFName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.idx", (i+1)))

		// Create slice
		ms.files[i] = &fileSlice{fileName: fileName, idxFName: idxFName}

		if done {
			continue
		}

		// On recovery...
		if doRecover {
			// We previously pre-created all files, we don't anymore.
			// So if a slice is not present, we are done.
			if s, statErr := os.Stat(fileName); s == nil || statErr != nil {
				done = true
				continue
			}
			// If an index file is present, recover only the index file, not the data.
			if s, statErr := os.Stat(idxFName); s != nil && statErr == nil {
				useIdxFile = true
			}
		}
		// On create, simply create the first file, on recovery we need to recover
		// each existing file
		if i == 0 || doRecover {
			err = ms.openDataAndIndexFiles(fileName, idxFName)
		}
		// Should we try to recover (startup case)
		if err == nil && doRecover {
			done, err = ms.recoverOneMsgFile(useIdxFile, i)
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

// openDataAndIndexFiles opens/creates the data and index file with the given
// file names.
func (ms *FileMsgStore) openDataAndIndexFiles(dataFileName, idxFileName string) error {
	file, err := openFile(dataFileName)
	if err != nil {
		return err
	}
	idxFile, err := openFile(idxFileName)
	if err != nil {
		file.Close()
		return err
	}
	ms.setFile(file, idxFile)
	return nil
}

// closeDataAndIndexFiles closes both current data and index files.
func (ms *FileMsgStore) closeDataAndIndexFiles() error {
	err := ms.flush()
	if cerr := ms.file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if cerr := ms.idxFile.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// setFile sets the current data and index file.
// The buffered writer is recreated.
func (ms *FileMsgStore) setFile(dataFile, idxFile *os.File) {
	ms.bw = nil
	ms.file = dataFile
	if ms.file != nil {
		ms.bw = bufio.NewWriterSize(ms.file, ms.fstore.opts.BufferSize)
	}
	ms.idxFile = idxFile
}

// recovers one of the file
func (ms *FileMsgStore) recoverOneMsgFile(useIdxFile bool, numFile int) (bool, error) {
	var err error

	msgSize := 0
	var msg *pb.MsgProto
	var mrec *msgRecord
	var seq uint64
	done := false

	fslice := ms.files[numFile]

	// Select which file to recover based on presence of index file
	file := ms.file
	if useIdxFile {
		file = ms.idxFile
	}

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(file, defaultBufSize)

	// The first record starts after the file version record
	offset := int64(4)

	if useIdxFile {
		for {
			seq, mrec, err = ms.readIndex(br)
			if err != nil {
				if err == io.EOF {
					// We are done, reset err
					err = nil
				}
				break
			}

			// Update file slice
			if fslice.firstSeq == 0 {
				fslice.firstSeq = seq
			}
			fslice.lastSeq = seq
			fslice.msgsCount++
			fslice.msgsSize += uint64(mrec.msgSize)

			if ms.first == 0 {
				ms.first = seq
			}
		}
	} else {
		// Get these from the file store object
		crcTable := ms.fstore.crcTable
		doCRC := ms.fstore.opts.DoCRC
		cache := ms.fstore.opts.CacheMsgs

		// We are going to write the index file while recovering the data file
		bw := bufio.NewWriterSize(ms.idxFile, msgIndexRecSize*1000)

		for {
			ms.tmpMsgBuf, msgSize, _, err = readRecord(br, ms.tmpMsgBuf, false, crcTable, doCRC)
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

			if fslice.firstSeq == 0 {
				fslice.firstSeq = msg.Sequence
			}
			fslice.lastSeq = msg.Sequence
			fslice.msgsCount++
			fslice.msgsSize += uint64(msgSize)

			if ms.first == 0 {
				ms.first = msg.Sequence
				ms.firstMsg = msg
			}
			ms.lastMsg = msg
			mrec := &msgRecord{offset: offset, timestamp: msg.Timestamp, msgSize: uint32(msgSize)}
			ms.msgs[msg.Sequence] = mrec
			if cache {
				mrec.msg = msg
			}
			// There was no index file, update it
			err = ms.writeIndex(bw, msg.Sequence, offset, msg.Timestamp, msgSize)
			if err != nil {
				break
			}
			// Move offset
			offset += int64(recordHeaderSize + msgSize)
		}
		if err == nil {
			err = bw.Flush()
			if err == nil {
				err = ms.idxFile.Sync()
			}
		}
		// Since there was no index and there was an error, remove the index
		// file so when server restarts, it recovers again from the data file.
		if err != nil {
			// Close the index file
			ms.idxFile.Close()
			// Remove it, and panic if we can't
			if rmErr := os.Remove(fslice.idxFName); rmErr != nil {
				panic(fmt.Errorf("Error during recovery of file %q: %v, you need "+
					"to manually remove index file %q (remove failed with err: %v)",
					fslice.fileName, err, fslice.idxFName, rmErr))
			}
		}
	}

	// Do more accounting and bump the current slice index if we recovered
	// at least one message on that file.
	if err == nil && fslice.msgsCount > 0 {
		ms.last = fslice.lastSeq
		ms.totalCount += fslice.msgsCount
		ms.totalBytes += fslice.msgsSize
		ms.currSliceIdx = numFile
		if useIdxFile {
			// Take the offset of the end of file
			ms.wOffset, err = ms.file.Seek(0, 2)
		} else {
			ms.wOffset = offset
		}
	} else if err == nil {
		done = true
		// If we are not at the first slice and we did not recover anything,
		// close the current data and index files and open the ones from
		// the previous slice.
		if numFile > 0 {
			ms.closeDataAndIndexFiles()
			err = ms.openDataAndIndexFiles(
				ms.files[numFile-1].fileName, ms.files[numFile-1].idxFName)
		}
	}
	return done, err
}

// writeIndex writes a message index record to the writer `w`
func (ms *FileMsgStore) writeIndex(w io.Writer, seq uint64, offset, timestamp int64, msgSize int) error {
	_buf := [msgIndexRecSize]byte{}
	buf := _buf[:]
	ms.addIndex(buf, seq, offset, timestamp, msgSize)
	_, err := w.Write(buf[:msgIndexRecSize])
	return err
}

// addIndex adds a message index record in the given buffer
func (ms *FileMsgStore) addIndex(buf []byte, seq uint64, offset, timestamp int64, msgSize int) {
	util.ByteOrder.PutUint64(buf, seq)
	util.ByteOrder.PutUint64(buf[8:], uint64(offset))
	util.ByteOrder.PutUint64(buf[16:], uint64(timestamp))
	util.ByteOrder.PutUint32(buf[24:], uint32(msgSize))
	crc := crc32.Checksum(buf[:msgIndexRecSize-crcSize], ms.fstore.crcTable)
	util.ByteOrder.PutUint32(buf[msgIndexRecSize-crcSize:], crc)
}

// readIndex reads a message index record from the given reader
// and returns an allocated msgRecord object.
func (ms *FileMsgStore) readIndex(r io.Reader) (uint64, *msgRecord, error) {
	_buf := [msgIndexRecSize]byte{}
	buf := _buf[:]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	mrec := &msgRecord{}
	seq := util.ByteOrder.Uint64(buf)
	mrec.offset = int64(util.ByteOrder.Uint64(buf[8:]))
	mrec.timestamp = int64(util.ByteOrder.Uint64(buf[16:]))
	mrec.msgSize = util.ByteOrder.Uint32(buf[24:])
	if ms.fstore.opts.DoCRC {
		storedCRC := util.ByteOrder.Uint32(buf[msgIndexRecSize-crcSize:])
		crc := crc32.Checksum(buf[:msgIndexRecSize-crcSize], ms.fstore.crcTable)
		if storedCRC != crc {
			return 0, nil, fmt.Errorf("corrupted data, expected crc to be 0x%08x, got 0x%08x", storedCRC, crc)
		}
	}
	ms.msgs[seq] = mrec
	return seq, mrec, nil
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
		if err := ms.closeDataAndIndexFiles(); err != nil {
			return nil, err
		}
		// Open the new slice
		if err := ms.openDataAndIndexFiles(
			ms.files[nextSlice].fileName,
			ms.files[nextSlice].idxFName); err != nil {
			return nil, err
		}
		// Success, update the store's variables
		ms.currSliceIdx = nextSlice
		ms.wOffset = int64(4)

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

	cache := ms.fstore.opts.CacheMsgs
	pinMsg := false

	var recSize int
	var err error

	bufferedBeforeWrite := ms.bw.Buffered()
	ms.tmpMsgBuf, recSize, err = writeRecord(ms.bw, ms.tmpMsgBuf, recNoType, m, ms.fstore.crcTable)
	if err != nil {
		return nil, err
	}
	bufferedAfterWrite := ms.bw.Buffered()
	msgSize := recSize - recordHeaderSize
	// We need to know which messages where flushed on disk so we
	// correctly update the index file.
	// We know that writeRecord will flush the buffer if it can not fit
	// the current record. We also know that bufio will not use the internal
	// buffer if what we try to write is bigger than the bw capacity (bufio
	// writes directly to the underlying writer).
	// So, there are 3 outcomes for bufferedAfter:
	// - > recSize:             pin current msg
	// - recSize  : unpin all + pin current msg
	// - 0		  : unpin all
	if bufferedBeforeWrite > 0 && bufferedAfterWrite <= recSize {
		bufPos := 0
		// We need to add the current if it has been flushed to disk too.
		oneMore := (bufferedAfterWrite == 0)
		// Add all index records in tmpMsgBuf
		ms.tmpMsgBuf, bufPos = ms.processBufferedMsgs(ms.tmpMsgBuf, oneMore)
		// Add the current message if applicable
		if oneMore {
			ms.addIndex(ms.tmpMsgBuf[bufPos:], seq, ms.wOffset, m.Timestamp, msgSize)
			bufPos += msgIndexRecSize
		}
		// If there is something to write, do so now.
		if bufPos > 0 {
			if _, err := ms.idxFile.Write(ms.tmpMsgBuf[:bufPos]); err != nil {
				return nil, err
			}
		}
	}
	// no else here...
	if bufferedAfterWrite >= recSize {
		ms.bufferedMsgs = append(ms.bufferedMsgs, seq)
		pinMsg = true
	}

	if ms.first == 0 {
		ms.first = 1
		ms.firstMsg = m
	}
	ms.last = seq
	ms.lastMsg = m
	mrec := &msgRecord{offset: ms.wOffset, timestamp: m.Timestamp, msgSize: uint32(msgSize)}
	ms.msgs[ms.last] = mrec
	if cache || pinMsg {
		mrec.msg = m
	}
	ms.wOffset += int64(recSize)

	// Total stats
	ms.totalCount++
	ms.totalBytes += uint64(msgSize)

	// Stats per file slice
	fslice.msgsCount++
	fslice.msgsSize += uint64(msgSize)

	// Save references to first and last sequences for this slice
	if fslice.firstSeq == 0 {
		fslice.firstSeq = seq
	}
	fslice.lastSeq = seq

	// Enfore limits and update file slice if needed.
	if err := ms.enforceLimits(); err != nil {
		return nil, err
	}
	return m, nil
}

// processBufferedMsgs adds message index records in the given buffer
// for every pending buffered messages. If `addOneMore` is true, the
// returned buffer is big enough to accomodate another message index
// record.
func (ms *FileMsgStore) processBufferedMsgs(buf []byte, addOneMore bool) ([]byte, int) {
	idxBufferSize := len(ms.bufferedMsgs) * msgIndexRecSize
	if addOneMore {
		idxBufferSize += msgIndexRecSize
	}
	buf = util.EnsureBufBigEnough(buf, idxBufferSize)
	bufOffset := 0
	cache := ms.fstore.opts.CacheMsgs
	for _, pseq := range ms.bufferedMsgs {
		mrec := ms.msgs[pseq]
		if mrec != nil {
			// We add the index info for this flushed message
			ms.addIndex(buf[bufOffset:], pseq, mrec.offset, mrec.timestamp, int(mrec.msgSize))
			bufOffset += msgIndexRecSize
			if !cache {
				mrec.msg = nil
			}
		}
	}
	ms.bufferedMsgs = ms.bufferedMsgs[:0]
	return buf, bufOffset
}

// enforceLimits checks total counts with current msg store's limits,
// removing a file slice and/or updating slices' count as necessary.
func (ms *FileMsgStore) enforceLimits() error {
	// We may inspect several slices, start with the first at index 0.
	idx := 0
	var slice *fileSlice
	// Check if we need to remove any (but leave at least the last added).
	// Note that we may have to remove more than one msg if we are here
	// after a restart with smaller limits than originally set.
	for ms.totalCount > 1 &&
		((ms.totalCount > ms.limits.MaxNumMsgs) ||
			(ms.totalBytes > ms.limits.MaxMsgBytes)) {

		// slice we are inspecting, make sure the slice is not
		// empty.
		for slice = ms.files[idx]; slice.msgsCount == 0; idx++ {
		}
		// Size of the first message in this slice
		firstMsgSize := uint64(ms.msgs[slice.firstSeq].msgSize)
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
		// Invalidate ms.firstMsg, it will be looked-up on demand.
		ms.firstMsg = nil
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
				slice.firstSeq = 0
				slice.lastSeq = 0

				// We move the index to check the other slices if needed.
				idx++
				// This should not happen, but just in case...
				if idx > ms.currSliceIdx {
					break
				}
			}
		} else {
			// This is the new first message in this slice.
			slice.firstSeq = ms.first
		}
	}
	return nil
}

// removeAndShiftFiles
func (ms *FileMsgStore) removeAndShiftFiles() error {
	// Close the currently opened file since it is going to be renamed.
	if err := ms.closeDataAndIndexFiles(); err != nil {
		return err
	}
	// Close all files that may have been opened due to lookups
	for _, slice := range ms.files {
		if slice.file != nil {
			slice.file.Close()
			slice.file = nil
		}
	}

	// Rename msgs.2.dat to msgs.1.dat, refresh state, etc...
	for i := 0; i < numFiles-1; i++ {
		file1 := ms.files[i]
		file2 := ms.files[i+1]

		if err := os.Rename(file2.fileName, file1.fileName); err != nil {
			return err
		}
		if err := os.Rename(file2.idxFName, file1.idxFName); err != nil {
			return err
		}
		// Update total stats for the first store being removed
		if i == 0 {
			ms.totalCount -= file1.msgsCount
			ms.totalBytes -= file1.msgsSize

			// Remove all messages from first file from our cache
			seqStart := file1.firstSeq
			seqEnd := file1.lastSeq
			for i := seqStart; i <= seqEnd; i++ {
				delete(ms.msgs, i)
			}
			// Update sequence of first available message
			ms.first = file2.firstSeq
		}

		// Copy over values from the next slice
		file1.firstSeq = file2.firstSeq
		file1.lastSeq = file2.lastSeq
		file1.msgsCount = file2.msgsCount
		file1.msgsSize = file2.msgsSize
	}

	// Create new files for the last slice.
	fslice := ms.files[numFiles-1]
	if err := ms.openDataAndIndexFiles(fslice.fileName, fslice.idxFName); err != nil {
		return err
	}
	// Close them now
	if err := ms.closeDataAndIndexFiles(); err != nil {
		return err
	}
	// Reset the last slice's counts.
	fslice.firstSeq = 0
	fslice.lastSeq = 0
	fslice.msgsCount = 0
	fslice.msgsSize = uint64(0)

	// Now re-open the file we closed at the beginning, which is the one
	// before last.
	if err := ms.openDataAndIndexFiles(
		ms.files[numFiles-2].fileName,
		ms.files[numFiles-2].idxFName); err != nil {
		return err
	}
	return nil
}

// getFileForSeq returns the file where the message of the given sequence
// is stored. If the file is opened, a task is triggered to close this
// file when no longer used after a period of time.
func (ms *FileMsgStore) getFileForSeq(seq uint64) (*os.File, error) {
	// Start with current slice
	slice := ms.files[ms.currSliceIdx]
	if (slice.firstSeq <= seq) && (seq <= slice.lastSeq) {
		return ms.file, nil
	}
	// Check all previous slices
	for i := 0; i < ms.currSliceIdx; i++ {
		slice = ms.files[i]
		if (slice.firstSeq <= seq) && (seq <= slice.lastSeq) {
			file := slice.file
			if file == nil {
				var err error
				file, err = openFile(slice.fileName)
				if err != nil {
					return nil, fmt.Errorf("unable to open file %q: %v", slice.fileName, err)
				}
				slice.file = file
				if ms.tasksTimer == nil {
					ms.allDone.Add(1)
					ms.timeTick = time.Now().UnixNano()
					ms.tasksTimer = time.AfterFunc(time.Second, ms.backgroundTasks)
				}
			}
			slice.lastUsed = ms.timeTick
			return file, nil
		}
	}
	return nil, fmt.Errorf("could not find file slice for store %q, message seq: %v", ms.subject, seq)
}

// backgroundTasks performs some background tasks related to this
// messages store.
func (ms *FileMsgStore) backgroundTasks() {
	ms.Lock()
	defer ms.Unlock()

	if ms.closed {
		ms.allDone.Done()
		return
	}

	// Update time
	ms.timeTick = time.Now().UnixNano()

	// Close unused file slices
	for i := 0; i < ms.currSliceIdx; i++ {
		slice := ms.files[i]
		if slice.file != nil && time.Duration(ms.timeTick-slice.lastUsed) >= time.Second {
			slice.file.Close()
			slice.file = nil
		}
	}

	// Fire again..
	ms.tasksTimer.Reset(time.Second)
}

// lookup returns the message for the given sequence number, possibly
// reading the message from disk.
func (ms *FileMsgStore) lookup(seq uint64) *pb.MsgProto {
	var msg *pb.MsgProto
	m := ms.msgs[seq]
	if m != nil {
		msg = m.msg
		if msg == nil {
			var msgSize int
			// Look in which file slice the message is located.
			file, err := ms.getFileForSeq(seq)
			if err != nil {
				return nil
			}
			// Position file to message's offset. 0 means from start.
			if _, err := file.Seek(m.offset, 0); err != nil {
				return nil
			}
			ms.tmpMsgBuf, msgSize, _, err = readRecord(file, ms.tmpMsgBuf, false, ms.fstore.crcTable, ms.fstore.opts.DoCRC)
			if err != nil {
				return nil
			}
			// Recover this message
			msg = &pb.MsgProto{}
			err = msg.Unmarshal(ms.tmpMsgBuf[:msgSize])
			if err != nil {
				return nil
			}
			// Cache if allowed
			if ms.fstore.opts.CacheMsgs {
				m.msg = msg
			}
		}
	}
	return msg
}

// Lookup returns the stored message with given sequence number.
func (ms *FileMsgStore) Lookup(seq uint64) *pb.MsgProto {
	ms.Lock()
	msg := ms.lookup(seq)
	ms.Unlock()
	return msg
}

// FirstMsg returns the first message stored.
func (ms *FileMsgStore) FirstMsg() *pb.MsgProto {
	ms.RLock()
	if ms.firstMsg == nil {
		ms.firstMsg = ms.lookup(ms.first)
	}
	m := ms.firstMsg
	ms.RUnlock()
	return m
}

// LastMsg returns the last message stored.
func (ms *FileMsgStore) LastMsg() *pb.MsgProto {
	ms.RLock()
	if ms.lastMsg == nil {
		ms.lastMsg = ms.lookup(ms.last)
	}
	m := ms.lastMsg
	ms.RUnlock()
	return m
}

// GetSequenceFromTimestamp returns the sequence of the first message whose
// timestamp is greater or equal to given timestamp.
func (ms *FileMsgStore) GetSequenceFromTimestamp(timestamp int64) uint64 {
	ms.RLock()
	defer ms.RUnlock()

	index := sort.Search(len(ms.msgs), func(i int) bool {
		if ms.msgs[uint64(i)+ms.first].timestamp >= timestamp {
			return true
		}
		return false
	})

	return uint64(index) + ms.first
}

// Close closes the store.
func (ms *FileMsgStore) Close() error {
	ms.Lock()
	if ms.closed {
		ms.Unlock()
		return nil
	}

	ms.closed = true

	var err error
	// Close file slices that may have been opened due to
	// message lookups.
	for i := 0; i < ms.currSliceIdx; i++ {
		slice := ms.files[i]
		if slice.file != nil {
			if lerr := slice.file.Close(); lerr != nil && err == nil {
				err = lerr
			}
		}
	}
	// Flush and close current files
	if lerr := ms.closeDataAndIndexFiles(); lerr != nil && err == nil {
		err = lerr
	}
	if ms.tasksTimer != nil {
		if ms.tasksTimer.Stop() {
			// If we can stop, timer callback won't fire,
			// so we need to decrement the wait group.
			ms.allDone.Done()
		}
	}

	ms.Unlock()

	// Wait on go routines/timers to finish
	ms.allDone.Wait()

	return err
}

func (ms *FileMsgStore) flush() error {
	if ms.bw != nil && ms.bw.Buffered() > 0 {
		if err := ms.bw.Flush(); err != nil {
			return err
		}
		// If there were pending buffered messages, they have now been
		// written to disk, so we need to write the corresponding
		// message index records.
		bufPos := 0
		ms.tmpMsgBuf, bufPos = ms.processBufferedMsgs(ms.tmpMsgBuf, false)
		if bufPos > 0 {
			if _, err := ms.idxFile.Write(ms.tmpMsgBuf[:bufPos]); err != nil {
				return err
			}
		}
	}
	if ms.fstore.opts.DoSync {
		if err := ms.file.Sync(); err != nil {
			return err
		}
		if err := ms.idxFile.Sync(); err != nil {
			return err
		}
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
	maxBufSize := ss.opts.BufferSize
	// Set ss.bufSize before calling ss.setWriter()
	if maxBufSize > 0 {
		// If BufferSize is big enough, start with a minimum
		// buffer size. The buffer will grow as needed.
		if maxBufSize >= subBufMinShrinkSize {
			ss.bufSize = subBufMinShrinkSize
		} else {
			// Otherwise, use the size as given.
			ss.bufSize = maxBufSize
		}
	}
	// Grab the lock here to prevent possible shrinkTimer to start until we are done
	ss.Lock()
	ss.setWriter()
	// Do not attempt to shrink unless the option is greater than the
	// minimum shrinkable size.
	if maxBufSize > subBufMinShrinkSize {
		ss.allDone.Add(1)
		ss.shrinkTimer = time.AfterFunc(subShrinkTimerInterval, ss.shrinkBuffer)
	}
	if doRecover {
		if err := ss.recoverSubscriptions(); err != nil {
			ss.Unlock()
			ss.Close()
			return nil, fmt.Errorf("unable to create subscription store for [%s]: %v", channel, err)
		}
	}
	ss.Unlock()
	return ss, nil
}

// setWriter sets the writer to either file or buffered writer (and create it),
// based on store option.
func (ss *FileSubStore) setWriter() {
	ss.writer = ss.file
	if ss.bufSize > 0 {
		ss.bw = bufio.NewWriterSize(ss.file, ss.bufSize)
		ss.writer = ss.bw
	}
}

// shrinkBuffer is a timer callback that shrinks the buffer writer when possible
func (ss *FileSubStore) shrinkBuffer() {
	ss.Lock()
	defer ss.Unlock()

	if ss.closed {
		ss.allDone.Done()
		return
	}

	// Make the timer fire again with a defer since we have multiple returns.
	defer func() {
		ss.shrinkTimer.Reset(subShrinkTimerInterval)
	}()

	// Nothing to do if we are already at the lowest
	if ss.bufSize == subBufMinShrinkSize {
		return
	}

	if !ss.shrinkReq {
		percentFilled := ss.bw.Buffered() * 100 / ss.bufSize
		if percentFilled <= bufShrinkThreshold {
			ss.shrinkReq = true
		}
		// We are requesting to shrink the buffer and will wait the next
		// tick. If this request is still true, we will shrink, otherwise
		// check again...
		return
	}
	// If we get an error simply return here. The buffer (in bufio) memorizes
	// the error so any other write/flush on that buffer will fail. We will get
	// the error at the next "synchronous" operation where we can report back
	// to the user.
	if err := ss.bw.Flush(); err != nil {
		return
	}
	ss.bufSize /= 2
	if ss.bufSize < subBufMinShrinkSize {
		ss.bufSize = subBufMinShrinkSize
	}
	ss.bw = bufio.NewWriterSize(ss.file, ss.bufSize)
	ss.writer = ss.bw
	// Don't reset shrinkReq to false unless we are down to minimum size.
	if ss.bufSize == subBufMinShrinkSize {
		ss.shrinkReq = false
	}
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
	if err := ss.writeRecord(ss.writer, subRecNew, sub); err != nil {
		return err
	}
	// We need to get a copy of the passed sub, we can't hold a reference
	// to it.
	csub := *sub
	s := &subscription{sub: &csub, seqnos: make(map[uint64]struct{})}
	ss.subs[sub.ID] = s
	return nil
}

// UpdateSub updates a given subscription represented by SubState.
func (ss *FileSubStore) UpdateSub(sub *spb.SubState) error {
	ss.Lock()
	defer ss.Unlock()
	if err := ss.writeRecord(ss.writer, subRecUpdate, sub); err != nil {
		return err
	}
	// We need to get a copy of the passed sub, we can't hold a reference
	// to it.
	csub := *sub
	s := ss.subs[sub.ID]
	if s != nil {
		s.sub = &csub
	} else {
		s := &subscription{sub: &csub, seqnos: make(map[uint64]struct{})}
		ss.subs[sub.ID] = s
	}
	return nil
}

// DeleteSub invalidates this subscription.
func (ss *FileSubStore) DeleteSub(subid uint64) {
	ss.Lock()
	ss.delSub.ID = subid
	ss.writeRecord(ss.writer, subRecDel, &ss.delSub)
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
	if err := ss.writeRecord(ss.writer, subRecMsg, &ss.updateSub); err != nil {
		ss.Unlock()
		return err
	}
	s := ss.subs[subid]
	if s != nil {
		if seqno > s.sub.LastSent {
			s.sub.LastSent = seqno
		}
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
	if err := ss.writeRecord(ss.writer, subRecAck, &ss.updateSub); err != nil {
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

	// Set the file and create buffered writer if applicable
	ss.setWriter()
	// Update the timestamp of this last successful compact
	ss.compactTS = time.Now()
	return nil
}

// writes a record in the subscriptions file.
// store's lock is held on entry.
func (ss *FileSubStore) writeRecord(w io.Writer, recType recordType, rec record) error {
	var err error
	totalSize := 0

	// If we are using the buffer writer on this call, and the buffer is
	// not already at the max size...
	if w == ss.bw && ss.bufSize != ss.opts.BufferSize {
		// Check if record fits
		if rec.Size()+recordHeaderSize > ss.bw.Available() {
			// If there was a request to shrink the buffer, cancel that.
			ss.shrinkReq = false
			// If there was something, flush first
			if ss.bw.Buffered() > 0 {
				if err := ss.bw.Flush(); err != nil {
					return err
				}
			}
			// Double the size, but cap it.
			ss.bufSize *= 2
			if ss.bufSize > ss.opts.BufferSize {
				ss.bufSize = ss.opts.BufferSize
			}
			ss.bw = bufio.NewWriterSize(ss.file, ss.bufSize)
			ss.writer = ss.bw
			// `w` is used in this function, so point it to the new buffer
			w = ss.bw
		}
	}
	ss.tmpSubBuf, totalSize, err = writeRecord(w, ss.tmpSubBuf, recType, rec, ss.crcTable)
	if err != nil {
		return err
	}
	if ss.shrinkReq {
		percentFilled := ss.bw.Buffered() * 100 / ss.bufSize
		// If above the threshold, cancel the request.
		if percentFilled > bufShrinkThreshold {
			ss.shrinkReq = false
		}
	}
	// Indicate that we wrote something to the buffer/file
	ss.activity = true
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
	// Skip this if nothing was written since the last flush
	if !ss.activity {
		return nil
	}
	// Reset this now
	ss.activity = false
	if ss.bw != nil {
		if err := ss.bw.Flush(); err != nil {
			return err
		}
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
	ss.Lock()
	if ss.closed {
		ss.Unlock()
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
	if ss.shrinkTimer != nil {
		if ss.shrinkTimer.Stop() {
			// If we can stop, timer callback won't fire,
			// so we need to decrement the wait group.
			ss.allDone.Done()
		}
	}
	ss.Unlock()

	// Wait on timers/callbacks
	ss.allDone.Wait()

	return err
}
