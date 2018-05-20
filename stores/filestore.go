// Copyright 2016-2018 The NATS Authors
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
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

const (
	// Our file version.
	fileVersion = 1

	// Prefix for message log files
	msgFilesPrefix = "msgs."

	// Data files suffix
	datSuffix = ".dat"

	// Index files suffix
	idxSuffix = ".idx"

	// Backup file suffix
	bakSuffix = ".bak"

	// Name of the subscriptions file.
	subsFileName = "subs" + datSuffix

	// Name of the clients file.
	clientsFileName = "clients" + datSuffix

	// Name of the server file.
	serverFileName = "server" + datSuffix

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

	// msgRecordOverhead is the number of bytes to count toward the size
	// of a serialized message so that file slice size is closer to
	// channels and/or file slice limits.
	msgRecordOverhead = recordHeaderSize + msgIndexRecSize

	// Percentage of buffer usage to decide if the buffer should shrink
	bufShrinkThreshold = 50

	// Interval when to check/try to shrink buffer writers
	defaultBufShrinkInterval = 5 * time.Second

	// Interval an unused file slice is left opened
	defaultSliceCloseInterval = time.Second

	// If FileStoreOption's BufferSize is > 0, the buffer writer is initially
	// created with this size (unless this is > than BufferSize, in which case
	// BufferSize is used). When possible, the buffer will shrink but not lower
	// than this value. This is for FileSubStore's
	subBufMinShrinkSize = 128

	// If FileStoreOption's BufferSize is > 0, the buffer writer is initially
	// created with this size (unless this is > than BufferSize, in which case
	// BufferSize is used). When possible, the buffer will shrink but not lower
	// than this value. This is for FileMsgStore's
	msgBufMinShrinkSize = 512

	// This is the sleep time in the background tasks go routine.
	defaultBkgTasksSleepDuration = time.Second

	// This is the default amount of time a message is cached.
	defaultCacheTTL = time.Second

	// defaultFileFlags are the default file flags used when opening a file
	defaultFileFlags = os.O_RDWR | os.O_CREATE | os.O_APPEND

	// Lock file name
	lockFileName = ".rootdir.lck"

	// Witness file for TruncateUnexpectedEOF option
	truncateBadEOFFileName = ".truncate.lck"
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

	// Regardless of channel limits, the options below allow to split a message
	// log in smaller file chunks. If all those options were to be set to 0,
	// some file slice limit will be selected automatically based on the channel
	// limits.
	// SliceMaxMsgs defines how many messages can fit in a file slice (0 means
	// count is not checked).
	SliceMaxMsgs int
	// SliceMaxBytes defines how many bytes can fit in a file slice, including
	// the corresponding index file (0 means size is not checked).
	SliceMaxBytes int64
	// SliceMaxAge defines the period of time covered by a slice starting when
	// the first message is stored (0 means time is not checked).
	SliceMaxAge time.Duration
	// SliceArchiveScript is the path to a script to be invoked when a file
	// slice (and the corresponding index file) is going to be removed.
	// The script will be invoked with the channel name and names of data and
	// index files (which both have been previously renamed with a '.bak'
	// extension). It is the responsibility of the script to move/remove
	// those files.
	SliceArchiveScript string

	// FileDescriptorsLimit is a soft limit hinting at FileStore to try to
	// limit the number of concurrent opened files to that limit.
	FileDescriptorsLimit int64

	// Number of channels recovered in parallel (default is 1).
	ParallelRecovery int

	// TruncateUnexpectedEOF is set to true means that if recovery reports
	// an error about unexpected end of file, the last bad record will be
	// removed (the file is truncated at the beginning of the first incomplete
	// record). Dataloss may occur.
	TruncateUnexpectedEOF bool
}

// This is an internal error to detect situations where we do
// not get an EOF but all data we read are zeros. The file
// will be rewind to previous position and use this as the
// first write position.
var errNeedRewind = errors.New("end of file padded with zeros")

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
	SliceMaxBytes:        64 * 1024 * 1024, // 64MB
	ParallelRecovery:     1,
}

// BufferSize is a FileStore option that sets the size of the buffer used
// during store writes. This can help improve write performance.
func BufferSize(size int) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if size < 0 {
			return fmt.Errorf("buffer size value must be a positive number")
		}
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
		if seconds <= 0 {
			return fmt.Errorf("compact interval value must at least be 1 seconds")
		}
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
		if fragmentation <= 0 {
			return fmt.Errorf("compact fragmentation value must at least be 1")
		}
		o.CompactFragmentation = fragmentation
		return nil
	}
}

// CompactMinFileSize is a FileStore option that defines the minimum file size below
// which compaction would not occur. Specify `0` if you don't want any minimum.
func CompactMinFileSize(fileSize int64) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if fileSize < 0 {
			return fmt.Errorf("compact minimum file size value must be a positive number")
		}
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
		if polynomial <= 0 || polynomial > int64(0xFFFFFFFF) {
			return fmt.Errorf("crc polynomial should be between 1 and %v", int64(0xFFFFFFFF))
		}
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

// SliceConfig is a FileStore option that allows the configuration of
// file slice limits and optional archive script file name.
func SliceConfig(maxMsgs int, maxBytes int64, maxAge time.Duration, script string) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if maxMsgs < 0 || maxBytes < 0 || maxAge < 0 {
			return fmt.Errorf("slice max values must be positive numbers")
		}
		o.SliceMaxMsgs = maxMsgs
		o.SliceMaxBytes = maxBytes
		o.SliceMaxAge = maxAge
		o.SliceArchiveScript = script
		return nil
	}
}

// FileDescriptorsLimit is a soft limit hinting at FileStore to try to
// limit the number of concurrent opened files to that limit.
func FileDescriptorsLimit(limit int64) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if limit < 0 {
			return fmt.Errorf("file descriptor limit must be a positive number")
		}
		o.FileDescriptorsLimit = limit
		return nil
	}
}

// ParallelRecovery is a FileStore option that allows the parallel
// recovery of channels. When running with SSDs, try to use a higher
// value than the default number of 1. When running with HDDs,
// performance may be better if it stays at 1.
func ParallelRecovery(count int) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if count <= 0 {
			return fmt.Errorf("parallel recovery value must be at least 1")
		}
		o.ParallelRecovery = count
		return nil
	}
}

// TruncateUnexpectedEOF indicates if on recovery the store should
// truncate a file that reports an unexpected end-of-file (EOF) on recovery.
// If set to true, the invalid record byte content is printed but the store
// will truncate the file prior to this bad record and proceed with recovery.
// Dataloss may occur.
func TruncateUnexpectedEOF(truncate bool) FileStoreOption {
	return func(o *FileStoreOptions) error {
		o.TruncateUnexpectedEOF = truncate
		return nil
	}
}

// AllOptions is a convenient option to pass all options from a FileStoreOptions
// structure to the constructor.
func AllOptions(opts *FileStoreOptions) FileStoreOption {
	return func(o *FileStoreOptions) error {
		if err := BufferSize(opts.BufferSize)(o); err != nil {
			return err
		}
		if err := CompactInterval(opts.CompactInterval)(o); err != nil {
			return err
		}
		if err := CompactFragmentation(opts.CompactFragmentation)(o); err != nil {
			return err
		}
		if err := CompactMinFileSize(opts.CompactMinFileSize)(o); err != nil {
			return err
		}
		if err := CRCPolynomial(opts.CRCPolynomial)(o); err != nil {
			return err
		}
		if err := SliceConfig(opts.SliceMaxMsgs, opts.SliceMaxBytes, opts.SliceMaxAge, opts.SliceArchiveScript)(o); err != nil {
			return err
		}
		if err := FileDescriptorsLimit(opts.FileDescriptorsLimit)(o); err != nil {
			return err
		}
		if err := ParallelRecovery(opts.ParallelRecovery)(o); err != nil {
			return err
		}
		o.CompactEnabled = opts.CompactEnabled
		o.DoCRC = opts.DoCRC
		o.DoSync = opts.DoSync
		o.TruncateUnexpectedEOF = opts.TruncateUnexpectedEOF
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

type fileID int64

type beforeFileClose func() error

const (
	invalidFileID fileID = -1

	fileOpened  = int32(1)
	fileInUse   = int32(2)
	fileClosing = int32(3)
	fileClosed  = int32(4)
	fileRemoved = int32(5)
	fmClosed    = int32(6)
)

type file struct {
	// Atomic need to be memory aligned. Put them first in the
	// structure definition.
	state int32

	id          fileID
	handle      *os.File
	name        string
	flags       int
	beforeClose beforeFileClose
}

type filesManager struct {
	sync.Mutex
	openedFDs int64
	limit     int64
	rootDir   string
	files     map[fileID]*file
	nextID    fileID
	isClosed  bool
}

// FileStore is the storage interface for STAN servers, backed by files.
type FileStore struct {
	genericStore
	fm            *filesManager
	serverFile    *file
	clientsFile   *file
	opts          FileStoreOptions
	compactItvl   time.Duration
	clients       map[string]*Client
	delClientRec  spb.ClientDelete
	cliFileSize   int64
	cliDeleteRecs int // Number of deleted client records
	cliCompactTS  time.Time
	crcTable      *crc32.Table
	lockFile      util.LockFile
}

type subscription struct {
	sub    *spb.SubState
	seqnos map[uint64]struct{}
}

type bufferedWriter struct {
	buf           *bufio.Writer
	bufSize       int  // current buffer size
	minShrinkSize int  // minimum shrink size. Note that this can be bigger than maxSize (see setSizes)
	maxSize       int  // maximum size the buffer can grow
	shrinkReq     bool // used to decide if buffer should shrink
}

// FileSubStore is a subscription store in files.
type FileSubStore struct {
	genericSubStore
	fstore      *FileStore
	fm          *filesManager
	tmpSubBuf   []byte
	file        *file
	bw          *bufferedWriter
	delSub      spb.SubStateDelete
	updateSub   spb.SubStateUpdate
	opts        *FileStoreOptions // points to options from FileStore
	compactItvl time.Duration
	fileSize    int64
	numRecs     int // Number of records (sub and msgs)
	delRecs     int // Number of delete (or ack) records
	compactTS   time.Time
	crcTable    *crc32.Table // reference to the one from FileStore
	activity    bool         // was there any write between two flush calls
	writer      io.Writer    // this is either `bw` or `file` depending if buffer writer is used or not
	shrinkTimer *time.Timer  // timer associated with callback shrinking buffer when possible
	allDone     sync.WaitGroup
}

// fileSlice represents one of the message store file (there are a number
// of files for a MsgStore on a given channel).
type fileSlice struct {
	file       *file
	idxFile    *file
	firstSeq   uint64
	lastSeq    uint64
	rmCount    int // Count of messages "removed" from the slice due to limits.
	msgsCount  int
	msgsSize   uint64
	firstWrite int64 // Time the first message was added to this slice (used for slice age limit)
	lastUsed   int64
}

// msgIndex contains the message's offset in the data file, its timestamp
// and size, which allows quick recovery of message and reconstructing of
// file slices. It also helps for GetSequenceFromTimestamp by not having
// to recover actual messages to find out the correct message sequence
// based on timestamp.
type msgIndex struct {
	offset    int64
	timestamp int64
	msgSize   uint32
}

// bufferedMsg is required to keep track of a message and msgRecord when
// file buffering is used. It is possible that a message and index is
// not flushed on disk while the message gets removed from the store
// due to limit. We need a map that keeps a reference to message and
// record until the file is flushed.
type bufferedMsg struct {
	msg   *pb.MsgProto
	index *msgIndex
}

// cachedMsg is a structure that contains a reference to a message
// and cache expiration value. The cache has a map and list so
// that cached messages can be ordered by expiration time.
type cachedMsg struct {
	expiration int64
	msg        *pb.MsgProto
	prev       *cachedMsg
	next       *cachedMsg
}

// msgsCache is the file store cache.
type msgsCache struct {
	tryEvict int32
	seqMaps  map[uint64]*cachedMsg
	head     *cachedMsg
	tail     *cachedMsg
}

// FileMsgStore is a per channel message file store.
type FileMsgStore struct {
	genericMsgStore
	// Atomic operations require 64bit aligned fields to be able
	// to run with 32bit processes.
	checkSlices int64 // used with atomic operations
	timeTick    int64 // time captured in background tasks go routine

	tmpMsgBuf    []byte
	fm           *filesManager // shortcut to ms.fstore.fm
	hasFDsLimit  bool          // shortcut to ms.fstore.opts.FileDescriptorsLimit > 0
	bw           *bufferedWriter
	writer       io.Writer // this is `bw.buf` or `file` depending if buffer writer is used or not
	files        map[int]*fileSlice
	writeSlice   *fileSlice
	channelName  string
	firstFSlSeq  int // First file slice sequence number
	lastFSlSeq   int // Last file slice sequence number
	slCountLim   int
	slSizeLim    uint64
	slAgeLim     int64
	slHasLimits  bool
	fstore       *FileStore // pointer to file store object
	cache        *msgsCache
	wOffset      int64
	firstMsg     *pb.MsgProto
	lastMsg      *pb.MsgProto
	expiration   int64
	bufferedSeqs []uint64
	bufferedMsgs map[uint64]*bufferedMsg
	bkgTasksDone chan bool // signal the background tasks go routine to stop
	bkgTasksWake chan bool // signal the background tasks go routine to get out of a sleep
	allDone      sync.WaitGroup
}

// some variables based on constants but that we can change
// for tests puposes.
var (
	bufShrinkInterval     = defaultBufShrinkInterval
	bkgTaskMu             sync.Mutex
	bkgTaskRefs           int
	bkgTasksSleepDuration = defaultBkgTasksSleepDuration
	cacheTTL              = int64(defaultCacheTTL)
	sliceCloseInterval    = defaultSliceCloseInterval
)

// FileStoreTestSetBackgroundTaskInterval is used by tests to reduce the interval
// at which some tasks are performed in the background
func FileStoreTestSetBackgroundTaskInterval(wait time.Duration) {
	// It is possible that both the server test package and
	// stores test package run in paraller. Ensure that only
	// one is setting the value to avoid races.
	bkgTaskMu.Lock()
	if bkgTaskRefs == 0 {
		bkgTasksSleepDuration = wait
	}
	bkgTaskRefs++
	bkgTaskMu.Unlock()
}

// openFile opens the file specified by `filename`.
// If the file exists, it checks that the version is supported.
// The file is created if not present, opened in Read/Write and Append mode.
var openFile = func(fileName string) (*os.File, error) {
	return openFileWithFlags(fileName, defaultFileFlags)
}

// openFileWithModes opens the file specified by `filename`, using
// the `modes` as open flags.
// If the file exists, it checks that the version is supported.
// If no open mode override is provided, the file is created if not present,
// opened in Read/Write and Append mode.
func openFileWithFlags(fileName string, flags int) (*os.File, error) {
	checkVersion := false

	// Check if file already exists
	if s, err := os.Stat(fileName); s != nil && err == nil {
		checkVersion = true
	}
	file, err := os.OpenFile(fileName, flags, 0666)
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
func writeRecord(w io.Writer, buf []byte, recType recordType, rec record, recSize int, crcTable *crc32.Table) ([]byte, int, error) {
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
	if recSize == 0 && recType == 0 {
		crc := util.ByteOrder.Uint32(header[4:recordHeaderSize])
		if crc == 0 {
			return buf, 0, 0, errNeedRewind
		}
	}
	// Now we are going to read the payload
	buf = util.EnsureBufBigEnough(buf, recSize)
	if _, err := io.ReadFull(r, buf[:recSize]); err != nil {
		return buf, 0, recNoType, err
	}
	if checkCRC {
		crc := util.ByteOrder.Uint32(header[4:recordHeaderSize])
		// check CRC against what was stored
		if c := crc32.Checksum(buf[:recSize], crcTable); c != crc {
			return buf, 0, recNoType, fmt.Errorf("corrupted data, expected crc to be 0x%08x, got 0x%08x", crc, c)
		}
	}
	return buf, recSize, recType, nil
}

// setSize sets the initial buffer size and keep track of min/max allowed sizes
func newBufferWriter(minShrinkSize, maxSize int) *bufferedWriter {
	w := &bufferedWriter{minShrinkSize: minShrinkSize, maxSize: maxSize}
	w.bufSize = minShrinkSize
	// The minSize is the minimum size the buffer can shrink to.
	// However, if the given max size is smaller than the min
	// shrink size, use that instead.
	if maxSize < minShrinkSize {
		w.bufSize = maxSize
	}
	return w
}

// createNewWriter creates a new buffer writer for `file` with
// the bufferedWriter's current buffer size.
func (w *bufferedWriter) createNewWriter(file *os.File) io.Writer {
	w.buf = bufio.NewWriterSize(file, w.bufSize)
	return w.buf
}

// expand the buffer (first flushing the buffer if not empty)
func (w *bufferedWriter) expand(file *os.File, required int) (io.Writer, error) {
	// If there was a request to shrink the buffer, cancel that.
	w.shrinkReq = false
	// If there was something, flush first
	if w.buf.Buffered() > 0 {
		if err := w.buf.Flush(); err != nil {
			return w.buf, err
		}
	}
	// Double the size
	w.bufSize *= 2
	// If still smaller than what is required, adjust
	if w.bufSize < required {
		w.bufSize = required
	}
	// But cap it.
	if w.bufSize > w.maxSize {
		w.bufSize = w.maxSize
	}
	w.buf = bufio.NewWriterSize(file, w.bufSize)
	return w.buf, nil
}

// tryShrinkBuffer checks and possibly shrinks the buffer
func (w *bufferedWriter) tryShrinkBuffer(file *os.File) (io.Writer, error) {
	// Nothing to do if we are already at the lowest
	// or file not set/opened.
	if w.bufSize == w.minShrinkSize || file == nil {
		return w.buf, nil
	}

	if !w.shrinkReq {
		percentFilled := w.buf.Buffered() * 100 / w.bufSize
		if percentFilled <= bufShrinkThreshold {
			w.shrinkReq = true
		}
		// Wait for next tick to see if we can shrink
		return w.buf, nil
	}
	if err := w.buf.Flush(); err != nil {
		return w.buf, err
	}
	// Reduce size, but ensure it does not go below the limit
	w.bufSize /= 2
	if w.bufSize < w.minShrinkSize {
		w.bufSize = w.minShrinkSize
	}
	w.buf = bufio.NewWriterSize(file, w.bufSize)
	// Don't reset shrinkReq unless we are down to the limit
	if w.bufSize == w.minShrinkSize {
		w.shrinkReq = true
	}
	return w.buf, nil
}

// checkShrinkRequest checks how full the buffer is, and if is above a certain
// threshold, cancels the shrink request
func (w *bufferedWriter) checkShrinkRequest() {
	percentFilled := w.buf.Buffered() * 100 / w.bufSize
	// If above the threshold, cancel the request.
	if percentFilled > bufShrinkThreshold {
		w.shrinkReq = false
	}
}

////////////////////////////////////////////////////////////////////////////
// filesManager methods
////////////////////////////////////////////////////////////////////////////

// createFilesManager returns an instance of the files manager.
func createFilesManager(rootDir string, openedFilesLimit int64) *filesManager {
	fm := &filesManager{
		rootDir: rootDir,
		limit:   openedFilesLimit,
		files:   make(map[fileID]*file),
	}
	return fm
}

// closeUnusedFiles cloes files that are opened and not currently in-use.
// Since the number of opened files is a soft limit, and if this function
// is unable to close any file, the caller will still attempt to create/open
// the requested file. If the system's file descriptor limit is reached,
// opening the file will fail and that error will be returned to the caller.
// Lock is required on entry.
func (fm *filesManager) closeUnusedFiles(idToSkip fileID) {
	for _, file := range fm.files {
		if file.id == idToSkip {
			continue
		}
		if atomic.CompareAndSwapInt32(&file.state, fileOpened, fileClosing) {
			fm.doClose(file)
			if fm.openedFDs < fm.limit {
				break
			}
		}
	}
}

// createFile creates a file, open it, adds it to the list of files and returns
// an instance of `*file` with the state sets to `fileInUse`.
// This call will possibly cause opened but unused files to be closed if the
// number of open file requests is above the set limit.
func (fm *filesManager) createFile(name string, flags int, bfc beforeFileClose) (*file, error) {
	fm.Lock()
	if fm.isClosed {
		fm.Unlock()
		return nil, fmt.Errorf("unable to create file %q, store is being closed", name)
	}
	if fm.limit > 0 && fm.openedFDs >= fm.limit {
		fm.closeUnusedFiles(0)
	}
	fileName := filepath.Join(fm.rootDir, name)
	handle, err := openFileWithFlags(fileName, flags)
	if err != nil {
		fm.Unlock()
		return nil, err
	}
	fm.nextID++
	newFile := &file{
		state:       fileInUse,
		id:          fm.nextID,
		handle:      handle,
		name:        fileName,
		flags:       flags,
		beforeClose: bfc,
	}
	fm.files[newFile.id] = newFile
	fm.openedFDs++
	fm.Unlock()
	return newFile, nil
}

// openFile opens the given file and sets its state to `fileInUse`.
// If the file manager has been closed or the file removed, this call
// returns an error.
// Otherwise, if the file's state is not `fileClosed` this call will panic.
// This call will possibly cause opened but unused files to be closed if the
// number of open file requests is above the set limit.
func (fm *filesManager) openFile(file *file) error {
	fm.Lock()
	if fm.isClosed {
		fm.Unlock()
		return fmt.Errorf("unable to open file %q, store is being closed", file.name)
	}
	curState := atomic.LoadInt32(&file.state)
	if curState == fileRemoved {
		fm.Unlock()
		return fmt.Errorf("unable to open file %q, it has been removed", file.name)
	}
	if curState != fileClosed || file.handle != nil {
		fm.Unlock()
		panic(fmt.Errorf("request to open file %q but invalid state: handle=%v - state=%v", file.name, file.handle, file.state))
	}
	var err error
	if fm.limit > 0 && fm.openedFDs >= fm.limit {
		fm.closeUnusedFiles(file.id)
	}
	file.handle, err = openFileWithFlags(file.name, file.flags)
	if err == nil {
		atomic.StoreInt32(&file.state, fileInUse)
		fm.openedFDs++
	}
	fm.Unlock()
	return err
}

// closeLockedFile closes the handle of the given file, but only if the caller
// has locked the file. Will panic otherwise.
// If the file's beforeClose callback is not nil, this callback is invoked
// before the file handle is closed.
func (fm *filesManager) closeLockedFile(file *file) error {
	if !atomic.CompareAndSwapInt32(&file.state, fileInUse, fileClosing) {
		panic(fmt.Errorf("file %q is requested to be closed but was not locked by caller", file.name))
	}
	fm.Lock()
	err := fm.doClose(file)
	fm.Unlock()
	return err
}

// closeFileIfOpened closes the handle of the given file, but only if the
// file is opened and not currently locked. Does not return any error or panic
// if file is in any other state.
// If the file's beforeClose callback is not nil, this callback is invoked
// before the file handle is closed.
func (fm *filesManager) closeFileIfOpened(file *file) error {
	if !atomic.CompareAndSwapInt32(&file.state, fileOpened, fileClosing) {
		return nil
	}
	fm.Lock()
	err := fm.doClose(file)
	fm.Unlock()
	return err
}

// closeLockedOrOpenedFile closes the handle of the given file if this file
// is either locked or opened. Does not return any error or panic if file
// is in any other state.
// If the file's beforeClose callback is not nil, this callback is invoked
// before the file handle is closed.
func (fm *filesManager) closeLockedOrOpenedFile(file *file) error {
	// Check first locked files
	if !atomic.CompareAndSwapInt32(&file.state, fileInUse, fileClosing) {
		// then opened but unlocked files
		if !atomic.CompareAndSwapInt32(&file.state, fileOpened, fileClosing) {
			return nil
		}
	}
	fm.Lock()
	err := fm.doClose(file)
	fm.Unlock()
	return err
}

// doClose closes the file handle, setting it to nil and switching state to `fileClosed`.
// If a `beforeClose` callback was registered on file creation, it is invoked
// before the file handler is actually closed.
// Lock is required on entry.
func (fm *filesManager) doClose(file *file) error {
	var err error
	if file.beforeClose != nil {
		err = file.beforeClose()
	}
	util.CloseFile(err, file.handle)
	// Regardless of error, we need to change the state to closed.
	file.handle = nil
	atomic.StoreInt32(&file.state, fileClosed)
	fm.openedFDs--
	return err
}

// lockFile locks the given file.
// If the file was already opened, the boolean returned is true,
// otherwise, the file is opened and the call returns false.
func (fm *filesManager) lockFile(file *file) (bool, error) {
	if atomic.CompareAndSwapInt32(&file.state, fileOpened, fileInUse) {
		return true, nil
	}
	return false, fm.openFile(file)
}

// lockFileIfOpened is like lockFile but returns true only if the
// file is already opened, false otherwise (and the file remain closed).
func (fm *filesManager) lockFileIfOpened(file *file) bool {
	return atomic.CompareAndSwapInt32(&file.state, fileOpened, fileInUse)
}

// unlockFile unlocks the file if currently locked, otherwise panic.
func (fm *filesManager) unlockFile(file *file) {
	if !atomic.CompareAndSwapInt32(&file.state, fileInUse, fileOpened) {
		panic(fmt.Errorf("failed to switch state from fileInUse to fileOpened for file %q, state=%v",
			file.name, file.state))
	}
}

// trySwitchState attempts to switch an initial state of `fileOpened`
// or `fileClosed` to the given newState. If it can't it will return an
// error, otherwise, returned a boolean to indicate if the initial state
// was `fileOpened`.
func (fm *filesManager) trySwitchState(file *file, newState int32) (bool, error) {
	wasOpened := false
	wasClosed := false
	for i := 0; i < 10000; i++ {
		if atomic.CompareAndSwapInt32(&file.state, fileOpened, newState) {
			wasOpened = true
			break
		}
		if atomic.CompareAndSwapInt32(&file.state, fileClosed, newState) {
			wasClosed = true
			break
		}
		if i%1000 == 1 {
			time.Sleep(time.Millisecond)
		}
	}
	if !wasOpened && !wasClosed {
		return false, fmt.Errorf("file %q is still probably locked", file.name)
	}
	return wasOpened, nil
}

// remove a file from the list of files. The initial state must be either `fileOpened`
// or `fileClosed`. This call will loop until it can switch the file's state from
// one of these states to `fileRemoved`, or return an error if the change can't
// be made after a certain number of attempts.
// When removed, this call returns true and the given `file` is untouched (except
// for its state). So it is still possible for caller to read/write (if handle is
// valid) or close this file.
func (fm *filesManager) remove(file *file) bool {
	fm.Lock()
	wasOpened, err := fm.trySwitchState(file, fileRemoved)
	if err != nil {
		fm.Unlock()
		return false
	}
	// With code above, we can't be removing a file twice, so no need to check if
	// file is present in map.
	delete(fm.files, file.id)
	if wasOpened {
		fm.openedFDs--
	}
	fm.Unlock()
	return true
}

// setBeforeCloseCb sets the beforeFileClose callback for this file.
// When this callback is set, and the files manager closes a file,
// the callback is invoked prior to actual closing of the file handle.
// This allows the caller to perfom some work before the file is
// asynchronously (form its perspective) closed.
func (fm *filesManager) setBeforeCloseCb(file *file, bccb beforeFileClose) {
	fm.Lock()
	file.beforeClose = bccb
	fm.Unlock()
}

// truncateFile truncates the file to the given offset.
// The file is assumed to be locked on entry.
// If the file's flags indicate that this file is opened with O_APPEND, it
// is first closed, reopened in non append mode, truncated, then reopened
// (and locked) with original flags.
func (fm *filesManager) truncateFile(file *file, offset int64) error {
	reopen := false
	fd := file.handle
	if file.flags&os.O_APPEND != 0 {
		if err := fm.closeLockedFile(file); err != nil {
			return err
		}
		var err error
		fd, err = openFileWithFlags(file.name, os.O_RDWR)
		if err != nil {
			return err
		}
		reopen = true
	}
	newPos := offset
	if err := fd.Truncate(newPos); err != nil {
		return err
	}
	pos, err := fd.Seek(newPos, io.SeekStart) // or Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if pos != newPos {
		return fmt.Errorf("unable to set position of file %q to %v", file.name, newPos)
	}
	if reopen {
		if err := fd.Close(); err != nil {
			return err
		}
		if err := fm.openFile(file); err != nil {
			return err
		}
	}
	return nil
}

// close the files manager, including all files currently opened.
// Returns the first error encountered when closing the files.
func (fm *filesManager) close() error {
	fm.Lock()
	if fm.isClosed {
		fm.Unlock()
		return nil
	}
	fm.isClosed = true

	files := make([]*file, 0, len(fm.files))
	for _, file := range fm.files {
		files = append(files, file)
	}
	fm.files = nil
	fm.Unlock()

	var err error
	for _, file := range files {
		wasOpened, sserr := fm.trySwitchState(file, fmClosed)
		if sserr != nil {
			if err == nil {
				err = sserr
			}
		} else if wasOpened {
			fm.Lock()
			if cerr := fm.doClose(file); cerr != nil && err == nil {
				err = cerr
			}
			fm.Unlock()
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileStore methods
////////////////////////////////////////////////////////////////////////////

// NewFileStore returns a factory for stores backed by files.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewFileStore(log logger.Logger, rootDir string, limits *StoreLimits, options ...FileStoreOption) (*FileStore, error) {
	if rootDir == "" {
		return nil, fmt.Errorf("for %v stores, root directory must be specified", TypeFile)
	}

	fs := &FileStore{opts: DefaultFileStoreOptions, clients: make(map[string]*Client)}
	if err := fs.init(TypeFile, log, limits); err != nil {
		return nil, err
	}

	for _, opt := range options {
		if err := opt(&fs.opts); err != nil {
			return nil, err
		}
	}
	// Create filesManager based on options' FD limit
	fs.fm = createFilesManager(rootDir, fs.opts.FileDescriptorsLimit)
	// Convert the compact interval in time.Duration
	fs.compactItvl = time.Duration(fs.opts.CompactInterval) * time.Second
	// Create the table using polynomial in options
	if fs.opts.CRCPolynomial == int64(crc32.IEEE) {
		fs.crcTable = crc32.IEEETable
	} else {
		fs.crcTable = crc32.MakeTable(uint32(fs.opts.CRCPolynomial))
	}

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("unable to create the root directory [%s]: %v", rootDir, err)
	}

	// If the TruncateUnexpectedEOF is set, check that the witness
	// file is not present. If it is, fail starting. If it isn't,
	// create the witness file.
	truncateFName := filepath.Join(rootDir, truncateBadEOFFileName)
	if fs.opts.TruncateUnexpectedEOF {
		// Try to create the file, if it exists, this is an error.
		f, err := os.OpenFile(truncateFName, os.O_CREATE|os.O_EXCL, 0666)
		if f != nil {
			f.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("file store should not be opened consecutively with the TruncateUnexpectedEOF option set to true")
		}
	} else {
		// Delete possible TruncateUnexpectedEOF witness file
		os.Remove(truncateFName)
	}

	return fs, nil
}

type channelRecoveryCtx struct {
	wg        *sync.WaitGroup
	poolCh    chan struct{}
	errCh     chan error
	recoverCh chan *recoveredChannel
}

type recoveredChannel struct {
	name string
	rc   *RecoveredChannel
}

// Recover implements the Store interface
func (fs *FileStore) Recover() (*RecoveredState, error) {
	fs.Lock()
	defer fs.Unlock()
	var (
		err               error
		recoveredState    *RecoveredState
		serverInfo        *spb.ServerInfo
		recoveredClients  []*Client
		recoveredChannels = make(map[string]*RecoveredChannel)
		channels          []os.FileInfo
	)

	// Ensure store is closed in case of return with error
	defer func() {
		if fs.serverFile != nil {
			fs.fm.unlockFile(fs.serverFile)
		}
		if fs.clientsFile != nil {
			fs.fm.unlockFile(fs.clientsFile)
		}
	}()

	// Open/Create the server file (note that this file must not be opened,
	// in APPEND mode to allow truncate to work).
	fs.serverFile, err = fs.fm.createFile(serverFileName, os.O_RDWR|os.O_CREATE, nil)
	if err != nil {
		return nil, err
	}

	// Open/Create the client file.
	fs.clientsFile, err = fs.fm.createFile(clientsFileName, defaultFileFlags, nil)
	if err != nil {
		return nil, err
	}

	// Recover the server file.
	serverInfo, err = fs.recoverServerInfo()
	if err != nil {
		return nil, fmt.Errorf("unable to recover server file %q: %v", fs.serverFile.name, err)
	}
	// If the server file is empty, then we are done
	if serverInfo == nil {
		// We return the file store instance, but no recovered state.
		return nil, nil
	}

	// Recover the clients file
	recoveredClients, err = fs.recoverClients()
	if err != nil {
		return nil, fmt.Errorf("unable to recover client file %q: %v", fs.clientsFile.name, err)
	}

	// Get the channels (there are subdirectories of rootDir)
	channels, err = ioutil.ReadDir(fs.fm.rootDir)
	if err != nil {
		return nil, err
	}
	if len(channels) > 0 {
		wg, poolCh, errCh, recoverCh := initParalleRecovery(fs.opts.ParallelRecovery, len(channels))
		ctx := &channelRecoveryCtx{wg: wg, poolCh: poolCh, errCh: errCh, recoverCh: recoverCh}
		for _, c := range channels {
			// Channels are directories. Ignore simple files
			if !c.IsDir() {
				continue
			}
			channel := c.Name()
			channelDirName := filepath.Join(fs.fm.rootDir, channel)
			limits := fs.genericStore.getChannelLimits(channel)
			// This will block if the max number of go-routines is reached.
			// When one of the go-routine finishes, it will add back to the
			// pool and we will be able to start the recovery of another
			// channel.
			<-poolCh
			wg.Add(1)
			go fs.recoverOneChannel(channelDirName, channel, limits, ctx)
			// Fail as soon as we detect that a go routine has encountered
			// an error
			if len(errCh) > 0 {
				break
			}
		}
		// We need to wait for all current go routines to exit
		wg.Wait()
		// Also, even if there was an error, we need to collect
		// all channels that were recovered so that we can close
		// the msgs/subs stores on exit.
		done := false
		for !done {
			select {
			case rc := <-recoverCh:
				recoveredChannels[rc.name] = rc.rc
				fs.channels[rc.name] = rc.rc.Channel
			default:
				done = true
			}
		}
		select {
		case err = <-errCh:
			return nil, err
		default:
		}
	}
	// Create the recovered state to return
	recoveredState = &RecoveredState{
		Info:     serverInfo,
		Clients:  recoveredClients,
		Channels: recoveredChannels,
	}
	return recoveredState, nil
}

func initParalleRecovery(maxGoRoutines, foundChannels int) (*sync.WaitGroup, chan struct{}, chan error, chan *recoveredChannel) {
	wg := sync.WaitGroup{}
	poolCh := make(chan struct{}, maxGoRoutines)
	for i := 0; i < maxGoRoutines; i++ {
		poolCh <- struct{}{}
	}
	errCh := make(chan error, 1)
	// foundChannels is the number of directories (channels) found
	// in the root directory. It is the max number of elements we will
	// put in this channel during the recovery process.
	recoverCh := make(chan *recoveredChannel, foundChannels)
	return &wg, poolCh, errCh, recoverCh
}

func (fs *FileStore) recoverOneChannel(dir, name string, limits *ChannelLimits, ctx *channelRecoveryCtx) {
	var (
		msgStore *FileMsgStore
		subStore *FileSubStore
		err      error
	)
	defer func() {
		if err != nil {
			select {
			case ctx.errCh <- err:
			default:
			}
		}
		ctx.poolCh <- struct{}{}
		ctx.wg.Done()
	}()
	msgStore, err = fs.newFileMsgStore(dir, name, &limits.MsgStoreLimits, true)
	if err != nil {
		return
	}
	subStore, err = fs.newFileSubStore(name, &limits.SubStoreLimits, true)
	if err != nil {
		msgStore.Close()
		return
	}

	recoveredChannel := &recoveredChannel{
		name: name,
		rc: &RecoveredChannel{
			Channel: &Channel{
				Subs: subStore,
				Msgs: msgStore,
			},
			Subscriptions: make([]*RecoveredSubscription, 0, len(subStore.subs)),
		},
	}

	// Fill that array with what we got from newFileSubStore.
	for _, subi := range subStore.subs {
		sub := subi.(*subscription)
		// The server is making a copy of rss.Sub, still it is not
		// a good idea to return a pointer to an object that belong
		// to the store. So make a copy and return the pointer to
		// that copy.
		csub := *sub.sub
		rs := &RecoveredSubscription{
			Sub:     &csub,
			Pending: make(PendingAcks),
		}
		// If we recovered any seqno...
		if len(sub.seqnos) > 0 {
			// Lookup messages, and if we find those, update the
			// Pending map.
			for seq := range sub.seqnos {
				rs.Pending[seq] = struct{}{}
			}
		}
		// Add to the array of recovered subscriptions
		recoveredChannel.rc.Subscriptions = append(recoveredChannel.rc.Subscriptions, rs)
	}
	// Push our recovered info into the recovered channel.
	ctx.recoverCh <- recoveredChannel
}

// GetExclusiveLock implements the Store interface
func (fs *FileStore) GetExclusiveLock() (bool, error) {
	fs.Lock()
	defer fs.Unlock()
	if fs.lockFile != nil {
		return true, nil
	}
	f, err := util.CreateLockFile(filepath.Join(fs.fm.rootDir, lockFileName))
	if err != nil {
		if err == util.ErrUnableToLockNow {
			return false, nil
		}
		return false, err
	}
	// We must keep a reference to the file, otherwise, it `f` is GC'ed,
	// its file descriptor is closed, which automatically releases the lock.
	fs.lockFile = f
	return true, nil
}

// Init is used to persist server's information after the first start
func (fs *FileStore) Init(info *spb.ServerInfo) error {
	fs.Lock()
	defer fs.Unlock()

	if fs.serverFile == nil {
		var err error
		// Open/Create the server file (note that this file must not be opened,
		// in APPEND mode to allow truncate to work).
		fs.serverFile, err = fs.fm.createFile(serverFileName, os.O_RDWR|os.O_CREATE, nil)
		if err != nil {
			return err
		}
	} else {
		if _, err := fs.fm.lockFile(fs.serverFile); err != nil {
			return err
		}
	}
	f := fs.serverFile.handle
	// defer is ok for this function...
	defer fs.fm.unlockFile(fs.serverFile)

	// Truncate the file (4 is the size of the fileVersion record)
	if err := f.Truncate(4); err != nil {
		return err
	}
	// Move offset to 4 (truncate does not do that)
	if _, err := f.Seek(4, io.SeekStart); err != nil {
		return err
	}
	// ServerInfo record is not typed. We also don't pass a reusable buffer.
	if _, _, err := writeRecord(f, nil, recNoType, info, info.Size(), fs.crcTable); err != nil {
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
	offset := int64(4)

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(fs.clientsFile.handle, defaultBufSize)

	for {
		buf, recSize, recType, err = readRecord(br, buf, true, fs.crcTable, fs.opts.DoCRC)
		if err != nil {
			switch err {
			case io.EOF:
				err = nil
			case errNeedRewind:
				err = fs.fm.truncateFile(fs.clientsFile, offset)
			default:
				err = fs.handleUnexpectedEOF(err, fs.clientsFile, offset, true)
			}
			if err == nil {
				break
			}
			return nil, err
		}
		readBytes := int64(recSize + recordHeaderSize)
		offset += readBytes
		fs.cliFileSize += readBytes
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
	info := &spb.ServerInfo{}
	buf, size, _, err := readRecord(fs.serverFile.handle, nil, false, fs.crcTable, fs.opts.DoCRC)
	if err != nil {
		if err == io.EOF {
			// We are done, no state recovered
			return nil, nil
		}
		fs.log.Errorf("Server file %q corrupted: %v", fs.serverFile.name, err)
		fs.log.Errorf("Follow instructions in documentation in order to recover from this")
		return nil, err
	}
	// Check that the size of the file is consistent with the size
	// of the record we are supposed to recover. Account for the
	// 12 bytes (4 + recordHeaderSize) corresponding to the fileVersion and
	// record header.
	fstat, err := fs.serverFile.handle.Stat()
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

// CreateChannel implements the Store interface
func (fs *FileStore) CreateChannel(channel string) (*Channel, error) {
	fs.Lock()
	defer fs.Unlock()

	// Verify that it does not already exist or that we did not hit the limits
	if err := fs.canAddChannel(channel); err != nil {
		return nil, err
	}

	// We create the channel here...

	channelDirName := filepath.Join(fs.fm.rootDir, channel)
	if err := os.MkdirAll(channelDirName, os.ModeDir+os.ModePerm); err != nil {
		return nil, err
	}

	var err error
	var msgStore MsgStore
	var subStore SubStore

	channelLimits := fs.genericStore.getChannelLimits(channel)

	msgStore, err = fs.newFileMsgStore(channelDirName, channel, &channelLimits.MsgStoreLimits, false)
	if err != nil {
		return nil, err
	}
	subStore, err = fs.newFileSubStore(channel, &channelLimits.SubStoreLimits, false)
	if err != nil {
		msgStore.Close()
		return nil, err
	}

	c := &Channel{
		Subs: subStore,
		Msgs: msgStore,
	}

	fs.channels[channel] = c

	return c, nil
}

// DeleteChannel implements the Store interface
func (fs *FileStore) DeleteChannel(channel string) error {
	fs.Lock()
	defer fs.Unlock()
	err := fs.deleteChannel(channel)
	if err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(fs.fm.rootDir, channel))
}

// AddClient implements the Store interface
func (fs *FileStore) AddClient(info *spb.ClientInfo) (*Client, error) {
	fs.Lock()
	if _, err := fs.fm.lockFile(fs.clientsFile); err != nil {
		fs.Unlock()
		return nil, err
	}
	_, size, err := writeRecord(fs.clientsFile.handle, nil, addClient, info, info.Size(), fs.crcTable)
	if err != nil {
		fs.fm.unlockFile(fs.clientsFile)
		fs.Unlock()
		return nil, err
	}
	fs.cliFileSize += int64(size)
	fs.fm.unlockFile(fs.clientsFile)
	client := &Client{*info}
	fs.clients[client.ID] = client
	fs.Unlock()
	return client, nil
}

// DeleteClient implements the Store interface
func (fs *FileStore) DeleteClient(clientID string) error {
	fs.Lock()
	if _, err := fs.fm.lockFile(fs.clientsFile); err != nil {
		fs.Unlock()
		return err
	}
	fs.delClientRec = spb.ClientDelete{ID: clientID}
	_, size, err := writeRecord(fs.clientsFile.handle, nil, delClient, &fs.delClientRec, fs.delClientRec.Size(), fs.crcTable)
	// Even if there is an error, proceed. If we compact the file,
	// this may resolve the issue.
	delete(fs.clients, clientID)
	fs.cliDeleteRecs++
	fs.cliFileSize += int64(size)
	// Check if this triggers a need for compaction
	if fs.shouldCompactClientFile() {
		// close the file now
		// If we can't close the file, it does not make sense
		// to proceed with compaction.
		if lerr := fs.fm.closeLockedFile(fs.clientsFile); lerr != nil {
			fs.Unlock()
			return lerr
		}
		// compact (this uses a temporary file)
		// Override writeRecord error with the result of compaction.
		// If compaction works, the original error is no longer an issue
		// since the file has been replaced.
		err = fs.compactClientFile(fs.clientsFile.name)
	} else {
		fs.fm.unlockFile(fs.clientsFile)
	}
	fs.Unlock()
	return err
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
	if time.Since(fs.cliCompactTS) < fs.compactItvl {
		return false
	}
	return true
}

// Rewrite the content of the clients map into a temporary file,
// then swap back to active file.
// Store lock held on entry
func (fs *FileStore) compactClientFile(orgFileName string) error {
	// Open a temporary file
	tmpFile, err := getTempFile(fs.fm.rootDir, clientsFileName)
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
		buf, size, err = writeRecord(bw, buf, addClient, &c.ClientInfo, c.ClientInfo.Size(), fs.crcTable)
		if err != nil {
			return err
		}
		fileSize += int64(size)
	}
	// Flush the buffer on disk
	if err := bw.Flush(); err != nil {
		return err
	}
	// Start by closing the temporary file.
	if err := tmpFile.Close(); err != nil {
		return err
	}
	// Rename the tmp file to original file name
	if err := os.Rename(tmpFile.Name(), orgFileName); err != nil {
		return err
	}
	// Avoid unnecessary attempt to cleanup
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

// Close closes all stores.
func (fs *FileStore) Close() error {
	fs.Lock()
	if fs.closed {
		fs.Unlock()
		return nil
	}
	fs.closed = true

	err := fs.genericStore.close()

	fm := fs.fm
	lockFile := fs.lockFile
	fs.Unlock()

	if fm != nil {
		if fmerr := fm.close(); fmerr != nil && err == nil {
			err = fmerr
		}
	}
	if lockFile != nil {
		err = util.CloseFile(err, lockFile)
	}
	return err
}

func (fs *FileStore) handleUnexpectedEOF(recoveryErr error, f *file, offset int64, recTyped bool) error {
	// Regardless the recoveryErr, we will dump the bytes for
	// the corrupted record, however, we attempt to fix only
	// for io.ErrUnexpectedEOF.
	if recoveryErr == io.ErrUnexpectedEOF {
		fs.log.Errorf("Unexpected EOF for file %q", f.name)
		if !fs.opts.TruncateUnexpectedEOF {
			fs.log.Errorf("It is recommended that you make a copy of the whole datatstore %q.", fs.fm.rootDir)
			fs.log.Errorf("Restart with the ContinueOnUnexpectedEOF flag to truncate this file to this offset: %v.", offset)
			fs.log.Errorf("Dataloss may occur. Details about the first corrupted record follows...")
		}
	} else {
		fs.log.Errorf("Corrupted record in file %q: %v", f.name, recoveryErr)
	}
	if _, err := f.handle.Seek(offset, io.SeekStart); err != nil {
		panic(fmt.Errorf("Unable to set position of file %q to %v: %v", f.name, offset, err))
	}
	var (
		expectedSize int
		read         int
		part         string
	)
	fs.log.Errorf("Record header:")
	part = "record header"
	expectedSize = recordHeaderSize
	var (
		_header = [recordHeaderSize]byte{}
		header  = _header[:]
	)
	read, _ = io.ReadFull(f.handle, header)
	fs.log.Errorf(" Bytes:")
	dumpBytes(fs.log, header[:read], false)
	if read >= recordHeaderSize {
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
		if recTyped {
			fs.log.Errorf(" Type: %v", recType)
		}
		fs.log.Errorf(" Size: %v", recSize)
		fs.log.Errorf(" CRC : 0x%08x", crc)
		fs.log.Errorf("Record payload:")

		part = "record payload"
		expectedSize = recSize
		buf := util.EnsureBufBigEnough(nil, recSize)
		read, _ = io.ReadFull(f.handle, buf)
		dumpBytes(fs.log, buf[:read], true)
	}
	if recoveryErr == io.ErrUnexpectedEOF {
		if fs.opts.TruncateUnexpectedEOF {
			if err := fs.fm.truncateFile(f, offset); err != nil {
				return fmt.Errorf("unable to repair file %q by truncating at offset %v: %v", f.name, offset, err)
			}
			fs.log.Noticef("File %q has been truncated to offset: %v", f.name, offset)
			fs.log.Noticef("Recovery resumes...")
			return nil
		}
		fs.log.Errorf("%s expected to be %v bytes, only read %v", part, expectedSize, read)
	}
	return recoveryErr
}

func dumpBytes(log logger.Logger, buf []byte, printTxt bool) {
	lines := len(buf) / 20
	start := 0
	for i := 0; i < lines+1; i++ {
		if start >= len(buf) {
			break
		}
		end := len(buf) - start
		if end > 20 {
			end = 20
		}
		bl := fmt.Sprintf("% x", buf[start:start+end])
		if printTxt {
			tl := ""
			for b := start; b < start+end; b++ {
				c := buf[b]
				if int(c) < 32 || int(c) > 128 {
					c = '.'
				}
				tl = fmt.Sprintf("%s%s", tl, []byte{c})
			}
			var paddingStr string
			padding := 3 * (20 - end)
			if padding > 0 {
				paddingStr = fmt.Sprintf("%*s", padding, " ")
			}
			log.Errorf("%s%s - %s", bl, paddingStr, tl)
		} else {
			log.Errorf(bl)
		}
		start += end
	}
}

////////////////////////////////////////////////////////////////////////////
// FileMsgStore methods
////////////////////////////////////////////////////////////////////////////

// newFileMsgStore returns a new instace of a file MsgStore.
func (fs *FileStore) newFileMsgStore(channelDirName, channel string, limits *MsgStoreLimits, doRecover bool) (*FileMsgStore, error) {
	// Create an instance and initialize
	ms := &FileMsgStore{
		fm:           fs.fm,
		hasFDsLimit:  fs.opts.FileDescriptorsLimit > 0,
		fstore:       fs,
		wOffset:      int64(4), // The very first record starts after the file version record
		files:        make(map[int]*fileSlice),
		channelName:  channel,
		bkgTasksDone: make(chan bool, 1),
		bkgTasksWake: make(chan bool, 1),
	}
	ms.init(channel, fs.log, limits)

	ms.setSliceLimits()
	ms.initCache()

	maxBufSize := fs.opts.BufferSize
	if maxBufSize > 0 {
		ms.bw = newBufferWriter(msgBufMinShrinkSize, maxBufSize)
		ms.bufferedSeqs = make([]uint64, 0, 1)
		ms.bufferedMsgs = make(map[uint64]*bufferedMsg)
	}

	// Use this variable for all errors below so we can do the cleanup
	var err error

	// Recovery case
	if doRecover {
		var dirFiles []os.FileInfo
		var fseq int64
		var datFile, idxFile *file
		var useIdxFile bool

		dirFiles, err = ioutil.ReadDir(channelDirName)
		for _, file := range dirFiles {
			if file.IsDir() {
				continue
			}
			fileName := file.Name()
			if !strings.HasPrefix(fileName, msgFilesPrefix) || !strings.HasSuffix(fileName, datSuffix) {
				continue
			}
			// Remove suffix
			fileNameWithoutSuffix := strings.TrimSuffix(fileName, datSuffix)
			// Remove prefix
			fileNameWithoutPrefixAndSuffix := strings.TrimPrefix(fileNameWithoutSuffix, msgFilesPrefix)
			// Get the file sequence number
			fseq, err = strconv.ParseInt(fileNameWithoutPrefixAndSuffix, 10, 64)
			if err != nil {
				err = fmt.Errorf("message log has an invalid name: %v", fileName)
				break
			}
			idxFName := fmt.Sprintf("%s%v%s", msgFilesPrefix, fseq, idxSuffix)
			useIdxFile = false
			if s, statErr := os.Stat(filepath.Join(channelDirName, idxFName)); s != nil && statErr == nil {
				useIdxFile = true
			}
			datFile, err = ms.fm.createFile(filepath.Join(channel, fileName), defaultFileFlags, nil)
			if err != nil {
				break
			}
			idxFile, err = ms.fm.createFile(filepath.Join(channel, idxFName), defaultFileFlags, nil)
			if err != nil {
				ms.fm.unlockFile(datFile)
				break
			}
			// Create the slice
			fslice := &fileSlice{file: datFile, idxFile: idxFile, lastUsed: time.Now().UnixNano()}
			// Recover the file slice
			err = ms.recoverOneMsgFile(fslice, int(fseq), useIdxFile)
			if err != nil {
				break
			}
		}
		if err == nil && ms.lastFSlSeq > 0 {
			// Now that all file slices have been recovered, we know which
			// one is the last, so use it as the write slice.
			ms.writeSlice = ms.files[ms.lastFSlSeq]
			// Need to set the writer, etc..
			ms.fm.lockFile(ms.writeSlice.file)
			err = ms.setFile(ms.writeSlice, -1)
			ms.fm.unlockFile(ms.writeSlice.file)
			if err == nil {
				// Set the beforeFileClose callback to the slices now that
				// we are done recovering.
				for _, fslice := range ms.files {
					ms.fm.setBeforeCloseCb(fslice.file, ms.beforeDataFileCloseCb(fslice))
					ms.fm.setBeforeCloseCb(fslice.idxFile, ms.beforeIndexFileCloseCb(fslice))
				}
				ms.checkSlices = 1
			}
		}
		if err == nil {
			// Apply message limits (no need to check if there are limits
			// defined, the call won't do anything if they aren't).
			err = ms.enforceLimits(false, true)
		}
	}
	if err == nil {
		ms.Lock()
		ms.allDone.Add(1)
		// Capture the time here first, it will then be captured
		// in the go routine we are about to start.
		ms.timeTick = time.Now().UnixNano()
		// On recovery, if there is age limit set and at least one message...
		if doRecover {
			if ms.limits.MaxAge > 0 && ms.totalCount > 0 {
				// Force the execution of the expireMsgs method.
				// This will take care of expiring messages that should have
				// expired while the server was stopped.
				ms.expireMsgs(ms.timeTick, int64(ms.limits.MaxAge))
			}
			// Now that we are done with recovery, close the write slice
			if ms.writeSlice != nil {
				ms.fm.closeFileIfOpened(ms.writeSlice.file)
				ms.fm.closeFileIfOpened(ms.writeSlice.idxFile)
			}
		}
		// Start the background tasks go routine
		go ms.backgroundTasks()
		ms.Unlock()
	}
	// Cleanup on error
	if err != nil {
		// The buffer writer may not be fully set yet
		if ms.bw != nil && ms.bw.buf == nil {
			ms.bw = nil
		}
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

// beforeDataFileCloseCb returns a beforeFileClose callback to be used
// by FileMsgStore's files when a data file for that slice is being closed.
// This is invoked asynchronously and should not acquire the store's lock.
// That being said, we have the guarantee that this will be not be invoked
// concurrently for a given file and that the store will not be using this file.
func (ms *FileMsgStore) beforeDataFileCloseCb(fslice *fileSlice) beforeFileClose {
	return func() error {
		if fslice != ms.writeSlice {
			return nil
		}
		if ms.bw != nil && ms.bw.buf != nil && ms.bw.buf.Buffered() > 0 {
			if err := ms.bw.buf.Flush(); err != nil {
				return err
			}
		}
		if ms.fstore.opts.DoSync {
			if err := fslice.file.handle.Sync(); err != nil {
				return err
			}
		}
		ms.writer = nil
		return nil
	}
}

// beforeIndexFileCloseCb returns a beforeFileClose callback to be used
// by FileMsgStore's files when an index file for that slice is being closed.
// This is invoked asynchronously and should not acquire the store's lock.
// That being said, we have the guarantee that this will be not be invoked
// concurrently for a given file and that the store will not be using this file.
func (ms *FileMsgStore) beforeIndexFileCloseCb(fslice *fileSlice) beforeFileClose {
	return func() error {
		if fslice != ms.writeSlice {
			return nil
		}
		if len(ms.bufferedMsgs) > 0 {
			if err := ms.processBufferedMsgs(fslice); err != nil {
				return err
			}
		}
		if ms.fstore.opts.DoSync {
			if err := fslice.idxFile.handle.Sync(); err != nil {
				return err
			}
		}
		return nil
	}
}

// setFile sets the current data and index file.
// The buffered writer is recreated.
func (ms *FileMsgStore) setFile(fslice *fileSlice, offset int64) error {
	var err error
	file := fslice.file.handle
	ms.writer = file
	if file != nil && ms.bw != nil {
		ms.writer = ms.bw.createNewWriter(file)
	}
	if offset == -1 {
		ms.wOffset, err = file.Seek(0, io.SeekEnd)
	} else {
		ms.wOffset = offset
	}
	return err
}

func (ms *FileMsgStore) doLockFiles(fslice *fileSlice, onlyIndexFile bool) error {
	var datWasOpened, idxWasOpened bool
	var err error

	if !onlyIndexFile {
		datWasOpened, err = ms.fm.lockFile(fslice.file)
		if err != nil {
			return err
		}
	}
	idxWasOpened, err = ms.fm.lockFile(fslice.idxFile)
	if err != nil {
		if !datWasOpened {
			ms.fm.unlockFile(fslice.file)
		}
		return err
	}
	if !onlyIndexFile {
		// We need to reset writer/offset only if the data file is opened
		// in this call and it is the slice to which we are currently
		// writing to.
		if fslice == ms.writeSlice && !datWasOpened {
			err = ms.setFile(fslice, -1)
		}
	}
	// If we try to limit FDs use or simply not the write slice, then
	// we need to notify the background task code that it should
	// try to close unused slices.
	if ms.hasFDsLimit || fslice != ms.writeSlice {
		if !datWasOpened || !idxWasOpened {
			atomic.StoreInt64(&ms.checkSlices, 1)
		}
		if fslice.lastUsed == 0 {
			fslice.lastUsed = atomic.LoadInt64(&ms.timeTick)
		} else {
			fslice.lastUsed++
		}
	}
	return err
}

// lockFiles locks the data and index files of the given file slice.
// If files were closed they are opened in this call, and if so,
// and if this slice is the write slice, the writer and offset are reset.
func (ms *FileMsgStore) lockFiles(fslice *fileSlice) error {
	return ms.doLockFiles(fslice, false)
}

// lockIndexFile locks the index file of the given file slice.
// If the file was closed it is opened in this call.
func (ms *FileMsgStore) lockIndexFile(fslice *fileSlice) error {
	return ms.doLockFiles(fslice, true)
}

// unlockIndexFile unlocks the already locked index file of the given file slice.
func (ms *FileMsgStore) unlockIndexFile(fslice *fileSlice) {
	ms.fm.unlockFile(fslice.idxFile)
}

// unlockFiles unlocks both data and index files of the given file slice.
func (ms *FileMsgStore) unlockFiles(fslice *fileSlice) {
	ms.fm.unlockFile(fslice.file)
	ms.fm.unlockFile(fslice.idxFile)
}

// closeLockedFiles (unlocks and) closes the files of the given file slice.
func (ms *FileMsgStore) closeLockedFiles(fslice *fileSlice) error {
	err := ms.fm.closeLockedFile(fslice.file)
	if idxErr := ms.fm.closeLockedFile(fslice.idxFile); idxErr != nil && err == nil {
		err = idxErr
	}
	return err
}

// recovers one of the file
func (ms *FileMsgStore) recoverOneMsgFile(fslice *fileSlice, fseq int, useIdxFile bool) error {
	var err error

	msgSize := 0
	var msg *pb.MsgProto
	var mindex *msgIndex
	var seq uint64

	// Select which file to recover based on presence of index file
	file := fslice.file
	if useIdxFile {
		file = fslice.idxFile
	}

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(file.handle, defaultBufSize)

	// The first record starts after the file version record
	offset := int64(4)

	if useIdxFile {
		var (
			lastIndex *msgIndex
			lastSeq   uint64
		)
		for {
			seq, mindex, err = ms.readIndex(br)
			if err != nil {
				switch err {
				case io.EOF:
					// We are done, reset err
					err = nil
				case errNeedRewind:
					err = ms.fm.truncateFile(file, offset)
				}
				break
			}

			// Update file slice
			if fslice.firstSeq == 0 {
				fslice.firstSeq = seq
			}
			fslice.lastSeq = seq
			fslice.msgsCount++
			// For size, add the message record size, the record header and the size
			// required for the corresponding index record.
			fslice.msgsSize += uint64(mindex.msgSize + msgRecordOverhead)
			if fslice.firstWrite == 0 {
				fslice.firstWrite = mindex.timestamp
			}
			lastIndex = mindex
			lastSeq = seq
			offset += msgIndexRecSize
		}
		if err == nil {
			err = ms.ensureLastMsgAndIndexMatch(fslice, lastSeq, lastIndex)
			if err != nil {
				ms.fstore.log.Errorf(err.Error())
				if _, serr := fslice.file.handle.Seek(4, io.SeekStart); serr != nil {
					panic(fmt.Errorf("File %q: unable to set position to beginning of file: %v", fslice.file.name, serr))
				}
			}
		} else {
			ms.fstore.log.Errorf("Error with index file %q: %v. Truncating and recovering from data file", fslice.idxFile.name, err)
		}
		// We can get an error either because the index file was corrupted,
		// or because the data file is. In both case, we truncate the index
		// file and recover from data file. The handling of unexpected EOF
		// is handled in the data file recovery down below.
		if err != nil {
			if terr := ms.fm.truncateFile(fslice.idxFile, 4); terr != nil {
				panic(fmt.Errorf("Error during recovery of file %q: %v, you need "+
					"to manually remove index file %q (truncate failed with err: %v)",
					fslice.file.name, err, fslice.idxFile.name, terr))
			}
			fslice.firstSeq = 0
			fslice.lastSeq = 0
			fslice.msgsCount = 0
			fslice.msgsSize = 0
			fslice.firstWrite = 0
			file = fslice.file
			br = bufio.NewReaderSize(file.handle, defaultBufSize)
			err = nil
			useIdxFile = false
		}
	}
	// No `else` here because in case of error recovering index file, we will do data file recovery
	if !useIdxFile {
		// Get these from the file store object
		crcTable := ms.fstore.crcTable
		doCRC := ms.fstore.opts.DoCRC

		// We are going to write the index file while recovering the data file
		bw := bufio.NewWriterSize(fslice.idxFile.handle, msgIndexRecSize*1000)

		for {
			ms.tmpMsgBuf, msgSize, _, err = readRecord(br, ms.tmpMsgBuf, false, crcTable, doCRC)
			if err != nil {
				switch err {
				case io.EOF:
					// We are done, reset err
					err = nil
				case errNeedRewind:
					err = ms.fm.truncateFile(file, offset)
				default:
					err = ms.fstore.handleUnexpectedEOF(err, file, offset, false)
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
			// For size, add the message record size, the record header and the size
			// required for the corresponding index record.
			fslice.msgsSize += uint64(msgSize + msgRecordOverhead)
			if fslice.firstWrite == 0 {
				fslice.firstWrite = msg.Timestamp
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
				err = fslice.idxFile.handle.Sync()
			}
		}
		// Since there was no index and there was an error, remove the index
		// file so when server restarts, it recovers again from the data file.
		if err != nil {
			// Close the index file
			ms.fm.closeLockedFile(fslice.idxFile)
			// Remove form store's map
			ms.fm.remove(fslice.idxFile)
			// Remove it, and panic if we can't
			if rmErr := os.Remove(fslice.idxFile.name); rmErr != nil {
				panic(fmt.Errorf("Error during recovery of file %q: %v, you need "+
					"to manually remove index file %q (remove failed with err: %v)",
					fslice.file.name, err, fslice.idxFile.name, rmErr))
			}
			// Close the data file
			ms.fm.closeLockedFile(fslice.file)
			return err
		}
	}

	// Close the files
	ms.fm.closeLockedFile(fslice.file)
	ms.fm.closeLockedFile(fslice.idxFile)

	// If no error and slice is not empty...
	if fslice.msgsCount > 0 {
		if ms.first == 0 || ms.first > fslice.firstSeq {
			ms.first = fslice.firstSeq
		}
		if ms.last < fslice.lastSeq {
			ms.last = fslice.lastSeq
		}
		ms.totalCount += fslice.msgsCount
		ms.totalBytes += fslice.msgsSize

		// On success, add to the map of file slices and
		// update first/last file slice sequence.
		ms.files[fseq] = fslice
		if ms.firstFSlSeq == 0 || ms.firstFSlSeq > fseq {
			ms.firstFSlSeq = fseq
		}
		if ms.lastFSlSeq < fseq {
			ms.lastFSlSeq = fseq
		}
		return nil
	}
	// Slice was empty and not recovered. Need to remove those from store's files manager.
	ms.fm.remove(fslice.file)
	ms.fm.remove(fslice.idxFile)
	return nil
}

func (ms *FileMsgStore) ensureLastMsgAndIndexMatch(fslice *fileSlice, seq uint64, index *msgIndex) error {
	var (
		msgSize  int
		err      error
		startErr = fmt.Sprintf("Verification of last message for file %q failed", fslice.file.name)
	)
	fd := fslice.file.handle
	// Position for the last record
	if _, err := fd.Seek(index.offset, io.SeekStart); err != nil {
		return fmt.Errorf("%s: unable to set position to %v", startErr, index.offset)
	}
	ms.tmpMsgBuf, msgSize, _, err = readRecord(fd, ms.tmpMsgBuf, false, ms.fstore.crcTable, true)
	if err != nil {
		return fmt.Errorf("%s: unable to read last record: %v", startErr, err)
	}
	if uint32(msgSize) != index.msgSize {
		return fmt.Errorf("%s: last message size in index is %v, data file is %v",
			startErr, index.msgSize, msgSize)
	}
	// Recover this message
	msg := &pb.MsgProto{}
	if err := msg.Unmarshal(ms.tmpMsgBuf[:msgSize]); err != nil {
		return fmt.Errorf("%s: error decoding message: %v", startErr, err)
	}
	if msg.Sequence != seq {
		return fmt.Errorf("%s: last message sequence in index is %v, data file is %v",
			startErr, seq, msg.Sequence)
	}
	return nil
}

// setSliceLimits sets the limits of a file slice based on options and/or
// channel limits.
func (ms *FileMsgStore) setSliceLimits() {
	// First set slice limits based on slice configuration.
	ms.slCountLim = ms.fstore.opts.SliceMaxMsgs
	ms.slSizeLim = uint64(ms.fstore.opts.SliceMaxBytes)
	ms.slAgeLim = int64(ms.fstore.opts.SliceMaxAge)
	// Did we configure any of the "dimension"?
	ms.slHasLimits = ms.slCountLim > 0 || ms.slSizeLim > 0 || ms.slAgeLim > 0

	// If so, we are done. We will use those limits to decide
	// when to move to a new slice.
	if ms.slHasLimits {
		return
	}

	// Slices limits were not configured. We will set a limit based on channel limits.
	if ms.limits.MaxMsgs > 0 {
		limit := ms.limits.MaxMsgs / 4
		if limit == 0 {
			limit = 1
		}
		ms.slCountLim = limit
	}
	if ms.limits.MaxBytes > 0 {
		limit := uint64(ms.limits.MaxBytes) / 4
		if limit == 0 {
			limit = 1
		}
		ms.slSizeLim = limit
	}
	if ms.limits.MaxAge > 0 {
		limit := time.Duration(int64(ms.limits.MaxAge) / 4)
		if limit < time.Second {
			limit = time.Second
		}
		ms.slAgeLim = int64(limit)
	}
	// Refresh our view of slices having limits.
	ms.slHasLimits = ms.slCountLim > 0 || ms.slSizeLim > 0 || ms.slAgeLim > 0
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
// and returns an allocated msgIndex object.
func (ms *FileMsgStore) readIndex(r io.Reader) (uint64, *msgIndex, error) {
	_buf := [msgIndexRecSize]byte{}
	buf := _buf[:]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	mindex := &msgIndex{}
	seq := util.ByteOrder.Uint64(buf)
	mindex.offset = int64(util.ByteOrder.Uint64(buf[8:]))
	mindex.timestamp = int64(util.ByteOrder.Uint64(buf[16:]))
	mindex.msgSize = util.ByteOrder.Uint32(buf[24:])
	// If all zeros, return that caller should rewind (for recovery)
	if seq == 0 && mindex.offset == 0 && mindex.timestamp == 0 && mindex.msgSize == 0 {
		storedCRC := util.ByteOrder.Uint32(buf[msgIndexRecSize-crcSize:])
		if storedCRC == 0 {
			return 0, nil, errNeedRewind
		}
	}
	if ms.fstore.opts.DoCRC {
		storedCRC := util.ByteOrder.Uint32(buf[msgIndexRecSize-crcSize:])
		crc := crc32.Checksum(buf[:msgIndexRecSize-crcSize], ms.fstore.crcTable)
		if storedCRC != crc {
			return 0, nil, fmt.Errorf("corrupted data, expected crc to be 0x%08x, got 0x%08x", storedCRC, crc)
		}
	}
	return seq, mindex, nil
}

// Store a given message.
func (ms *FileMsgStore) Store(m *pb.MsgProto) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	if m.Sequence <= ms.last {
		// We've already seen this message.
		return m.Sequence, nil
	}

	fslice := ms.writeSlice
	if fslice != nil {
		if err := ms.lockFiles(fslice); err != nil {
			return 0, err
		}
	}

	// Is there a gap in message sequence?
	if ms.last > 0 && m.Sequence > ms.last+1 {
		if err := ms.fillGaps(fslice, m); err != nil {
			ms.unlockFiles(fslice)
			return 0, err
		}
	}

	// Check if we need to move to next file slice
	if fslice == nil || ms.slHasLimits {
		if fslice == nil ||
			(ms.slSizeLim > 0 && fslice.msgsSize >= ms.slSizeLim) ||
			(ms.slCountLim > 0 && fslice.msgsCount >= ms.slCountLim) ||
			(ms.slAgeLim > 0 && atomic.LoadInt64(&ms.timeTick)-fslice.firstWrite >= ms.slAgeLim) {

			// Don't change store variable until success...
			newSliceSeq := ms.lastFSlSeq + 1

			// Close the current file slice (if applicable) and open the next slice
			if fslice != nil {
				if err := ms.closeLockedFiles(fslice); err != nil {
					return 0, err
				}
			}
			// Create new slice
			datFName := filepath.Join(ms.channelName, fmt.Sprintf("%s%v%s", msgFilesPrefix, newSliceSeq, datSuffix))
			idxFName := filepath.Join(ms.channelName, fmt.Sprintf("%s%v%s", msgFilesPrefix, newSliceSeq, idxSuffix))
			datFile, err := ms.fm.createFile(datFName, defaultFileFlags, nil)
			if err != nil {
				return 0, err
			}
			idxFile, err := ms.fm.createFile(idxFName, defaultFileFlags, nil)
			if err != nil {
				ms.fm.closeLockedFile(datFile)
				ms.fm.remove(datFile)
				return 0, err
			}
			// Success, update the store's variables
			newSlice := &fileSlice{
				file:     datFile,
				idxFile:  idxFile,
				lastUsed: atomic.LoadInt64(&ms.timeTick),
			}
			ms.fm.setBeforeCloseCb(datFile, ms.beforeDataFileCloseCb(newSlice))
			ms.fm.setBeforeCloseCb(idxFile, ms.beforeIndexFileCloseCb(newSlice))
			ms.files[newSliceSeq] = newSlice
			ms.writeSlice = newSlice
			if ms.firstFSlSeq == 0 {
				ms.firstFSlSeq = newSliceSeq
			}
			ms.lastFSlSeq = newSliceSeq
			ms.setFile(newSlice, 4)

			// If we added a second slice and the first slice was empty but not removed
			// because it was the only one, we remove it now.
			if len(ms.files) == 2 && fslice.msgsCount == fslice.rmCount {
				ms.removeFirstSlice()
			}
			// Update the fslice reference to new slice for rest of function
			fslice = ms.writeSlice
		}
	}

	// !! IMPORTANT !!
	// We want to reduce use of defer in functions that are in the fast path,
	// so after this point, on error, use goto processErr instead of return.
	// It means that we should not use local errors like this:
	// if err := this(); err != nil {
	//    goto processErr
	// }

	seq := m.Sequence

	msgInBuffer := false

	var recSize int
	var err error
	var mindex *msgIndex
	var size uint64

	var bwBuf *bufio.Writer
	if ms.bw != nil {
		bwBuf = ms.bw.buf
	}
	msgSize := m.Size()
	if bwBuf != nil {
		required := msgSize + recordHeaderSize
		if required > bwBuf.Available() {
			ms.writer, err = ms.bw.expand(fslice.file.handle, required)
			if err != nil {
				goto processErr
			}
			if len(ms.bufferedMsgs) > 0 {
				err = ms.processBufferedMsgs(fslice)
				if err != nil {
					goto processErr
				}
			}
			// Refresh this since it has changed.
			bwBuf = ms.bw.buf
		}
	}
	ms.tmpMsgBuf, recSize, err = writeRecord(ms.writer, ms.tmpMsgBuf, recNoType, m, msgSize, ms.fstore.crcTable)
	if err != nil {
		goto processErr
	}
	if bwBuf != nil {
		// Check to see if we should cancel a buffer shrink request
		if ms.bw.shrinkReq {
			ms.bw.checkShrinkRequest()
		}
		// If message was added to the buffer we need to also save a reference
		// to that message outside of the cache, until the buffer is flushed.
		if bwBuf.Buffered() >= recSize {
			ms.bufferedSeqs = append(ms.bufferedSeqs, seq)
			mindex = &msgIndex{offset: ms.wOffset, timestamp: m.Timestamp, msgSize: uint32(msgSize)}
			ms.bufferedMsgs[seq] = &bufferedMsg{msg: m, index: mindex}
			msgInBuffer = true
		}
	}
	// Message was flushed to disk, write corresponding index
	if !msgInBuffer {
		err = ms.writeIndex(fslice.idxFile.handle, seq, ms.wOffset, m.Timestamp, msgSize)
		if err != nil {
			goto processErr
		}
	}

	if ms.first == 0 || ms.first == seq {
		// First ever message or after all messages expired and this is the
		// first new message.
		ms.first = seq
		ms.firstMsg = m
		if maxAge := ms.limits.MaxAge; maxAge > 0 {
			ms.expiration = m.Timestamp + int64(maxAge)
			if len(ms.bkgTasksWake) == 0 {
				ms.bkgTasksWake <- true
			}
		}
	}
	ms.last = seq
	ms.lastMsg = m
	ms.cache.add(seq, m, true)
	ms.wOffset += int64(recSize)

	// For size, add the message record size, the record header and the size
	// required for the corresponding index record.
	size = uint64(msgSize + msgRecordOverhead)

	// Total stats
	ms.totalCount++
	ms.totalBytes += size

	// Stats per file slice
	fslice.msgsCount++
	fslice.msgsSize += size
	if fslice.firstWrite == 0 {
		fslice.firstWrite = m.Timestamp
	}

	// Save references to first and last sequences for this slice
	if fslice.firstSeq == 0 {
		fslice.firstSeq = seq
	}
	fslice.lastSeq = seq

	if ms.limits.MaxMsgs > 0 || ms.limits.MaxBytes > 0 {
		// Enfore limits and update file slice if needed.
		err = ms.enforceLimits(true, false)
		if err != nil {
			goto processErr
		}
	}
	ms.unlockFiles(fslice)
	return seq, nil

processErr:
	ms.unlockFiles(fslice)
	return 0, err
}

func (ms *FileMsgStore) fillGaps(fslice *fileSlice, upToMsg *pb.MsgProto) error {
	// flush possible buffered messages.
	if err := ms.flush(fslice); err != nil {
		return err
	}

	var (
		recSize int
		err     error
		msgSize int
	)

	ms.lastMsg = nil
	emptyMsg := &pb.MsgProto{
		Subject:   ms.channelName,
		Timestamp: upToMsg.Timestamp,
	}
	for i := ms.last + 1; i < upToMsg.Sequence; i++ {
		emptyMsg.Sequence = i
		msgSize = emptyMsg.Size()
		ms.tmpMsgBuf, recSize, err = writeRecord(fslice.file.handle, ms.tmpMsgBuf, recNoType, emptyMsg, msgSize, ms.fstore.crcTable)
		if err != nil {
			return err
		}
		if err := ms.writeIndex(fslice.idxFile.handle, i, ms.wOffset, emptyMsg.Timestamp, msgSize); err != nil {
			return err
		}
		ms.wOffset += int64(recSize)
		ms.last++
		ms.totalCount++
		size := uint64(msgSize + msgRecordOverhead)
		ms.totalBytes += size
		fslice.lastSeq = i
		fslice.msgsCount++
		fslice.msgsSize += size
	}
	return nil
}

// processBufferedMsgs adds message index records in the given buffer
// for every pending buffered messages.
func (ms *FileMsgStore) processBufferedMsgs(fslice *fileSlice) error {
	idxBufferSize := len(ms.bufferedMsgs) * msgIndexRecSize
	ms.tmpMsgBuf = util.EnsureBufBigEnough(ms.tmpMsgBuf, idxBufferSize)
	bufOffset := 0
	for _, pseq := range ms.bufferedSeqs {
		bm := ms.bufferedMsgs[pseq]
		if bm != nil {
			mindex := bm.index
			// We add the index info for this flushed message
			ms.addIndex(ms.tmpMsgBuf[bufOffset:], pseq, mindex.offset,
				mindex.timestamp, int(mindex.msgSize))
			bufOffset += msgIndexRecSize
			delete(ms.bufferedMsgs, pseq)
		}
	}
	if bufOffset > 0 {
		if _, err := fslice.idxFile.handle.Write(ms.tmpMsgBuf[:bufOffset]); err != nil {
			return err
		}
	}
	ms.bufferedSeqs = ms.bufferedSeqs[:0]
	return nil
}

// expireMsgs ensures that messages don't stay in the log longer than the
// limit's MaxAge.
// Returns the time of the next expiration (possibly 0 if no message left)
// The store's lock is assumed to be held on entry
func (ms *FileMsgStore) expireMsgs(now, maxAge int64) int64 {
	if ms.first == 0 {
		ms.expiration = 0
		return ms.expiration
	}
	var m *msgIndex
	var slice *fileSlice
	for {
		m = nil
		if ms.first <= ms.last {
			if slice == nil || ms.first > slice.lastSeq {
				// If slice is not nil, it means that we have expired all
				// messages belong to that slice, and the slice itslef.
				// So there is no need to unlock it since this has already
				// been done.
				slice = ms.getFileSliceForSeq(ms.first)
				if slice == nil {
					// If we did not find a slice for this sequence, it could
					// be cause there is a gap in message sequence due to
					// file truncation following unexpected EOF on recovery.
					// So set the first seq to the first sequence of the now
					// first slice.
					slice = ms.files[ms.firstFSlSeq]
					if slice != nil {
						ms.first = slice.firstSeq
					}
				}
				if slice != nil {
					if err := ms.lockIndexFile(slice); err != nil {
						slice = nil
						break
					}
				}
			}
			if slice != nil {
				m = ms.getMsgIndex(slice, ms.first)
			}
		}
		if m == nil {
			ms.expiration = 0
			break
		}
		elapsed := now - m.timestamp
		if elapsed >= maxAge {
			ms.removeFirstMsg(m, false)
		} else {
			if elapsed < 0 {
				ms.expiration = m.timestamp + maxAge
			} else {
				ms.expiration = now + (maxAge - elapsed)
			}
			break
		}
	}
	if slice != nil {
		ms.unlockIndexFile(slice)
	}
	return ms.expiration
}

// enforceLimits checks total counts with current msg store's limits,
// removing a file slice and/or updating slices' count as necessary.
func (ms *FileMsgStore) enforceLimits(reportHitLimit, lockFile bool) error {
	// Check if we need to remove any (but leave at least the last added).
	// Note that we may have to remove more than one msg if we are here
	// after a restart with smaller limits than originally set, or if
	// message is quite big, etc...
	maxMsgs := ms.limits.MaxMsgs
	maxBytes := ms.limits.MaxBytes
	for ms.totalCount > 1 &&
		((maxMsgs > 0 && ms.totalCount > maxMsgs) ||
			(maxBytes > 0 && ms.totalBytes > uint64(maxBytes))) {

		// Remove first message from first slice, potentially removing
		// the slice, etc...
		ms.removeFirstMsg(nil, lockFile)
		if reportHitLimit && !ms.hitLimit {
			ms.hitLimit = true
			ms.log.Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxMsgs,
				util.FriendlyBytes(int64(ms.totalBytes)), util.FriendlyBytes(ms.limits.MaxBytes))
		}
	}
	return nil
}

// getMsgIndex returns a msgIndex object for message with sequence `seq`,
// or nil if message is not found (or no longer valid: expired, removed
// due to limits, etc).
// This call first checks that the record is not present in
// ms.bufferedMsgs since it is possible that message and index are not
// yet stored on disk.
func (ms *FileMsgStore) getMsgIndex(slice *fileSlice, seq uint64) *msgIndex {
	bm := ms.bufferedMsgs[seq]
	if bm != nil {
		return bm.index
	}
	return ms.readMsgIndex(slice, seq)
}

// readMsgIndex reads a message index record from disk and returns a msgIndex
// object. Same than getMsgIndex but without checking for message in
// ms.bufferedMsgs first.
func (ms *FileMsgStore) readMsgIndex(slice *fileSlice, seq uint64) *msgIndex {
	// Compute the offset in the index file itself.
	idxFileOffset := 4 + (int64(seq-slice.firstSeq)+int64(slice.rmCount))*msgIndexRecSize
	// Then position the file pointer of the index file.
	if _, err := slice.idxFile.handle.Seek(idxFileOffset, io.SeekStart); err != nil {
		return nil
	}
	// Read the index record and ensure we have what we expect
	seqInIndexFile, msgIndex, err := ms.readIndex(slice.idxFile.handle)
	if seqInIndexFile != seq || err != nil {
		return nil
	}
	return msgIndex
}

// removeFirstMsg "removes" the first message of the first slice.
// If the slice is "empty" the file slice is removed.
func (ms *FileMsgStore) removeFirstMsg(mindex *msgIndex, lockFile bool) {
	// Work with the first slice
	slice := ms.files[ms.firstFSlSeq]
	// Get the message index for the first valid message in this slice
	if mindex == nil {
		if lockFile || slice != ms.writeSlice {
			ms.lockIndexFile(slice)
		}
		mindex = ms.getMsgIndex(slice, slice.firstSeq)
		if lockFile || slice != ms.writeSlice {
			ms.unlockIndexFile(slice)
		}
	}
	// Size of the first message in this slice
	firstMsgSize := mindex.msgSize
	// For size, we count the size of serialized message + record header +
	// the corresponding index record
	size := uint64(firstMsgSize + msgRecordOverhead)
	// Keep track of number of "removed" messages in this slice
	slice.rmCount++
	// Update total counts
	ms.totalCount--
	ms.totalBytes -= size
	// Messages sequence is incremental with no gap on a given msgstore.
	ms.first++
	// Invalidate ms.firstMsg, it will be looked-up on demand.
	ms.firstMsg = nil
	// Invalidate ms.lastMsg if it was the last message being removed.
	if ms.first > ms.last {
		ms.lastMsg = nil
	}
	// Is file slice is "empty" and not the last one
	if slice.msgsCount == slice.rmCount && len(ms.files) > 1 {
		ms.removeFirstSlice()
	} else {
		// This is the new first message in this slice.
		slice.firstSeq = ms.first
	}
}

// removeFirstSlice removes the first file slice.
// Should not be called if first slice is also last!
func (ms *FileMsgStore) removeFirstSlice() {
	sl := ms.files[ms.firstFSlSeq]
	// We may or may not have the first slice locked, so need to close
	// the file knowing that files can be in either state.
	ms.fm.closeLockedOrOpenedFile(sl.file)
	ms.fm.remove(sl.file)
	// Close index file too.
	ms.fm.closeLockedOrOpenedFile(sl.idxFile)
	ms.fm.remove(sl.idxFile)
	// Assume we will remove the files
	remove := true
	// If there is an archive script invoke it first
	script := ms.fstore.opts.SliceArchiveScript
	if script != "" {
		datBak := sl.file.name + bakSuffix
		idxBak := sl.idxFile.name + bakSuffix

		var err error
		if err = os.Rename(sl.file.name, datBak); err == nil {
			if err = os.Rename(sl.idxFile.name, idxBak); err != nil {
				// Remove first backup file
				os.Remove(datBak)
			}
		}
		if err == nil {
			// Files have been successfully renamed, so don't attempt
			// to remove the original files.
			remove = false

			// We run the script in a go routine to not block the server.
			ms.allDone.Add(1)
			go func(subj, dat, idx string) {
				defer ms.allDone.Done()
				cmd := exec.Command(script, subj, dat, idx)
				output, err := cmd.CombinedOutput()
				if err != nil {
					ms.log.Noticef("Error invoking archive script %q: %v (output=%v)", script, err, string(output))
				} else {
					ms.log.Noticef("Output of archive script for %s (%s and %s): %v", subj, dat, idx, string(output))
				}
			}(ms.subject, datBak, idxBak)
		}
	}
	// Remove files
	if remove {
		os.Remove(sl.file.name)
		os.Remove(sl.idxFile.name)
	}
	// Remove slice from map
	delete(ms.files, ms.firstFSlSeq)
	// Normally, file slices have an incremental sequence number with
	// no gap. However, we want to support the fact that an user could
	// copy back some old file slice to be recovered, and so there
	// may be a gap. So find out what is the new first file sequence.
	for ms.firstFSlSeq < ms.lastFSlSeq {
		ms.firstFSlSeq++
		if _, ok := ms.files[ms.firstFSlSeq]; ok {
			break
		}
	}
	// This should not happen!
	if ms.firstFSlSeq > ms.lastFSlSeq {
		panic("Removed last slice!")
	}
}

// getFileSliceForSeq returns the file slice where the message of the
// given sequence is stored, or nil if the message is not found in any
// of the file slices.
func (ms *FileMsgStore) getFileSliceForSeq(seq uint64) *fileSlice {
	if len(ms.files) == 0 {
		return nil
	}
	// Start with write slice
	slice := ms.writeSlice
	if (slice.firstSeq <= seq) && (seq <= slice.lastSeq) {
		return slice
	}
	// We want to support possible gaps in file slice sequence, so
	// no dichotomy, but simple iteration of the map, which in Go is
	// random.
	for _, slice := range ms.files {
		if (slice.firstSeq <= seq) && (seq <= slice.lastSeq) {
			return slice
		}
	}
	return nil
}

// backgroundTasks performs some background tasks related to this
// messages store.
func (ms *FileMsgStore) backgroundTasks() {
	defer ms.allDone.Done()

	ms.RLock()
	hasBuffer := ms.bw != nil
	maxAge := int64(ms.limits.MaxAge)
	nextExpiration := ms.expiration
	lastCacheCheck := ms.timeTick
	lastBufShrink := ms.timeTick
	ms.RUnlock()

	for {
		// Update time
		timeTick := time.Now().UnixNano()
		atomic.StoreInt64(&ms.timeTick, timeTick)

		// Close unused file slices
		if atomic.LoadInt64(&ms.checkSlices) == 1 {
			ms.Lock()
			opened := 0
			for _, slice := range ms.files {
				// If no FD limit and this is the write slice, skip.
				if !ms.hasFDsLimit && slice == ms.writeSlice {
					continue
				}
				opened++
				if slice.lastUsed > 0 && time.Duration(timeTick-slice.lastUsed) >= sliceCloseInterval {
					slice.lastUsed = 0
					ms.fm.closeFileIfOpened(slice.file)
					ms.fm.closeFileIfOpened(slice.idxFile)
					opened--
				}
			}
			if opened == 0 {
				// We can update this without atomic since we are under store lock
				// and this go routine is the only place where we check the value.
				ms.checkSlices = 0
			}
			ms.Unlock()
		}

		// Shrink the buffer if applicable
		if hasBuffer && time.Duration(timeTick-lastBufShrink) >= bufShrinkInterval {
			ms.Lock()
			if ms.writeSlice != nil {
				file := ms.writeSlice.file
				if ms.fm.lockFileIfOpened(file) {
					ms.writer, _ = ms.bw.tryShrinkBuffer(file.handle)
					ms.fm.unlockFile(file)
				}
			}
			ms.Unlock()
			lastBufShrink = timeTick
		}

		// Check for expiration
		if maxAge > 0 && nextExpiration > 0 && timeTick >= nextExpiration {
			ms.Lock()
			// Expire messages
			nextExpiration = ms.expireMsgs(timeTick, maxAge)
			ms.Unlock()
		}

		// Check for message caching
		if timeTick >= lastCacheCheck+cacheTTL {
			tryEvict := atomic.LoadInt32(&ms.cache.tryEvict)
			if tryEvict == 1 {
				ms.Lock()
				// Possibly remove some/all cached messages
				ms.cache.evict(timeTick)
				ms.Unlock()
			}
			lastCacheCheck = timeTick
		}

		select {
		case <-ms.bkgTasksDone:
			return
		case <-ms.bkgTasksWake:
			// wake up from a possible sleep to run the loop
			ms.RLock()
			nextExpiration = ms.expiration
			ms.RUnlock()
		case <-time.After(bkgTasksSleepDuration):
			// go back to top of for loop.
		}
	}
}

// lookup returns the message for the given sequence number, possibly
// reading the message from disk.
// Store write lock is assumed to be held on entry
func (ms *FileMsgStore) lookup(seq uint64) (*pb.MsgProto, error) {
	// Reject message for sequence outside valid range
	if seq < ms.first || seq > ms.last {
		return nil, nil
	}
	// Check first if it's in the cache.
	msg := ms.cache.get(seq)
	if msg == nil && ms.bufferedMsgs != nil {
		// Possibly in bufferedMsgs
		bm := ms.bufferedMsgs[seq]
		if bm != nil {
			msg = bm.msg
			ms.cache.add(seq, msg, false)
		}
	}
	// If not, we need to read it from disk...
	if msg == nil {
		fslice := ms.getFileSliceForSeq(seq)
		if fslice == nil {
			return nil, nil
		}
		err := ms.lockFiles(fslice)
		if err != nil {
			return nil, err
		}
		msgIndex := ms.readMsgIndex(fslice, seq)
		if msgIndex != nil {
			file := fslice.file.handle
			// Position file to message's offset. 0 means from start.
			_, err = file.Seek(msgIndex.offset, io.SeekStart)
			if err == nil {
				ms.tmpMsgBuf, _, _, err = readRecord(file, ms.tmpMsgBuf, false, ms.fstore.crcTable, ms.fstore.opts.DoCRC)
			}
		}
		ms.unlockFiles(fslice)
		if err != nil || msgIndex == nil {
			return nil, err
		}
		// Recover this message
		msg = &pb.MsgProto{}
		err = msg.Unmarshal(ms.tmpMsgBuf[:msgIndex.msgSize])
		if err != nil {
			return nil, err
		}
		ms.cache.add(seq, msg, false)
	}
	return msg, nil
}

// Lookup returns the stored message with given sequence number.
func (ms *FileMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.Lock()
	msg, err := ms.lookup(seq)
	ms.Unlock()
	return msg, err
}

// FirstMsg returns the first message stored.
func (ms *FileMsgStore) FirstMsg() (*pb.MsgProto, error) {
	var err error
	ms.RLock()
	if ms.firstMsg == nil {
		ms.firstMsg, err = ms.lookup(ms.first)
	}
	m := ms.firstMsg
	ms.RUnlock()
	return m, err
}

// LastMsg returns the last message stored.
func (ms *FileMsgStore) LastMsg() (*pb.MsgProto, error) {
	var err error
	ms.RLock()
	if ms.lastMsg == nil {
		ms.lastMsg, err = ms.lookup(ms.last)
	}
	m := ms.lastMsg
	ms.RUnlock()
	return m, err
}

// GetSequenceFromTimestamp returns the sequence of the first message whose
// timestamp is greater or equal to given timestamp.
func (ms *FileMsgStore) GetSequenceFromTimestamp(timestamp int64) (uint64, error) {
	ms.RLock()
	defer ms.RUnlock()

	// No message ever stored
	if ms.first == 0 {
		return 0, nil
	}
	// All messages have expired
	if ms.first > ms.last {
		return ms.last + 1, nil
	}
	// If we have some state, try to quickly get the sequence
	if ms.firstMsg != nil && ms.firstMsg.Timestamp >= timestamp {
		return ms.first, nil
	}
	if ms.lastMsg != nil && timestamp >= ms.lastMsg.Timestamp {
		return ms.last + 1, nil
	}

	smallest := int64(-1)
	// This will require disk access.
	for _, slice := range ms.files {
		if err := ms.lockIndexFile(slice); err != nil {
			return 0, err
		}
		mindex := ms.getMsgIndex(slice, slice.firstSeq)
		if timestamp >= mindex.timestamp {
			mindex = ms.getMsgIndex(slice, slice.lastSeq)
			if timestamp <= mindex.timestamp {
				// Could do binary search, but will be probably more efficient
				// to do sequential disk reads. The index records are small,
				// so read of a record will probably bring many consecutive ones
				// in the system's disk cache, resulting in memory-only access
				// for the following indexes...
				for seq := slice.firstSeq + 1; seq < slice.lastSeq; seq++ {
					mindex = ms.getMsgIndex(slice, seq)
					if mindex.timestamp >= timestamp {
						ms.unlockIndexFile(slice)
						return seq, nil
					}
				}
			}
		} else if smallest == -1 || mindex.timestamp < smallest {
			smallest = mindex.timestamp
		}
		ms.unlockIndexFile(slice)
	}
	if timestamp < smallest {
		return ms.first, nil
	}
	return ms.last + 1, nil
}

// initCache initializes the message cache
func (ms *FileMsgStore) initCache() {
	ms.cache = &msgsCache{
		seqMaps: make(map[uint64]*cachedMsg),
	}
}

// add adds a message to the cache.
// Store write lock is assumed held on entry
func (c *msgsCache) add(seq uint64, msg *pb.MsgProto, isNew bool) {
	exp := cacheTTL
	if isNew {
		exp += msg.Timestamp
	} else {
		exp += time.Now().UnixNano()
	}
	cMsg := &cachedMsg{
		expiration: exp,
		msg:        msg,
	}
	if c.tail == nil {
		c.head = cMsg
	} else {
		c.tail.next = cMsg
		// Ensure last expiration is at least >= previous one.
		if cMsg.expiration < c.tail.expiration {
			cMsg.expiration = c.tail.expiration
		}
	}
	cMsg.prev = c.tail
	c.tail = cMsg
	c.seqMaps[seq] = cMsg
	if len(c.seqMaps) == 1 {
		atomic.StoreInt32(&c.tryEvict, 1)
	}
}

// get returns a message if available in the cache.
// Store write lock is assumed held on entry
func (c *msgsCache) get(seq uint64) *pb.MsgProto {
	cMsg := c.seqMaps[seq]
	if cMsg == nil {
		return nil
	}
	// Bump the expiration
	cMsg.expiration = time.Now().UnixNano() + cacheTTL
	// If not already at the tail of the list, move it there
	if cMsg != c.tail {
		if cMsg.prev != nil {
			cMsg.prev.next = cMsg.next
		}
		if cMsg.next != nil {
			cMsg.next.prev = cMsg.prev
		}
		if cMsg == c.head {
			c.head = cMsg.next
		}
		cMsg.prev = c.tail
		c.tail.next = cMsg
		cMsg.next = nil
		// Ensure last expiration is at least >= previous one.
		if cMsg.expiration < c.tail.expiration {
			cMsg.expiration = c.tail.expiration
		}
		c.tail = cMsg
	}
	return cMsg.msg
}

// evict move down the cache maps, evicting the last one.
// Store write lock is assumed held on entry
func (c *msgsCache) evict(now int64) {
	if c.head == nil {
		return
	}
	if now >= c.tail.expiration {
		// Bulk remove
		c.seqMaps = make(map[uint64]*cachedMsg)
		c.head, c.tail, c.tryEvict = nil, nil, 0
		return
	}
	cMsg := c.head
	for cMsg != nil && cMsg.expiration <= now {
		delete(c.seqMaps, cMsg.msg.Sequence)
		cMsg = cMsg.next
	}
	if cMsg != c.head {
		// There should be at least one left, otherwise, they
		// would all have been bulk removed at top of this function.
		cMsg.prev = nil
		c.head = cMsg
	}
}

// empty empties the cache
func (c *msgsCache) empty() {
	atomic.StoreInt32(&c.tryEvict, 0)
	c.head, c.tail = nil, nil
	c.seqMaps = make(map[uint64]*cachedMsg)
}

// Close closes the store.
func (ms *FileMsgStore) Close() error {
	ms.Lock()
	if ms.closed {
		ms.Unlock()
		return nil
	}

	ms.closed = true

	// Signal the background tasks go-routine to exit
	ms.bkgTasksDone <- true

	ms.Unlock()

	// Wait on go routines/timers to finish
	ms.allDone.Wait()

	ms.Lock()
	var err error
	if ms.writeSlice != nil {
		// Flush current file slice where writes happen
		ms.lockFiles(ms.writeSlice)
		err = ms.flush(ms.writeSlice)
		ms.unlockFiles(ms.writeSlice)
	}
	// Remove/close all file slices
	for _, slice := range ms.files {
		ms.fm.remove(slice.file)
		ms.fm.remove(slice.idxFile)
		if slice.file.handle != nil {
			err = util.CloseFile(err, slice.file.handle)
		}
		if slice.idxFile.handle != nil {
			err = util.CloseFile(err, slice.idxFile.handle)
		}
	}
	ms.Unlock()

	return err
}

func (ms *FileMsgStore) flush(fslice *fileSlice) error {
	if ms.bw != nil && ms.bw.buf != nil && ms.bw.buf.Buffered() > 0 {
		if err := ms.bw.buf.Flush(); err != nil {
			return err
		}
	}
	// This used to be inside the above `if` statement, but now it has
	// to be separate because the data file may have been closed
	// (and therefore the buffer flushed) and we could still have
	// buffered messages that need to be processed.
	if len(ms.bufferedMsgs) > 0 {
		if err := ms.processBufferedMsgs(fslice); err != nil {
			return err
		}
	}
	if ms.fstore.opts.DoSync {
		if err := fslice.file.handle.Sync(); err != nil {
			return err
		}
		if err := fslice.idxFile.handle.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes outstanding data into the store.
func (ms *FileMsgStore) Flush() error {
	ms.Lock()
	var err error
	if ms.writeSlice != nil {
		err = ms.lockFiles(ms.writeSlice)
		if err == nil {
			err = ms.flush(ms.writeSlice)
			ms.unlockFiles(ms.writeSlice)
		}
	}
	ms.Unlock()
	return err
}

// Empty implements the MsgStore interface
func (ms *FileMsgStore) Empty() error {
	ms.Lock()
	defer ms.Unlock()

	var err error
	// Remove/close all file slices
	for sliceID, slice := range ms.files {
		ms.fm.remove(slice.file)
		ms.fm.remove(slice.idxFile)
		if slice.file.handle != nil {
			err = util.CloseFile(err, slice.file.handle)
		}
		if lerr := os.Remove(slice.file.name); lerr != nil && err == nil {
			err = lerr
		}
		if slice.idxFile.handle != nil {
			err = util.CloseFile(err, slice.idxFile.handle)
		}
		if lerr := os.Remove(slice.idxFile.name); lerr != nil && err == nil {
			err = lerr
		}
		delete(ms.files, sliceID)
	}
	// Reset generic counters
	ms.empty()
	// FileMsgStore specific
	ms.writer = nil
	ms.writeSlice = nil
	ms.cache.empty()
	ms.wOffset = 0
	ms.firstMsg, ms.lastMsg = nil, nil
	ms.expiration = 0
	ms.firstFSlSeq, ms.lastFSlSeq = 0, 0
	// If we are running in buffered mode...
	if ms.bw != nil {
		ms.bw = newBufferWriter(msgBufMinShrinkSize, ms.fstore.opts.BufferSize)
		ms.bufferedSeqs = make([]uint64, 0, 1)
		ms.bufferedMsgs = make(map[uint64]*bufferedMsg)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// FileSubStore methods
////////////////////////////////////////////////////////////////////////////

// newFileSubStore returns a new instace of a file SubStore.
func (fs *FileStore) newFileSubStore(channel string, limits *SubStoreLimits, doRecover bool) (*FileSubStore, error) {
	ss := &FileSubStore{
		fstore:   fs,
		fm:       fs.fm,
		opts:     &fs.opts,
		crcTable: fs.crcTable,
	}
	ss.init(fs.log, limits)
	// Convert the CompactInterval in time.Duration
	ss.compactItvl = time.Duration(ss.opts.CompactInterval) * time.Second

	var err error

	fileName := filepath.Join(channel, subsFileName)
	ss.file, err = fs.fm.createFile(fileName, defaultFileFlags, func() error {
		ss.writer = nil
		return ss.flush()
	})
	if err != nil {
		return nil, err
	}
	maxBufSize := ss.opts.BufferSize
	ss.writer = ss.file.handle
	// If we allow buffering, then create the buffered writer and
	// set ss's writer to that buffer.
	if maxBufSize > 0 {
		ss.bw = newBufferWriter(subBufMinShrinkSize, maxBufSize)
		ss.writer = ss.bw.createNewWriter(ss.file.handle)
	}
	if doRecover {
		if err := ss.recoverSubscriptions(); err != nil {
			fs.fm.unlockFile(ss.file)
			ss.Close()
			return nil, fmt.Errorf("unable to recover subscription store for [%s]: %v", channel, err)
		}
	}
	// Do not attempt to shrink unless the option is greater than the
	// minimum shrinkable size.
	if maxBufSize > subBufMinShrinkSize {
		// Use lock to avoid RACE report between setting shrinkTimer and
		// execution of the callback itself.
		ss.Lock()
		ss.allDone.Add(1)
		ss.shrinkTimer = time.AfterFunc(bufShrinkInterval, func() {
			ss.shrinkBuffer(true)
		})
		ss.Unlock()
	}
	if doRecover {
		fs.fm.closeLockedFile(ss.file)
	} else {
		fs.fm.unlockFile(ss.file)
	}
	return ss, nil
}

// getFile ensures that the store's file handle is valid, opening
// the file if needed. If file needs to be opened, the store's writer
// is set to either the bare file or the buffered writer (based on
// store's configuration).
func (ss *FileSubStore) lockFile() error {
	wasOpened, err := ss.fm.lockFile(ss.file)
	if err != nil {
		return err
	}
	// If file was not opened, we need to reset ss.writer
	if !wasOpened {
		if ss.bw != nil {
			ss.writer = ss.bw.createNewWriter(ss.file.handle)
		} else {
			ss.writer = ss.file.handle
		}
	}
	return nil
}

// shrinkBuffer is a timer callback that shrinks the buffer writer when possible.
// Since this function is called directly in tests, the boolean `fromTimer` is
// used to indicate if this function is invoked from the timer callback (in which
// case, the timer need to be Reset()) or not. Reseting a timer while timer fires
// can lead to unexpected behavior.
func (ss *FileSubStore) shrinkBuffer(fromTimer bool) {
	ss.Lock()
	defer ss.Unlock()

	if ss.closed {
		ss.allDone.Done()
		return
	}
	// Fire again
	if fromTimer {
		ss.shrinkTimer.Reset(bufShrinkInterval)
	}

	// If file currently opened, lock it, otherwise we are done for now.
	if !ss.fm.lockFileIfOpened(ss.file) {
		return
	}
	// If error, the buffer (in bufio) memorizes the error
	// so any other write/flush on that buffer will fail. We will get the
	// error at the next "synchronous" operation where we can report back
	// to the user.
	ss.writer, _ = ss.bw.tryShrinkBuffer(ss.file.handle)
	ss.fm.unlockFile(ss.file)
}

// recoverSubscriptions recovers subscriptions state for this store.
func (ss *FileSubStore) recoverSubscriptions() error {
	var err error
	var recType recordType

	recSize := 0
	offset := int64(4)

	// Create a buffered reader to speed-up recovery
	br := bufio.NewReaderSize(ss.file.handle, defaultBufSize)

	for {
		ss.tmpSubBuf, recSize, recType, err = readRecord(br, ss.tmpSubBuf, true, ss.crcTable, ss.opts.DoCRC)
		if err != nil {
			switch err {
			case io.EOF:
				// We are done, reset err
				err = nil
			case errNeedRewind:
				err = ss.fm.truncateFile(ss.file, offset)
			default:
				err = ss.fstore.handleUnexpectedEOF(err, ss.file, offset, true)
			}
			if err == nil {
				break
			}
			return err
		}
		readBytes := int64(recSize + recordHeaderSize)
		offset += readBytes
		ss.fileSize += readBytes
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
			// Keep track of max subscription ID found.
			if newSub.ID > ss.maxSubID {
				ss.maxSubID = newSub.ID
			}
			ss.numRecs++
		case subRecUpdate:
			modifiedSub := &spb.SubState{}
			if err := modifiedSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			// Search if the create has been recovered.
			subi, exists := ss.subs[modifiedSub.ID]
			if exists {
				sub := subi.(*subscription)
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
		case subRecDel:
			delSub := spb.SubStateDelete{}
			if err := delSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if si, exists := ss.subs[delSub.ID]; exists {
				s := si.(*subscription)
				delete(ss.subs, delSub.ID)
				// Delete and count all non-ack'ed messages free space.
				ss.delRecs++
				ss.delRecs += len(s.seqnos)
			}
			// Keep track of max subscription ID found.
			if delSub.ID > ss.maxSubID {
				ss.maxSubID = delSub.ID
			}
		case subRecMsg:
			updateSub := spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if subi, exists := ss.subs[updateSub.ID]; exists {
				sub := subi.(*subscription)
				seqno := updateSub.Seqno
				// Same seqno/ack can appear several times for the same sub.
				// See queue subscribers redelivery.
				if seqno > sub.sub.LastSent {
					sub.sub.LastSent = seqno
				}
				sub.seqnos[seqno] = struct{}{}
				ss.numRecs++
			}
		case subRecAck:
			updateSub := spb.SubStateUpdate{}
			if err := updateSub.Unmarshal(ss.tmpSubBuf[:recSize]); err != nil {
				return err
			}
			if subi, exists := ss.subs[updateSub.ID]; exists {
				sub := subi.(*subscription)
				delete(sub.seqnos, updateSub.Seqno)
				// A message is ack'ed
				ss.delRecs++
			}
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
	if err := ss.writeRecord(nil, subRecNew, sub); err != nil {
		delete(ss.subs, sub.ID)
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
	if err := ss.writeRecord(nil, subRecUpdate, sub); err != nil {
		return err
	}
	// We need to get a copy of the passed sub, we can't hold a reference
	// to it.
	csub := *sub
	si := ss.subs[sub.ID]
	if si != nil {
		s := si.(*subscription)
		s.sub = &csub
	} else {
		s := &subscription{sub: &csub, seqnos: make(map[uint64]struct{})}
		ss.subs[sub.ID] = s
	}
	return nil
}

// DeleteSub invalidates this subscription.
func (ss *FileSubStore) DeleteSub(subid uint64) error {
	ss.Lock()
	ss.delSub.ID = subid
	err := ss.writeRecord(nil, subRecDel, &ss.delSub)
	// Even if there is an error, continue with cleanup. If later
	// a compact is successful, the sub won't be present in the compacted file.
	if si, exists := ss.subs[subid]; exists {
		s := si.(*subscription)
		delete(ss.subs, subid)
		// writeRecord has already accounted for the count of the
		// delete record. We add to this the number of pending messages
		ss.delRecs += len(s.seqnos)
		// Check if this triggers a need for compaction
		if ss.shouldCompact() {
			ss.fm.closeFileIfOpened(ss.file)
			ss.compact(ss.file.name)
		}
	}
	ss.Unlock()
	return err
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
	if time.Since(ss.compactTS) < ss.compactItvl {
		return false
	}
	return true
}

// AddSeqPending adds the given message seqno to the given subscription.
func (ss *FileSubStore) AddSeqPending(subid, seqno uint64) error {
	ss.Lock()
	ss.updateSub.ID, ss.updateSub.Seqno = subid, seqno
	if err := ss.writeRecord(nil, subRecMsg, &ss.updateSub); err != nil {
		ss.Unlock()
		return err
	}
	si := ss.subs[subid]
	if si != nil {
		s := si.(*subscription)
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
	if err := ss.writeRecord(nil, subRecAck, &ss.updateSub); err != nil {
		ss.Unlock()
		return err
	}
	si := ss.subs[subid]
	if si != nil {
		s := si.(*subscription)
		delete(s.seqnos, seqno)
		// Test if we should compact
		if ss.shouldCompact() {
			ss.fm.closeFileIfOpened(ss.file)
			ss.compact(ss.file.name)
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
func (ss *FileSubStore) compact(orgFileName string) error {
	tmpFile, err := getTempFile(ss.fm.rootDir, "subs")
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
	for _, subi := range ss.subs {
		sub := subi.(*subscription)
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
	// Start by closing the temporary file.
	if err := tmpFile.Close(); err != nil {
		return err
	}
	// Rename the tmp file to original file name
	if err := os.Rename(tmpFile.Name(), orgFileName); err != nil {
		return err
	}
	// Prevent cleanup on success
	tmpFile = nil
	// Update the timestamp of this last successful compact
	ss.compactTS = time.Now()
	return nil
}

// writes a record in the subscriptions file.
// store's lock is held on entry.
func (ss *FileSubStore) writeRecord(w io.Writer, recType recordType, rec record) error {
	var err error
	totalSize := 0
	recSize := rec.Size()

	var bwBuf *bufio.Writer
	needsUnlock := false

	if w == nil {
		if err := ss.lockFile(); err != nil {
			return err
		}
		needsUnlock = true
		if ss.bw != nil {
			bwBuf = ss.bw.buf
			// If we are using the buffer writer on this call, and the buffer is
			// not already at the max size...
			if bwBuf != nil && ss.bw.bufSize != ss.opts.BufferSize {
				// Check if record fits
				required := recSize + recordHeaderSize
				if required > bwBuf.Available() {
					ss.writer, err = ss.bw.expand(ss.file.handle, required)
					if err != nil {
						ss.fm.unlockFile(ss.file)
						return err
					}
					bwBuf = ss.bw.buf
				}
			}
		}
		w = ss.writer
	}
	ss.tmpSubBuf, totalSize, err = writeRecord(w, ss.tmpSubBuf, recType, rec, recSize, ss.crcTable)
	if err != nil {
		if needsUnlock {
			ss.fm.unlockFile(ss.file)
		}
		return err
	}
	if bwBuf != nil && ss.bw.shrinkReq {
		ss.bw.checkShrinkRequest()
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
	if needsUnlock {
		ss.fm.unlockFile(ss.file)
	}
	return nil
}

func (ss *FileSubStore) flush() error {
	// Skip this if nothing was written since the last flush
	if !ss.activity {
		return nil
	}
	// Reset this now
	ss.activity = false
	if ss.bw != nil && ss.bw.buf.Buffered() > 0 {
		if err := ss.bw.buf.Flush(); err != nil {
			return err
		}
	}
	if ss.opts.DoSync {
		return ss.file.handle.Sync()
	}
	return nil
}

// Flush persists buffered operations to disk.
func (ss *FileSubStore) Flush() error {
	ss.Lock()
	err := ss.lockFile()
	if err == nil {
		err = ss.flush()
		ss.fm.unlockFile(ss.file)
	}
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

	ss.Lock()
	var err error
	if ss.fm.remove(ss.file) {
		if ss.file.handle != nil {
			err = ss.flush()
			err = util.CloseFile(err, ss.file.handle)
		}
	}
	ss.Unlock()

	return err
}
