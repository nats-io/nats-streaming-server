// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"bufio"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

var testDefaultServerInfo = spb.ServerInfo{
	ClusterID:   "id",
	Discovery:   "discovery",
	Publish:     "publish",
	Subscribe:   "subscribe",
	Unsubscribe: "unsubscribe",
	Close:       "close",
}

var defaultDataStore string
var disableBufferWriters bool
var setFDsLimit bool

var testFDsLimit = int64(5)

func init() {
	tmpDir, err := ioutil.TempDir(".", "data_stores_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	defaultDataStore = tmpDir
}

func TestMain(m *testing.M) {
	flag.BoolVar(&disableBufferWriters, "no_buffer", false, "Disable use of buffer writers")
	flag.BoolVar(&setFDsLimit, "set_fds_limit", false, "Set some FDs limit")
	flag.Parse()
	os.Exit(m.Run())
}

func TestFSFilesManager(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	if err := os.MkdirAll(defaultDataStore, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		t.Fatalf("Unable to create the root directory [%s]: %v", defaultDataStore, err)
	}

	fm := createFilesManager(defaultDataStore, 4)
	defer fm.close()
	// This should fail since file does not exist
	failedFile, err := fm.createFile("foo", os.O_RDONLY, nil)
	if err == nil {
		t.Fatal("Creating file should have failed")
	}
	if failedFile != nil {
		t.Fatalf("On error, file should be nil, got %#v", failedFile)
	}
	// Ensure not in fm's map
	fm.Lock()
	lenMap := len(fm.files)
	fm.Unlock()
	if lenMap > 0 {
		t.Fatalf("Map should be empty, got %v", lenMap)
	}
	closeCbCalled := 0
	bccb := func() error {
		closeCbCalled++
		return nil
	}
	firstFile, err := fm.createFile("foo", defaultFileFlags, bccb)
	if err != nil {
		t.Fatalf("Unexpected error on create: %v", err)
	}
	if firstFile.id == invalidFileID {
		t.Fatal("Got invalid file ID on success")
	}
	if firstFile == nil {
		t.Fatal("Got nil file on success")
	}
	// Check content
	// The callback cannot be checked, we need to temporarly set to nil
	firstFile.beforeClose = nil
	expectedFile := file{
		id:     fileID(1),
		handle: firstFile.handle, // we cannot know what to expect here, so use value returned
		name:   filepath.Join(defaultDataStore, "foo"),
		flags:  defaultFileFlags,
		state:  fileInUse,
	}
	fobj := *firstFile
	if !reflect.DeepEqual(expectedFile, fobj) {
		t.Fatalf("Expected file to be %#v, got %#v", expectedFile, fobj)
	}
	firstFile.beforeClose = bccb
	// unlock the file
	fm.unlockFile(firstFile)
	// Create more files
	moreFiles := make([]*file, 10)
	for i := 0; i < len(moreFiles); i++ {
		fname := fmt.Sprintf("foo.%d", (i + 1))
		f, err := fm.createFile(fname, defaultFileFlags, nil)
		if err != nil {
			t.Fatalf("Error on file create: %q - %v", fname, err)
		}
		moreFiles[i] = f
		fm.unlockFile(f)
	}
	// Check the number of opened files.
	fm.Lock()
	opened := fm.openedFDs
	fm.Unlock()
	if opened > fm.limit {
		t.Fatalf("Expected up to %v opened files, got %v", fm.limit, opened)
	}
	// Verify that number is accurate
	actualOpened := int64(0)
	fm.Lock()
	for _, file := range fm.files {
		if file.state == fileOpened && file.handle != nil {
			actualOpened++
		}
	}
	fm.Unlock()
	if actualOpened != opened {
		t.Fatalf("FM's opened is %v, but actual number of files opened is %v", opened, actualOpened)
	}
	// Lock all files, which should cause the opened count to go over limit
	for _, file := range moreFiles {
		fm.lockFile(file)
	}
	fm.Lock()
	opened = fm.openedFDs
	fm.Unlock()
	if opened != int64(len(moreFiles)) {
		t.Fatalf("Expected opened to be %v, got %v", len(moreFiles), opened)
	}
	// Unlock the files now
	for _, file := range moreFiles {
		fm.unlockFile(file)
	}
	// We should be able to lock them back and they should still be opened
	for _, file := range moreFiles {
		if !fm.lockFileIfOpened(file) {
			t.Fatalf("LockIfOpened for file %q should not have failed", file.name)
		}
		// This would panic if the file is not locked
		if err := fm.closeLockedFile(file); err != nil {
			t.Fatalf("Unexpected error on close: %v", err)
		}
		// This should do nothing and not return an error
		if err := fm.closeFileIfOpened(file); err != nil {
			t.Fatalf("Unexpected error on closeIfOpened: %v", err)
		}
	}
	// Open them all again and closed if opened
	for _, file := range moreFiles {
		if _, err := fm.lockFile(file); err != nil {
			t.Fatalf("Unexpected error opening file %q: %v", file.name, err)
		}
		fm.unlockFile(file)
		if err := fm.closeFileIfOpened(file); err != nil {
			t.Fatalf("Unexpected error closing file %q that was opened: %v", file.name, err)
		}
	}
	// Remove all the files in moreFiles
	for _, file := range moreFiles {
		if !fm.remove(file) {
			t.Fatalf("Should have been able to remove file %q", file.name)
		}
	}
	// Try to remove a file already removed
	if fm.remove(moreFiles[0]) {
		t.Fatalf("Should have failed to remove file %q", moreFiles[0].name)
	}

	// At this point, there should be no file opened
	fm.Lock()
	opened = fm.openedFDs
	fm.Unlock()
	if opened != 0 {
		t.Fatalf("There should be no file opened, got %v", opened)
	}

	// Open our first file
	fm.lockFile(firstFile)
	// Unlock now, it should stay opened
	fm.unlockFile(firstFile)
	// Create a file that will be left locked when calling close
	lockedFile, err := fm.createFile("bar", defaultFileFlags, nil)
	if err != nil {
		t.Fatalf("Unexpected error on file create: %v", err)
	}
	// Close the manager, which should close this file
	if err := fm.close(); err == nil || !strings.Contains(err.Error(), "still probably locked") {
		t.Fatalf("Unexpected error on FM close: %v", err)
	}
	if closeCbCalled != 2 {
		t.Fatal("Expected closeCbCalled to not be 0")
	}
	// Ensure that closing a second time is fine
	if err := fm.close(); err != nil {
		t.Fatalf("Unexpected error on double close: %v", err)
	}
	// Check file `f` is closed
	if firstFile.handle != nil || firstFile.state != fileClosed {
		t.Fatalf("File %q should be closed: handle=%v state=%v", firstFile.name, firstFile.handle, firstFile.state)
	}
	// Close the lockedFile
	if err := fm.closeLockedFile(lockedFile); err != nil {
		t.Fatalf("Error closing locked file: %v", err)
	}
	// Verify that we can no longer create or open files
	if fmClosedFile, err := fm.createFile("baz", defaultFileFlags, nil); err == nil || !strings.Contains(err.Error(), "is being closed") {
		fm.closeLockedFile(fmClosedFile)
		t.Fatalf("Creating file should have failed after FM was closed, got %v", err)
	}
	if _, err := fm.lockFile(lockedFile); err == nil || !strings.Contains(err.Error(), "is being closed") {
		fm.closeLockedFile(lockedFile)
		t.Fatalf("Should not be able to lock a file after FM was closed, got %v", err)
	}

	// Recreate a file manager
	fm = createFilesManager(defaultDataStore, 0)
	defer fm.close()

	closeCbCalled = 0
	testcb, err := fm.createFile("testcb", defaultFileFlags, bccb)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if err := fm.closeLockedFile(testcb); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}
	if closeCbCalled != 1 {
		t.Fatalf("Expected callback to be invoked once, got %v", closeCbCalled)
	}
	fm.setBeforeCloseCb(testcb, nil)
	if _, err := fm.lockFile(testcb); err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	if err := fm.closeLockedFile(testcb); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}
	if closeCbCalled != 1 {
		t.Fatalf("Expected callback to be invoked once, got %v", closeCbCalled)
	}
	if !fm.remove(testcb) {
		t.Fatal("Should have been able to remove file")
	}

	testcloseOrOpenedFile, err := fm.createFile("testcloselockedoropened", defaultFileFlags, nil)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if err := fm.closeLockedOrOpenedFile(testcloseOrOpenedFile); err != nil {
		t.Fatalf("Error closing file: %v", err)
	}
	if _, err := fm.lockFile(testcloseOrOpenedFile); err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	// simply unlock (which will leave it opened)
	fm.unlockFile(testcloseOrOpenedFile)
	if err := fm.closeLockedOrOpenedFile(testcloseOrOpenedFile); err != nil {
		t.Fatalf("Error closing file: %v", err)
	}
	if !fm.remove(testcloseOrOpenedFile) {
		t.Fatal("Should have been able to remove file")
	}

	// Following tests are supposed to produce panic
	file, err := fm.createFile("failure", defaultFileFlags, nil)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	lockLockedFile := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Locking a locked file should panic")
			}
		}()
		fm.lockFile(file)
	}
	lockLockedFile()
	if err := fm.closeLockedFile(file); err != nil {
		t.Fatalf("Error closing locked file: %v", err)
	}
	fm.lockFile(file)
	fm.unlockFile(file)
	closeLockedFile := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("closeLockedFile should panic if file is not locked")
			}
		}()
		fm.closeLockedFile(file)
	}
	closeLockedFile()
	unlockUnlockedFile := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Unlocking an unlocked file should panic")
			}
		}()
		fm.unlockFile(file)
	}
	unlockUnlockedFile()
	if !fm.remove(file) {
		t.Fatal("File should have been removed")
	}
	lockRemovedFile := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Locking a removed file should panic")
			}
		}()
		fm.lockFile(file)
	}
	lockRemovedFile()
	fm.close()
	fm.Lock()
	lenMap = len(fm.files)
	fm.Unlock()
	if lenMap != 0 {
		t.Fatalf("Map should be empty, got %v", lenMap)
	}
	// Close file to avoid issue when cleaning up datastore on Windows.
	file.handle.Close()
}

func cleanupDatastore(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		stackFatalf(t, "Error cleaning up datastore: %v", err)
	}
}

func newFileStore(t *testing.T, dataStore string, limits *StoreLimits, options ...FileStoreOption) (*FileStore, *RecoveredState, error) {
	opts := DefaultFileStoreOptions
	// Set those options based on command line parameters.
	// Each test may override those.
	if disableBufferWriters {
		opts.BufferSize = 0
	}
	if setFDsLimit {
		opts.FileDescriptorsLimit = testFDsLimit
	}
	// Apply the provided options
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, nil, err
		}
	}
	fs, err := NewFileStore(dataStore, limits, AllOptions(&opts))
	if err != nil {
		return nil, nil, err
	}
	state, err := fs.Recover()
	if err != nil {
		return nil, nil, err
	}
	return fs, state, nil
}

func createDefaultFileStore(t *testing.T, options ...FileStoreOption) *FileStore {
	limits := testDefaultStoreLimits
	fs, state, err := newFileStore(t, defaultDataStore, &limits, options...)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs
}

func openDefaultFileStore(t *testing.T, options ...FileStoreOption) (*FileStore, *RecoveredState) {
	limits := testDefaultStoreLimits
	fs, state, err := newFileStore(t, defaultDataStore, &limits, options...)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	return fs, state
}

func expectedErrorOpeningDefaultFileStore(t *testing.T) error {
	limits := testDefaultStoreLimits
	fs, _, err := newFileStore(t, defaultDataStore, &limits)
	if err == nil {
		fs.Close()
		stackFatalf(t, "Expected an error opening the FileStore, got none")
	}
	return err
}

func TestFSBasicCreate(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicCreate(t, fs, TypeFile)
}

func TestFSNoDirectoryError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs, err := NewFileStore("", nil)
	if err == nil || !strings.Contains(err.Error(), "specified") {
		if fs != nil {
			fs.Close()
		}
		t.Fatalf("Expected error about missing root directory, got: %v", err)
	}
}

func TestFSInit(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Init is done in createDefaultFileStore().
	// A second call to Init() should not fail, and data should be replaced.
	newInfo := testDefaultServerInfo
	newInfo.ClusterID = "newID"
	if err := fs.Init(&newInfo); err != nil {
		t.Fatalf("Unexpected failure on store init: %v", err)
	}

	// Close the store
	fs.Close()

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	// Check content
	info := *state.Info
	if !reflect.DeepEqual(newInfo, info) {
		t.Fatalf("Unexpected server info, expected %v, got %v",
			newInfo, info)
	}
}

func TestFSUseDefaultLimits(t *testing.T) {
	fs, _, err := newFileStore(t, defaultDataStore, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()
	if !reflect.DeepEqual(fs.limits, DefaultStoreLimits) {
		t.Fatalf("Default limits are not used: %v\n", fs.limits)
	}
}

func TestFSUnsupportedFileVersion(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()
	storeMsg(t, fs, "foo", []byte("test"))
	storeSub(t, fs, "foo")

	// Close store
	fs.Close()

	// Overwrite the file version of a message store to an unsupported version
	writeVersion(t, filepath.Join(defaultDataStore, "foo", msgFilesPrefix+"1"+datSuffix), fileVersion+1)

	// Recover store (should fail)
	err := expectedErrorOpeningDefaultFileStore(t)
	fileVerStr := fmt.Sprintf("%d", (fileVersion + 1))
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}

	// Restore the correct version.
	writeVersion(t, filepath.Join(defaultDataStore, "foo", msgFilesPrefix+"1"+datSuffix), fileVersion)

	// Overwrite the file version of the subscriptions store to an unsupported version
	writeVersion(t, filepath.Join(defaultDataStore, "foo", subsFileName), fileVersion+1)

	// Recover store (should fail)
	err = expectedErrorOpeningDefaultFileStore(t)
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}
}

func writeVersion(t *testing.T, fileName string, version int) {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := util.WriteInt(file, version); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestFSOptions(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Check that default options are used
	fs.RLock()
	opts := fs.opts
	fs.RUnlock()

	checkOpts := func(expected, actual FileStoreOptions) {
		if !reflect.DeepEqual(actual, expected) {
			stackFatalf(t, "Expected options to be %v, got %v", expected, actual)
		}
	}
	expected := DefaultFileStoreOptions
	if disableBufferWriters {
		expected.BufferSize = 0
	}
	if setFDsLimit {
		expected.FileDescriptorsLimit = testFDsLimit
	}
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs := fs.LookupChannel("foo")
	ss := cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	// Now try to set the options in the constructor
	fs.Close()
	cleanupDatastore(t, defaultDataStore)

	// Prepare the golden options with custom values
	expected = FileStoreOptions{
		BufferSize:           1025 * 1024,
		CompactEnabled:       false,
		CompactFragmentation: 60,
		CompactInterval:      60,
		CompactMinFileSize:   1024 * 1024,
		DoCRC:                false,
		CRCPolynomial:        int64(crc32.Castagnoli),
		DoSync:               false,
		SliceMaxMsgs:         100,
		SliceMaxBytes:        1024 * 1024,
		SliceMaxAge:          time.Second,
		SliceArchiveScript:   "myscript.sh",
		FileDescriptorsLimit: 20,
	}
	// Create the file with custom options
	fs, err := NewFileStore(defaultDataStore, &testDefaultStoreLimits,
		BufferSize(expected.BufferSize),
		CompactEnabled(expected.CompactEnabled),
		CompactFragmentation(expected.CompactFragmentation),
		CompactInterval(expected.CompactInterval),
		CompactMinFileSize(expected.CompactMinFileSize),
		DoCRC(expected.DoCRC),
		CRCPolynomial(expected.CRCPolynomial),
		DoSync(expected.DoSync),
		SliceConfig(100, 1024*1024, time.Second, "myscript.sh"),
		FileDescriptorsLimit(20))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	fs.Close()
	cleanupDatastore(t, defaultDataStore)
	// Create the file with custom options, pass all of them at once
	fs, err = NewFileStore(defaultDataStore, &testDefaultStoreLimits, AllOptions(&expected))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	if _, err := fs.Recover(); err != nil {
		t.Fatalf("Error recovering state: %v", err)
	}
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	fs.CreateChannel("foo", nil)
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)
}

func TestFSNothingRecoveredOnFreshStart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testNothingRecoveredOnFreshStart(t, fs)
}

func TestFSNewChannel(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testNewChannel(t, fs)
}

func TestFSCloseIdempotent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testCloseIdempotent(t, fs)
}

func TestFSBasicMsgStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicMsgStore(t, fs)
}

func TestFSBasicRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fooRecovered := false
	barRecovered := false

	fs := createDefaultFileStore(t)
	defer fs.Close()

	if fs.LookupChannel("foo") != nil {
		fooRecovered = true
	}
	if fs.LookupChannel("bar") != nil {
		barRecovered = true
	}

	// Nothing should be recovered
	if fooRecovered || barRecovered {
		t.Fatalf("Unexpected recovery: foo=%v bar=%v", fooRecovered, barRecovered)
	}

	foo1 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo2 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo3 := storeMsg(t, fs, "foo", []byte("foomsg"))

	bar1 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar2 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar3 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar4 := storeMsg(t, fs, "bar", []byte("barmsg"))

	sub1 := storeSub(t, fs, "foo")
	sub2 := storeSub(t, fs, "bar")

	storeSubPending(t, fs, "foo", sub1, foo1.Sequence, foo2.Sequence, foo3.Sequence)
	storeSubAck(t, fs, "foo", sub1, foo1.Sequence, foo3.Sequence)

	storeSubPending(t, fs, "bar", sub2, bar1.Sequence, bar2.Sequence, bar3.Sequence, bar4.Sequence)
	storeSubAck(t, fs, "bar", sub2, bar4.Sequence)

	fs.Close()

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	// Check that subscriptions are restored
	for channel, recoveredSubs := range subs {
		if len(recoveredSubs) != 1 {
			t.Fatalf("Incorrect size of recovered subs. Expected 1, got %v ", len(recoveredSubs))
		}
		recSub := recoveredSubs[0]
		subID := recSub.Sub.ID

		switch channel {
		case "foo":
			if subID != sub1 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub1, subID)
			}
			for seq := range recSub.Pending {
				if seq != foo2.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub1: %v", seq)
				}
			}
		case "bar":
			if subID != sub2 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub2, subID)
			}
			for seq := range recSub.Pending {
				if seq != bar1.Sequence && seq != bar2.Sequence && seq != bar3.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub2: %v", seq)
				}
			}
		default:
			t.Fatalf("Recovered unknown channel: %v", channel)
		}
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	// In message store, the first message should still be foo1,
	// regardless of what has been consumed.
	m := cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != foo1.Sequence {
		t.Fatalf("Unexpected message for foo channel: %v", m)
	}
	// Check that messages recovered from MsgStore are never
	// marked as redelivered.
	checkRedelivered := func(ms MsgStore) bool {
		start, end := ms.FirstAndLastSequence()
		for i := start; i <= end; i++ {
			if m := ms.Lookup(i); m != nil && m.Redelivered {
				return true
			}
		}
		return false
	}
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
	}

	cs = fs.LookupChannel("bar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	// In message store, the first message should still be bar1,
	// regardless of what has been consumed.
	m = cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != bar1.Sequence {
		t.Fatalf("Unexpected message for bar channel: %v", m)
	}
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
	}

	cs = fs.LookupChannel("baz")
	if cs != nil {
		t.Fatal("Expected to get nil channel for baz, got something instead")
	}
}

func TestFSLimitsOnRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store some messages in various channels
	chanCount := 5
	msgCount := 50
	subsCount := 3
	payload := []byte("hello")
	expectedMsgBytes := uint64(0)
	maxMsgsAfterRecovery := 4
	expectedMsgBytesAfterRecovery := uint64(0)
	for c := 0; c < chanCount; c++ {
		channelName := fmt.Sprintf("channel.%d", (c + 1))

		// Create several subscriptions per channel.
		for s := 0; s < subsCount; s++ {
			storeSub(t, fs, channelName)
		}

		for m := 0; m < msgCount; m++ {
			msg := storeMsg(t, fs, channelName, payload)
			expectedMsgBytes += uint64(msg.Size())
			if c == 0 {
				if m < maxMsgsAfterRecovery {
					expectedMsgBytesAfterRecovery += uint64(msg.Size() + msgRecordOverhead)
				}
			}
		}
	}

	// Close the store
	fs.Close()

	// Now re-open with limits below all the above counts
	limit := testDefaultStoreLimits
	limit.MaxChannels = 1
	limit.MaxMsgs = maxMsgsAfterRecovery
	limit.MaxSubscriptions = 1
	fs, state, err := newFileStore(t, defaultDataStore, &limit, SliceConfig(1, int64(maxMsgsAfterRecovery), 0, ""))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	// Make sure that all our channels are recovered.
	if len(subs) != chanCount {
		t.Fatalf("Unexpected count of recovered channels. Expected %v, got %v", chanCount, len(state.Subs))
	}
	// Make sure that all our subscriptions are recovered.
	for _, recoveredSubs := range subs {
		if len(recoveredSubs) != subsCount {
			t.Fatalf("Unexpected count of recovered subs. Expected %v, got %v", subsCount, len(recoveredSubs))
		}
	}
	// Messages limits, however, are enforced on restart.
	recMsg, recBytes, err := fs.MsgsState(AllChannels)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != chanCount*maxMsgsAfterRecovery {
		t.Fatalf("Unexpected count of recovered msgs. Expected %v, got %v", chanCount*maxMsgsAfterRecovery, recMsg)
	}
	if recBytes != uint64(chanCount)*expectedMsgBytesAfterRecovery {
		t.Fatalf("Unexpected count of recovered bytes: Expected %v, got %v", uint64(chanCount)*expectedMsgBytesAfterRecovery, recBytes)
	}

	// Now check that any new addition would be rejected
	if _, _, err := fs.CreateChannel("new.channel", nil); err == nil {
		t.Fatal("Expected trying to create a new channel to fail")
	}
	channelOne := fs.LookupChannel("channel.1")
	if channelOne == nil {
		t.Fatal("Expected channel.1 to exist")
	}
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := channelOne.Subs.CreateSub(sub); err == nil {
		t.Fatal("Expected trying to create a new subscription to fail")
	}

	// Store one message
	lastMsg := storeMsg(t, fs, "channel.1", payload)

	// Check limits (should be 4 msgs)
	recMsg, recBytes, err = fs.MsgsState("channel.1")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != limit.MaxMsgs {
		t.Fatalf("Unexpected count of recovered msgs. Expected %v, got %v", limit.MaxMsgs, recMsg)
	}
	if recBytes != expectedMsgBytesAfterRecovery {
		t.Fatalf("Unexpected count of recovered bytes: Expected %v, got %v", expectedMsgBytes, recBytes)
	}

	cs := fs.channels["channel.1"]
	msgStore := cs.Msgs.(*FileMsgStore)

	// Check first avail message sequence
	expectedNewFirstSeq := uint64((msgCount + 1 - limit.MaxMsgs) + 1)
	if msgStore.first != expectedNewFirstSeq {
		t.Fatalf("Expected first sequence to be %v, got %v", expectedNewFirstSeq, msgStore.first)
	}
	// We should have moved to the second slice
	if msgStore.lastFSlSeq != 2 {
		t.Fatalf("Expected file slice to be the second one, got %v", msgStore.lastFSlSeq)
	}
	// Check second slice content
	secondSlice := msgStore.files[2]
	if secondSlice.msgsCount != 1 {
		t.Fatalf("Expected second slice to have 1 message, got %v", secondSlice.msgsCount)
	}
	if secondSlice.firstSeq != lastMsg.Sequence {
		t.Fatalf("Expected last message seq to be %v, got %v", lastMsg.Sequence, secondSlice.firstSeq)
	}
	// The first slice should have the new limit msgs count - 1.
	firstSlice := msgStore.files[1]
	left := firstSlice.msgsCount - firstSlice.rmCount
	if left != limit.MaxMsgs-1 {
		t.Fatalf("Expected first slice to have %v msgs, got %v", limit.MaxMsgs-1, left)
	}

	// Close the store
	fs.Close()

	// We are going to add an age limit (of 1ms for test purposes) and since
	// messages were stored before, if we wait say 5ms, no message should be
	// recovered.
	time.Sleep(5 * time.Millisecond)

	// Now re-open with limits below all the above counts
	limit = testDefaultStoreLimits
	limit.MaxMsgs = maxMsgsAfterRecovery
	limit.MaxAge = time.Millisecond
	fs, _, err = newFileStore(t, defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()
	recMsg, recBytes, err = fs.MsgsState(AllChannels)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if recMsg != 0 || recBytes != 0 {
		t.Fatalf("There should be no message recovered, got %v, %v bytes", recMsg, recBytes)
	}
}

func TestFSRecoveryFileSlices(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	storeMsg(t, fs, "foo", []byte("msg1"))
	storeMsg(t, fs, "foo", []byte("msg2"))

	// Close the store
	fs.Close()

	// Restart the store
	fs, state := openDefaultFileStore(t)
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to be recovered")
	}
	msgStore := cs.Msgs.(*FileMsgStore)

	// We should have moved to the second slice
	if msgStore.lastFSlSeq != 2 {
		t.Fatalf("Expected file slice to be the second one, got %v", msgStore.lastFSlSeq)
	}
}

func TestFSNoPanicAfterRestartWithSmallerLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	fs.Close()

	limit := testDefaultStoreLimits
	limit.MaxMsgs = 100
	fs, err := NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	msg := []byte("hello")
	for i := 0; i < 50; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	fs.Close()

	limit.MaxMsgs = 10
	fs, err = NewFileStore(defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()
	if _, err := fs.Recover(); err != nil {
		t.Fatalf("Unable to recover state: %v", err)
	}

	for i := 0; i < 10; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	cs := fs.LookupChannel("foo")
	first, last := cs.Msgs.FirstAndLastSequence()
	expectedFirst := uint64(51)
	expectedLast := uint64(60)
	if first != expectedFirst || last != expectedLast {
		t.Fatalf("Expected first/last to be %v/%v, got %v/%v",
			expectedFirst, expectedLast, first, last)
	}
}

func TestFSMsgsState(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testMsgsState(t, fs)
}

func TestFSMaxMsgs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testMaxMsgs(t, fs)
}

func TestFSMaxChannels(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limitCount := 2

	limits := testDefaultStoreLimits
	limits.MaxChannels = limitCount

	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	testMaxChannels(t, fs, limitCount)

	// Set the limit to 0
	limits.MaxChannels = 0
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	// Now try to test the limit against
	// any value, it should not fail
	testMaxChannels(t, fs, 0)
}

func TestFSMaxSubs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limitCount := 2

	limits := testDefaultStoreLimits
	limits.MaxSubscriptions = limitCount

	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	testMaxSubs(t, fs, "foo", limitCount)

	// Set the limit to 0
	limits.MaxSubscriptions = 0
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}
	// Now try to test the limit against
	// any value, it should not fail
	testMaxSubs(t, fs, "bar", 0)
}

func TestFSMaxAge(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// For this test, reduce the background task go routine sleep interval
	bkgTasksSleepDuration = 10 * time.Millisecond
	defer func() {
		bkgTasksSleepDuration = defaultBkgTasksSleepDuration
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testMaxAge(t, fs)
}

func TestFSBasicSubStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicSubStore(t, fs)
}

func TestFSRecoverSubUpdatesForDeleteSubOK(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store one sub for which we are going to store updates
	// and then delete
	sub1 := storeSub(t, fs, "foo")
	// This one will stay and should be recovered
	sub2 := storeSub(t, fs, "foo")

	// Add several pending seq for sub1
	storeSubPending(t, fs, "foo", sub1, 1, 2, 3)

	// Delete sub
	storeSubDelete(t, fs, "foo", sub1)

	// Add more updates
	storeSubPending(t, fs, "foo", sub1, 4, 5)
	storeSubAck(t, fs, "foo", sub1, 1)

	// Delete unexisting subs
	storeSubDelete(t, fs, "foo", sub2+1, sub2+2, sub2+3)

	// Close the store
	fs.Close()

	// Recovers now, should not have any error
	limits := testDefaultStoreLimits
	limits.MaxSubscriptions = 1
	fs, state, err := newFileStore(t, defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// Only sub2 should be recovered
	recoveredSubs := subs["foo"]
	if len(recoveredSubs) != 1 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}
	// Make sure the subs count was not messed-up by the fact
	// that the store recovered delete requests for un-recovered
	// subscriptions.
	// Since we have set the limit of subs to 1, and we have
	// recovered one, we should fail creating a new one.
	cs := fs.LookupChannel("foo")
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := cs.Subs.CreateSub(sub); err == nil || err != ErrTooManySubs {
		t.Fatalf("Should have failed creating a sub, got %v", err)
	}
}

func TestFSNoSubIdCollisionAfterRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store a subscription.
	sub1 := storeSub(t, fs, "foo")

	// Close the store
	fs.Close()

	// Recovers now
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 should be recovered
	recoveredSubs := subs["foo"]
	if len(recoveredSubs) != 1 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}

	// Store new subscription
	sub2 := storeSub(t, fs, "foo")

	if sub2 <= sub1 {
		t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", sub1+1, sub2)
	}

	// Store a delete subscription with higher ID and make sure
	// we use something higher on restart
	delSub := uint64(sub1 + 10)
	storeSubDelete(t, fs, "foo", delSub)

	// Close the store
	fs.Close()

	// Recovers now
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs = state.Subs

	if !fs.HasChannel() || len(subs) != 1 || subs["foo"] == nil {
		t.Fatal("Channel foo should have been recovered")
	}

	// sub1 & sub2 should be recovered
	recoveredSubs = subs["foo"]
	if len(recoveredSubs) != 2 {
		t.Fatalf("A subscription should have been recovered, got %v", len(recoveredSubs))
	}

	// Store new subscription
	sub3 := storeSub(t, fs, "foo")

	if sub3 <= sub1 || sub3 <= delSub {
		t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", delSub+1, sub3)
	}
}

func TestFSSubLastSentCorrectOnRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Store a subscription.
	subID := storeSub(t, fs, "foo")

	// A message
	msg := []byte("hello")

	// Store msg seq 1 and 2
	m1 := storeMsg(t, fs, "foo", msg)
	m2 := storeMsg(t, fs, "foo", msg)

	// Store m1 and m2 for this subscription, then m1 again.
	storeSubPending(t, fs, "foo", subID, m1.Sequence, m2.Sequence, m1.Sequence)

	// Restart server
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("State should have been recovered")
	}
	subs := state.Subs["foo"]
	if subs == nil || len(subs) != 1 {
		t.Fatalf("One subscription should have been recovered, got %v", len(subs))
	}
	sub := subs[0]
	// Check that sub's last seq is m2.Sequence
	if sub.Sub.LastSent != m2.Sequence {
		t.Fatalf("Expected LastSent to be %v, got %v", m2.Sequence, sub.Sub.LastSent)
	}
}

func TestFSUpdatedSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Creeate a subscription.
	subID := storeSub(t, fs, "foo")

	// A message
	msg := []byte("hello")

	// Store msg seq 1 and 2
	m1 := storeMsg(t, fs, "foo", msg)
	m2 := storeMsg(t, fs, "foo", msg)
	m3 := storeMsg(t, fs, "foo", msg)

	// Store m1 and m2 for this subscription
	storeSubPending(t, fs, "foo", subID, m1.Sequence, m2.Sequence)

	// Update the subscription
	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	ss := cs.Subs
	updatedSub := &spb.SubState{
		ID:            subID,
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      "newAckInbox",
		AckWaitInSecs: 10,
	}
	if err := ss.UpdateSub(updatedSub); err != nil {
		t.Fatalf("Error updating subscription: %v", err)
	}
	// Store m3 for this subscription
	storeSubPending(t, fs, "foo", subID, m3.Sequence)

	// Store a subscription with update only, should be recovered
	subWithoutNew := &spb.SubState{
		ID:            subID + 1,
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.UpdateSub(subWithoutNew); err != nil {
		t.Fatalf("Error updating subscription: %v", err)
	}

	// Restart server
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("State should have been recovered")
	}
	subs := state.Subs["foo"]
	if subs == nil || len(subs) != 2 {
		t.Fatalf("Two subscriptions should have been recovered, got %v", len(subs))
	}
	// Subscriptions are recovered from a map, and then returned as an array.
	// There is no guarantee that we get them in the order they were persisted.
	for _, s := range subs {
		if s.Sub.ID == subID {
			// Check that sub's last seq is m3.Sequence
			if s.Sub.LastSent != m3.Sequence {
				t.Fatalf("Expected LastSent to be %v, got %v", m3.Sequence, s.Sub.LastSent)
			}
			// Update lastSent since we know it is correct.
			updatedSub.LastSent = m3.Sequence
			// Now compare that what we recovered is same that we used to update.
			if !reflect.DeepEqual(*s.Sub, *updatedSub) {
				t.Fatalf("Expected subscription to be %v, got %v", updatedSub, s.Sub)
			}
		} else if s.Sub.ID == subID+1 {
			// Compare that what we recovered is same that we used to update.
			if !reflect.DeepEqual(*s.Sub, *subWithoutNew) {
				t.Fatalf("Expected subscription to be %v, got %v", subWithoutNew, s.Sub)
			}
		} else {
			t.Fatalf("Unexpected subscription ID: %v", s.Sub.ID)
		}
	}
}

func TestFSGetSeqFromTimestamp(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testGetSeqFromStartTime(t, fs)
}

func TestFSBadClientFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the client's file
	fileName := filepath.Join(defaultDataStore, clientsFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the client's file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating client file: %v", err)
		}
		return file
	}

	clientID := "this-is-a-valid-client-id"
	cli := spb.ClientInfo{ID: clientID, HbInbox: "hbInbox"}
	b, _ := cli.Marshal()

	//
	// WRONG CRC
	//
	file := resetToValidFile()
	// Write the header
	if err := util.WriteInt(file, int(addClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write WRONG crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))+3); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHAL addClient ERROR
	//
	file = resetToValidFile()
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, int(addClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHAL delClient ERROR
	//
	file = resetToValidFile()
	// First write a valid addClient
	writeRecord(file, nil, addClient, &cli, cli.Size(), crc32.IEEETable)
	// Then write an invalid delClient
	delCli := spb.ClientDelete{ID: clientID}
	b, _ = delCli.Marshal()
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, int(delClient)<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)

	//
	// INVALID TYPE
	//
	file = resetToValidFile()
	b, _ = cli.Marshal()
	// Write the header
	if err := util.WriteInt(file, 99<<24|len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	expectedErrorOpeningDefaultFileStore(t)
}

func TestFSClientAPIs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testClientAPIs(t, fs)

	// Restart the store
	fs.Close()

	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	if len(state.Clients) != 2 {
		t.Fatalf("Expected 2 clients to be recovered, got %v", len(state.Clients))
	}
	for _, c := range state.Clients {
		if c.ID != "client2" && c.ID != "client3" {
			t.Fatalf("Unexpected recovered client: %v", c.ID)
		}
	}
}

func TestFSBadServerFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the server's file
	fileName := filepath.Join(defaultDataStore, serverFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the client's file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating client file: %v", err)
		}
		return file
	}

	// Now test with two ServerInfos, and expect to fail
	file := resetToValidFile()

	// Write two server info records
	for i := 0; i < 2; i++ {
		info := testDefaultServerInfo
		b, _ := info.Marshal()
		// Write the size of the proto buf
		if err := util.WriteInt(file, info.Size()); err != nil {
			t.Fatalf("Error writing header: %v", err)
		}
		// Write crc
		if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
			t.Fatalf("Error writing crc: %v", err)
		}
		// Write content
		if _, err := file.Write(b); err != nil {
			t.Fatalf("Error writing info: %v", err)
		}
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record, but with the size that's
	// more than the actual record.
	file = resetToValidFile()
	info := testDefaultServerInfo
	b, _ := info.Marshal()
	// Write the incorrect size (too big) of the proto buf
	if err := util.WriteInt(file, info.Size()+10); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record, but with the size that's
	// less than the actual record.
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the incorrect size (too small) of the proto buf
	if err := util.WriteInt(file, info.Size()-10); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record and then extra data
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the size of the proto buf
	if err := util.WriteInt(file, info.Size()); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write crc
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Write some extra content
	if _, err := file.Write([]byte("more data")); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Write a single record but corrupt the protobuf
	file = resetToValidFile()
	info = testDefaultServerInfo
	b, _ = info.Marshal()
	// Write the size of the proto buf
	if err := util.WriteInt(file, info.Size()); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Alter the content
	copy(b, []byte("hello"))
	// Write a valid CRC of the corrupted protobuf. This is checking
	// that Unmarshall error is correctly captured.
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Error writing crc: %v", err)
	}
	// Write the corrupted content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
}

func TestFSBadMsgFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	// Store a message
	storeMsg(t, fs, "foo", []byte("msg"))

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to exist")
	}
	msgStore := cs.Msgs.(*FileMsgStore)
	firstSliceFileName := msgStore.files[1].file.name
	firstIdxFileName := msgStore.files[1].idxFile.name

	// Close it
	fs.Close()

	//
	// INVALID INDEX FILE CONTENT
	//
	idxFile, err := openFileWithFlags(firstIdxFileName, os.O_RDWR)
	if err != nil {
		t.Fatalf("Error creating index file: %v", err)
	}
	if _, err := idxFile.Write([]byte("abcdefg")); err != nil {
		t.Fatalf("Error writing content: %v", err)
	}
	// Close the file
	if err := idxFile.Close(); err != nil {
		t.Fatalf("Unexpected error closing index file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Delete the file
	if err := os.Remove(firstIdxFileName); err != nil {
		t.Fatalf("Unable to delete the index file %q: %v", firstIdxFileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(firstIdxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Now for all other tests below, remove the index file so that
	// the server recovers the data file.
	if err := os.Remove(firstIdxFileName); err != nil {
		t.Fatalf("Unexpected error removing index file: %v", err)
	}

	// First delete the file...
	if err := os.Remove(firstSliceFileName); err != nil {
		t.Fatalf("Unable to delete the msg file %q: %v", firstSliceFileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(firstSliceFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(firstSliceFileName); err != nil {
			stackFatalf(t, "Unexpected error removing file: %v", err)
		}
		// If present, remove the index file
		os.Remove(firstIdxFileName)
		// Create the file with proper file version
		file, err := openFile(firstSliceFileName)
		if err != nil {
			stackFatalf(t, "Error creating file: %v", err)
		}
		return file
	}

	//
	// INVALID CONTENT
	//
	file := resetToValidFile()
	if err := util.WriteInt(file, 5); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatalf("Error writing content: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHALL ERROR
	//
	file = resetToValidFile()
	msg := &pb.MsgProto{Sequence: 1, Data: []byte("this is a message")}
	b, _ := msg.Marshal()
	// overwrite with dummy content
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write CRC
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Unexpected error writing CRC: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	//
	// ADD INVALID MESSAGE FILE NAME
	//
	os.Remove(firstSliceFileName)
	fileName := filepath.Join(defaultDataStore, "foo", msgFilesPrefix+"a"+datSuffix)
	file, err = openFile(fileName)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
	os.Remove(fileName)
	// Try with other malformed name
	fileName = filepath.Join(defaultDataStore, "foo", msgFilesPrefix+datSuffix[1:])
	file, err = openFile(fileName)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
}

func TestFSBadSubFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	// Store a subscription
	storeSub(t, fs, "foo")

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to exist")
	}

	// Close it
	fs.Close()

	// First delete the file...
	fileName := filepath.Join(defaultDataStore, "foo", subsFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the subscriptions file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating file: %v", err)
		}
		return file
	}

	// Restore a valid file
	file := resetToValidFile()
	// Write size that causes read of content to EOF
	if err := util.WriteInt(file, 100); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Test with various types
	types := []recordType{subRecNew, subRecUpdate, subRecDel, subRecMsg, subRecAck, 99}
	content := []byte("abc")
	crc := crc32.ChecksumIEEE(content)
	for _, oneType := range types {
		// Restore a valid file
		file = resetToValidFile()
		// Write a type that does not exist
		if err := util.WriteInt(file, int(oneType)<<24|len(content)); err != nil {
			t.Fatalf("Error writing header: %v", err)
		}
		// Write CRC
		if err := util.WriteInt(file, int(crc)); err != nil {
			t.Fatalf("Error writing crc: %v", err)
		}
		// Write dummy content
		if _, err := file.Write(content); err != nil {
			t.Fatalf("Error writing info: %v", err)
		}
		// Close the file
		if err := file.Close(); err != nil {
			t.Fatalf("Unexpected error closing file: %v", err)
		}
		// We should fail to create the filestore
		expectedErrorOpeningDefaultFileStore(t)
	}
}

func TestFSAddClientError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Test failure of AddClient (generic tested in common_test.go)
	// Close the client file to cause error
	fs.clientsFile.handle.Close()
	// Should fail
	if c, _, err := fs.AddClient("c1", "hbInbox", "test"); err == nil {
		t.Fatal("Expected error, got none")
	} else if c != nil {
		t.Fatalf("Should not have gotten a client back, got %v", c)
	}
}

func TestFSCompactClientFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 10
	threshold := total / 2

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	check := func(fs *FileStore, expectedClients, expectedDelRecs int) {
		fs.RLock()
		numClients := len(fs.clients)
		delRecs := fs.cliDeleteRecs
		fs.RUnlock()
		if numClients != expectedClients {
			stackFatalf(t, "Expected %v clients, got %v", expectedClients, numClients)
		}
		if delRecs != expectedDelRecs {
			stackFatalf(t, "Expected %v delete records, got %v", expectedDelRecs, delRecs)
		}
	}

	// Create clients below threshold
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete half.
	for i := 0; i < threshold-1; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	check(fs, threshold+1, threshold-1)

	// Recover
	fs.Close()
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	// Verify our numbers are same after recovery
	check(fs, threshold+1, threshold-1)

	// Delete one more, this should trigger compaction
	cid := fmt.Sprintf("cid_%d", threshold)
	fs.DeleteClient(cid)
	// One client less, 0 del records after a compaction
	check(fs, threshold, 0)

	// Make sure we don't compact too often
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Delete almost all of them
	for i := 0; i < total-1; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		fs.DeleteClient(cid)
	}
	// The number of clients should be same than before + 1,
	// and lots of delete
	check(fs, threshold+1, total-1)
	// Now wait for the interval and a bit more
	time.Sleep(1500 * time.Millisecond)
	// Delete one more, compaction should occur
	cid = fmt.Sprintf("cid_%d", 2*total)
	fs.DeleteClient(cid)
	// One less client, 0 delete records after compaction
	check(fs, threshold, 0)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: disable compaction
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: have a big min file size
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		if _, _, err := fs.AddClient(cid, "hbInbox", nil); err != nil {
			t.Fatalf("Unexpected error adding clients: %v", err)
		}
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		fs.DeleteClient(cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)
}

func checkSubStoreRecCounts(t *testing.T, s *FileSubStore, expectedSubs, expectedRecs, expectedDelRecs int) {
	s.RLock()
	numSubs := len(s.subs)
	numRecs := s.numRecs
	numDel := s.delRecs
	s.RUnlock()
	if numSubs != expectedSubs {
		stackFatalf(t, "Expected %v subs, got %v", expectedSubs, numSubs)
	}
	if numRecs != expectedRecs {
		stackFatalf(t, "Expected %v recs, got %v", expectedRecs, numRecs)
	}
	if numDel != expectedDelRecs {
		stackFatalf(t, "Expected %v free recs, got %v", expectedDelRecs, numDel)
	}
}

func TestFSCompactSubsFileOnDelete(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 6
	threshold := 3

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs, _, err := fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	subIDs := make([]uint64, 0, total)
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		subID := subIDs[i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Delete more to cause compaction
	ss.DeleteSub(subIDs[threshold-1])

	// Since we compact, we now have the same number of recs and subs,
	// and no delete records.
	checkSubStoreRecCounts(t, ss, total-threshold, total-threshold, 0)

	// Make sure we don't compact too often
	count := total - threshold - 1
	for i := 0; i < count; i++ {
		subID := subIDs[threshold+i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 1, total-threshold, count)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact by adding and then removing a subscription
	ss.DeleteSub(subIDs[total-1])
	// Check stats
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)

	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)
}

func TestFSCompactSubsFileOnAck(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs, _, err := fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	totalSeqs := 10
	threshold := 1 + 5 // 1 for the sub, 5 for acks

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}

	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Add 1 more ack to cause compaction
	if err := ss.AckSeqPending(sub.ID, uint64(threshold)); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Now the number of acks should be 0.
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs-threshold, 0)
	startCount := 1 + totalSeqs - threshold

	// Make sure we don't compact too often
	start := 10000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Then remove them all. Total gain/loss is 0.
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 2*totalSeqs)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact
	willCompactID := uint64(20000)
	if err := ss.AddSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding seq: %v", err)
	}
	if err := ss.AckSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Check stats
	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = fs.LookupChannel("foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Add more sequences
	start = 30000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Remove the subscription, this should cause a compact
	ss.DeleteSub(sub.ID)
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)

	fs.Close()
	// Wipe-out everything
	cleanupDatastore(t, defaultDataStore)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs, _, err = fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating channel: %v", err)
	}
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)
}

func TestFSFlush(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, DoSync(true))
	defer fs.Close()

	testFlush(t, fs)

	// Now specific tests to File store
	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	msg := storeMsg(t, fs, "foo", []byte("new msg"))
	subID := storeSub(t, fs, "foo")
	storeSubPending(t, fs, "foo", subID, msg.Sequence)
	// Close the underlying file
	ms := cs.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.writeSlice.file.handle.Close()
	ms.Unlock()
	// Expect Flush to fail
	if err := cs.Msgs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
	// Close the underlying file
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	ss.Unlock()
	// Expect Flush to fail
	if err := cs.Subs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}

	// Close and re-open
	fs.Close()
	fs, _ = openDefaultFileStore(t, DoSync(true))
	defer fs.Close()

	// Check that Flush() is still doing Sync even if
	// buf writer is empty
	cs = fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	// Close the underlying file
	ms = cs.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.writeSlice.file.handle.Close()
	ms.Unlock()
	// Expect Flush to fail
	if err := cs.Msgs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
	// Close the underlying file
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	// Simulate that there was activity (alternatively,
	// we would need a buffer size smaller than a sub record
	// being written so that buffer writer is by-passed).
	ss.activity = true
	ss.Unlock()
	// Expect Flush to fail
	if err := cs.Subs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
}

func TestFSDoSync(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	total := 100
	dur := [2]time.Duration{}

	for i := 0; i < 2; i++ {
		sOpts := DefaultFileStoreOptions
		sOpts.DoSync = true
		if i == 1 {
			sOpts.DoSync = false
		}
		fs, _, err := newFileStore(t, defaultDataStore, &testDefaultStoreLimits, AllOptions(&sOpts))
		if err != nil {
			stackFatalf(t, "Unable to create a FileStore instance: %v", err)
		}
		defer fs.Close()

		cs, _, err := fs.CreateChannel("foo", nil)
		if err != nil {
			t.Fatalf("Unexpected error creating channel: %v", err)
		}
		subID := storeSub(t, fs, "foo")

		msg := make([]byte, 1024)
		start := time.Now()
		// Send more message when fsync is disabled. It should still be faster,
		// and would catch if bug in code where we always do fsync, regardless
		// of option.
		for j := 0; j < total+(i*total/10); j++ {
			m := storeMsg(t, fs, "foo", msg)
			cs.Msgs.Flush()
			storeSubPending(t, fs, "foo", subID, m.Sequence)
			cs.Subs.Flush()
		}
		dur[i] = time.Since(start)

		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
	if dur[0] < dur[1] {
		t.Fatalf("Expected File sync enabled to be slower than disabled: %v vs %v", dur[0], dur[1])
	}
}

type testReader struct {
	content     []byte
	start       int
	errToReturn error
}

func (t *testReader) setContent(content []byte) {
	t.content = content
	t.start = 0
}

func (t *testReader) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testReader) Read(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	if len(t.content)-t.start < len(p) {
		return 0, io.EOF
	}
	copy(p, t.content[t.start:t.start+len(p)])
	t.start += len(p)
	return len(t.content), nil
}

func TestFSReadRecord(t *testing.T) {
	r := &testReader{}

	var err error

	buf := make([]byte, 5)
	var retBuf []byte
	recType := recNoType
	recSize := 0

	// Reader returns an error
	errReturned := fmt.Errorf("Fake error")
	r.setErrToReturn(errReturned)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != errReturned {
		t.Fatalf("Expected error %v, got: %v", errReturned, err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Record not containing CRC
	_header := [4]byte{}
	header := _header[:]
	util.ByteOrder.PutUint32(header, 0)
	r.setErrToReturn(nil)
	r.setContent(header)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Wrong CRC
	b := make([]byte, recordHeaderSize+5)
	util.ByteOrder.PutUint32(b, 5)
	copy(b[recordHeaderSize:], []byte("hello"))
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}
	// Not asking for CRC should return ok
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if string(retBuf[:recSize]) != "hello" {
		t.Fatalf("Expected body to be \"hello\", got %q", string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check that returned buffer has expanded as required
	b = make([]byte, recordHeaderSize+10)
	payload := []byte("hellohello")
	util.ByteOrder.PutUint32(b, uint32(len(payload)))
	util.ByteOrder.PutUint32(b[4:recordHeaderSize], crc32.ChecksumIEEE(payload))
	copy(b[recordHeaderSize:], payload)
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check rec type returned properly
	util.ByteOrder.PutUint32(b, 1<<24|10) // reuse previous buf
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, true, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recordType(1) {
		t.Fatalf("Expected recType to be 1, got %v", recType)
	}
}

type testWriter struct {
	buf         []byte
	errToReturn error
}

func (t *testWriter) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testWriter) reset() {
	t.buf = t.buf[:0]
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	t.buf = append(t.buf, p...)
	return len(p), nil
}

type recordProduceErrorOnMarshal struct {
	errToReturn error
}

func (r *recordProduceErrorOnMarshal) Size() int {
	return 1
}

func (r *recordProduceErrorOnMarshal) MarshalTo(b []byte) (int, error) {
	return 0, r.errToReturn
}

func TestFSWriteRecord(t *testing.T) {
	w := &testWriter{}

	var err error

	buf := make([]byte, 100)
	var retBuf []byte
	size := 0

	cli := &spb.ClientInfo{ID: "me", HbInbox: "inbox"}
	cliBuf, _ := cli.Marshal()

	retBuf, size, err = writeRecord(w, buf, addClient, cli, cli.Size(), crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header := util.ByteOrder.Uint32(w.buf[:4])
	recType := recordType(header >> 24 & 0xFF)
	if recType != addClient {
		t.Fatalf("Expected type %v, got %v", addClient, recType)
	}
	crc := util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:])
	}

	// Check with no type
	w.reset()
	retBuf, size, err = writeRecord(w, buf, recNoType, cli, cli.Size(), crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header = util.ByteOrder.Uint32(w.buf[:4])
	recType = recordType(header >> 24 & 0xFF)
	if recType != recNoType {
		t.Fatalf("Expected type %v, got %v", recNoType, recType)
	}
	crc = util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:])
	}

	// Check for marshalling error
	w.reset()
	errReturned := fmt.Errorf("Fake error")
	corruptRec := &recordProduceErrorOnMarshal{errToReturn: errReturned}
	retBuf, size, err = writeRecord(w, buf, recNoType, corruptRec, corruptRec.Size(), crc32.IEEETable)
	if err != errReturned {
		t.Fatalf("Expected error %v, got %v", errReturned, err)
	}
	if retBuf == nil {
		t.Fatal("Returned slice should not be nil")
	}
	if size != 0 {
		t.Fatalf("Size should be 0, got %v", size)
	}
}

func TestFSNoPartialWriteDueToBuffering(t *testing.T) {
	// We could use any record type here, using MsgProto
	m1 := &pb.MsgProto{Sequence: 1, Subject: "foo", Data: []byte("msg1")}
	m2 := &pb.MsgProto{Sequence: 2, Subject: "foo", Data: []byte("msg2")}

	w := &testWriter{}
	size := m1.Size() + recordHeaderSize + 2
	bw := bufio.NewWriterSize(w, size)

	// Write first record
	m1buf, _, _ := writeRecord(bw, nil, recNoType, m1, m1.Size(), crc32.IEEETable)
	// Now add m2
	writeRecord(bw, nil, recNoType, m2, m2.Size(), crc32.IEEETable)
	// Check that w's buf only contains m2 (no partial)
	if bw.Buffered() != m2.Size()+recordHeaderSize {
		t.Fatalf("Expected buffer to contain %v bytes, got %v", m2.Size()+recordHeaderSize, bw.Buffered())
	}
	if len(w.buf) != m1.Size()+recordHeaderSize {
		t.Fatalf("Expected backend buffer to contain %v bytes, got %v", m1.Size()+recordHeaderSize, len(w.buf))
	}
	if !reflect.DeepEqual(w.buf, m1buf) {
		t.Fatal("Backend buffer does not contain what we expected")
	}

	w.reset()
	errThrown := fmt.Errorf("On purpose")
	w.setErrToReturn(errThrown)
	// Write first record
	writeRecord(bw, nil, recNoType, m1, m1.Size(), crc32.IEEETable)
	// Now add m2
	_, _, err := writeRecord(bw, nil, recNoType, m2, m2.Size(), crc32.IEEETable)
	if err != errThrown {
		t.Fatalf("Expected error %v, got %v", errThrown, err)
	}
}

func TestFSNoReferenceToCallerSubState(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs, _, err := s.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Error creating channel [foo]: %v", err)
	}
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Error creating subscription")
	}
	// Get the fileStore sub object
	fss := ss.(*FileSubStore)
	fss.RLock()
	storeSub := fss.subs[sub.ID].sub
	fss.RUnlock()
	// Content of filestore's subscription must match sub
	if !reflect.DeepEqual(*sub, *storeSub) {
		t.Fatalf("Expected sub to be %v, got %v", sub, storeSub)
	}
	// However, these should not be the same objects (no sharing)
	if sub == storeSub {
		t.Fatalf("SubState should not be shared between server and store")
	}
}

func TestFSCompactSubsUpdateLastSent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := createDefaultFileStore(t)
	defer s.Close()

	total := 10
	for i := 0; i < total; i++ {
		storeMsg(t, s, "foo", []byte("hello"))
	}
	expectedLastSent := 5
	subID := storeSub(t, s, "foo")
	for i := 0; i < expectedLastSent; i++ {
		storeSubPending(t, s, "foo", subID, uint64(i+1))
	}
	// Consume the 2 last
	storeSubAck(t, s, "foo", subID, 4, 5)
	// Force a compact
	ss := s.LookupChannel("foo").Subs.(*FileSubStore)
	ss.compact(ss.file.name)
	// Close and re-open store
	s.Close()
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	// Get sub from recovered state
	if len(rs.Subs) != 1 {
		t.Fatalf("Expected to recover one subscription, got %v", len(rs.Subs))
	}
	rsubArray := rs.Subs["foo"]
	if len(rsubArray) != 1 {
		t.Fatalf("Expected to recover on subscription on foo channel, got %v", len(rsubArray))
	}
	rsub := rsubArray[0]
	if rsub.Sub.LastSent != uint64(expectedLastSent) {
		t.Fatalf("Expected recovered subscription LastSent to be %v, got %v", expectedLastSent, rsub.Sub.LastSent)
	}
}

func TestFSFileSlicesClosed(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Reduce some filestore values for this test
	bkgTasksSleepDuration = 100 * time.Millisecond
	cacheTTL = int64(200 * time.Millisecond)
	defer func() {
		bkgTasksSleepDuration = defaultBkgTasksSleepDuration
		cacheTTL = int64(defaultCacheTTL)
	}()

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 50
	fs, err := NewFileStore(defaultDataStore, &limits,
		SliceConfig(10, 0, 0, ""))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer fs.Close()
	payload := []byte("hello")
	for i := 0; i < limits.MaxMsgs; i++ {
		storeMsg(t, fs, "foo", payload)
	}
	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.Flush()
	// Wait for cache to be empty
	timeout := time.Now().Add(time.Duration(3 * cacheTTL))
	empty := false
	for time.Now().Before(timeout) {
		ms.RLock()
		empty = len(ms.cache.seqMaps) == 0
		ms.RUnlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !empty {
		t.Fatal("Cache should be empty")
	}
	for i := 0; i < limits.MaxMsgs; i++ {
		ms.Lookup(uint64(i + 1))
	}
	time.Sleep(1500 * time.Millisecond)
	ms.RLock()
	for i, s := range ms.files {
		if s == ms.writeSlice {
			continue
		}
		if s.file.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (data file)", i)
		}
		if s.idxFile.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (index file)", i)
		}
	}
	ms.RUnlock()
}

func TestFSRecoverWithoutIndexFiles(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 8
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	total := limits.MaxMsgs + 1
	payload := []byte("hello")
	msgs := make([]*pb.MsgProto, 0, total)
	for i := 0; i < total; i++ {
		msgs = append(msgs, storeMsg(t, fs, "foo", payload))
	}
	cs := fs.LookupChannel("foo")
	msgStore := cs.Msgs.(*FileMsgStore)
	// Get the index file names
	fs.RLock()
	idxFileNames := make([]string, 0, len(msgStore.files))
	for _, sl := range msgStore.files {
		idxFileNames = append(idxFileNames, sl.idxFile.name)
	}
	fs.RUnlock()
	// Close store
	fs.Close()

	// Remove the index files
	for _, fn := range idxFileNames {
		if err := os.Remove(fn); err != nil {
			t.Fatalf("Error removing file %q: %v", fn, err)
		}
	}
	// Restart store
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()
	cs = fs.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to be recovered")
	}
	for i := 0; i < total; i++ {
		m := cs.Msgs.Lookup(uint64(i + 1))
		if !reflect.DeepEqual(m, msgs[i]) {
			t.Fatalf("Expected to get message %v, got %v", msgs[i], m)
		}
	}
}

func TestFSEmptySlice(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	m := storeMsg(t, fs, "foo", []byte("hello"))

	fs.Close()

	// Add an empty slice
	file, err := openFile(filepath.Join(defaultDataStore, "foo", msgFilesPrefix+"2"+datSuffix))
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	file.Close()

	// Recover
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected to recover foo channel")
	}
	lm := cs.Msgs.Lookup(1)
	if !reflect.DeepEqual(m, lm) {
		t.Fatalf("Expected recovered message to be %v, got %v", m, lm)
	}
}

func TestFSStoreMsgCausesFlush(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, BufferSize(50))
	defer fs.Close()

	m1 := storeMsg(t, fs, "foo", []byte("hello"))
	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	buffered := ms.bw.buf.Buffered()
	bufferedMsgs := len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m1.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m1.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	m2 := storeMsg(t, fs, "foo", []byte("hello again!"))
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m2.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m2.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	// Now store a message that is bigger than the buffer, it should be
	// directly written to file
	payload := make([]byte, 200)
	storeMsg(t, fs, "foo", payload)
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != 0 {
		t.Fatalf("Expected buffered to be 0, got %v", buffered)
	}
	if bufferedMsgs != 0 {
		t.Fatalf("Expected 0 buffered message, got %v", bufferedMsgs)
	}
}

func TestFSRemoveFileSlices(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Set config such that each slice store only 1 message
	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	// Ensure that slices will be removed.
	limits.MaxMsgs = 3
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	expectedFirst := uint64(5)
	total := 7
	payload := []byte("hello")
	for i := 0; i < total; i++ {
		storeMsg(t, fs, "foo", payload)
	}
	// Check first and last indexes
	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	if ms.FirstMsg().Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, ms.FirstMsg().Sequence)
	}
	if ms.LastMsg().Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, ms.LastMsg().Sequence)
	}
	// Close store
	fs.Close()

	// Reopen
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()
	cs = fs.LookupChannel("foo")
	ms = cs.Msgs.(*FileMsgStore)
	if ms.FirstMsg().Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, ms.FirstMsg().Sequence)
	}
	if ms.LastMsg().Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, ms.LastMsg().Sequence)
	}
}

func TestFSFirstEmptySliceRemovedOnCreateNewSlice(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, time.Second, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxAge = time.Second
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	// Store a message
	storeMsg(t, fs, "foo", []byte("test"))

	// Wait for message to expire
	cs := fs.LookupChannel("foo")
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if n, _, _ := cs.Msgs.State(); n == 0 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Message should have expired")
	}

	// First slice should still exist altough empty
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	empty := false
	if ms.writeSlice != nil && ms.writeSlice.msgsCount == ms.writeSlice.rmCount {
		empty = true
	}
	firstWrite := ms.writeSlice.firstWrite
	ms.RUnlock()
	if !empty || numFiles != 1 || firstFileSeq != 1 {
		t.Fatalf("Expected slice to be empty, numFiles and firstFileSeq to be 1, got %v, %v and %v",
			empty, numFiles, firstFileSeq)
	}

	// Since slice time check uses ms.timeTick, ensure that we wait long enough.
	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		timeTick := atomic.LoadInt64(&ms.timeTick)

		if timeTick-firstWrite > int64(time.Second) {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Waited too long for timeTick to update")
	}

	// Send another message...
	storeMsg(t, fs, "foo", []byte("test"))

	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		if n, _, _ := cs.Msgs.State(); n == 1 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Should have gotten a message")
	}

	// A new slice should have been created and the first one deleted.
	ms.RLock()
	numFiles = len(ms.files)
	firstFileSeq = ms.firstFSlSeq
	updatedwriteSlice := ms.writeSlice == ms.files[2]
	ms.RUnlock()
	if !updatedwriteSlice || numFiles != 1 || firstFileSeq != 2 {
		t.Fatalf("Expected current slice to be updated to second slice, numFiles to be 1, firstFileSeq to be 2, got %v, %v and %v",
			updatedwriteSlice, numFiles, firstFileSeq)
	}
}

func TestFSSubStoreVariousBufferSizes(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	sizes := []int{0, subBufMinShrinkSize - subBufMinShrinkSize/10, subBufMinShrinkSize, 3*subBufMinShrinkSize + subBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		m := storeMsg(t, fs, "foo", []byte("hello"))

		// Perform some activities on subscriptions file
		subID := storeSub(t, fs, "foo")

		// Get FileSubStore
		cs := fs.LookupChannel("foo")
		ss := cs.Subs.(*FileSubStore)

		// Cause a flush to empty the buffer
		ss.Flush()

		// Check that bw is not nil and writer points to the buffer writer
		ss.RLock()
		bw := ss.bw
		writer := ss.writer
		file := ss.file.handle
		bufSize := 0
		if ss.bw != nil {
			bufSize = ss.bw.buf.Available()
		}
		ss.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileSubStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileSubStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileSubStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileSubStore's writer should be set to the buffer writer")
		}
		initialSize := size
		if size > subBufMinShrinkSize {
			initialSize = subBufMinShrinkSize
		}
		if bufSize != initialSize {
			t.Fatalf("Incorrect initial size, should be %v, got %v", initialSize, bufSize)
		}

		// Fill up the buffer (meaningfull only when buffer is used)
		fillBuffer := func() {
			total := 0
			for i := 0; i < 1000; i++ {
				ss.RLock()
				before := ss.bw.buf.Buffered()
				ss.RUnlock()
				storeSubPending(t, fs, "foo", subID, m.Sequence)
				ss.RLock()
				if ss.bw.buf.Buffered() > before {
					total += ss.bw.buf.Buffered() - before
				} else {
					total += ss.bw.buf.Buffered()
				}
				ss.RUnlock()
				// Stop when we have persisted at least 2 times the max buffer size
				if total >= 2*size {
					// We should have caused buffer to be flushed by now
					break
				}
			}
			if total < 2*size {
				t.Fatalf("Did not reach target total (%v, got %v) after limit iterations", 2*size, total)
			}
		}
		if size > 0 {
			fillBuffer()
		} else {
			// Just write a bunch of stuff
			for i := 0; i < 50; i++ {
				storeSubPending(t, fs, "foo", subID, m.Sequence)
			}
		}

		ss.RLock()
		bufSize = 0
		if size > 0 {
			bufSize = ss.bw.bufSize
		}
		ss.RUnlock()
		if size == 0 {
			if bufSize != 0 {
				t.Fatalf("BufferSize is 0, so ss.bufSize should be 0, got %v", bufSize)
			}
		} else if size <= subBufMinShrinkSize {
			// If size is smaller than min shrink size, the buffer should not have
			// increased in size
			if bufSize > subBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - ss.bw size should at or below %v, got %v", size, subBufMinShrinkSize, bufSize)
			}
		} else {
			// We should have started at min size, and now size should have been increased.
			if bufSize <= subBufMinShrinkSize || bufSize > size {
				t.Fatalf("BufferSize=%v - ss.bw size should have increased but no more than %v, got %v", size, size, bufSize)
			}
		}

		// Delete subscription
		storeSubDelete(t, fs, "foo", subID)
		// Compact
		ss.Lock()
		err := ss.compact(ss.file.name)
		ss.Unlock()
		if err != nil {
			t.Fatalf("Error during compact: %v", err)
		}
		ss.RLock()
		bw = ss.bw
		writer = ss.writer
		file = ss.file.handle
		shrinkTimerOn := ss.shrinkTimer != nil
		ss.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileSubStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileSubStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileSubStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileSubStore's writer should be set to the buffer writer")
		}
		// When buffer size is greater than min size, see if it shrinks
		if size > subBufMinShrinkSize {
			if !shrinkTimerOn {
				t.Fatal("Timer should have been created to try shrink the buffer")
			}
			// Invoke the timer callback manually (so we don't have to wait)
			// Call many times and make sure size never goes down too low.
			for i := 0; i < 14; i++ {
				ss.shrinkBuffer()
			}
			// Now check
			ss.RLock()
			bufSizeNow := ss.bw.bufSize
			ss.RUnlock()
			if bufSizeNow >= bufSize {
				t.Fatalf("BufferSize=%v - Buffer size expected to decrease, got: %v", size, bufSizeNow)
			}
			if bufSizeNow < subBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - Buffer should not go below %v, got %v", size, subBufMinShrinkSize, bufSizeNow)
			}

			// Check that the request to shrink is canceled if more data arrive
			// First make buffer expand.
			fillBuffer()
			// Flush to empty it
			ss.Flush()
			// Invoke shrink
			ss.shrinkBuffer()
			// Check that request is set
			ss.RLock()
			shrinkReq := ss.bw.shrinkReq
			ss.RUnlock()
			if !shrinkReq {
				t.Fatal("Shrink request should be true")
			}
			// Cause buffer to expand again
			fillBuffer()
			// Check that request should have been cancelled.
			ss.RLock()
			shrinkReq = ss.bw.shrinkReq
			ss.RUnlock()
			if shrinkReq {
				t.Fatal("Shrink request should be false")
			}
		}
		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
}

func TestFSMsgStoreVariousBufferSizes(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	sizes := []int{0, msgBufMinShrinkSize - msgBufMinShrinkSize/10, msgBufMinShrinkSize, 3*msgBufMinShrinkSize + msgBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		storeMsg(t, fs, "foo", []byte("hello"))

		// Get FileMsgStore
		cs := fs.LookupChannel("foo")
		ms := cs.Msgs.(*FileMsgStore)

		// Cause a flush to empty the buffer
		ms.Flush()

		// Check that bw is not nil and writer points to the buffer writer
		ms.RLock()
		bw := ms.bw
		writer := ms.writer
		file := ms.writeSlice.file.handle
		bufSize := 0
		if ms.bw != nil {
			bufSize = ms.bw.buf.Available()
		}
		ms.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileMsgStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileMsgStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileMsgStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileMsgStore's writer should be set to the buffer writer")
		}
		initialSize := size
		if size > msgBufMinShrinkSize {
			initialSize = msgBufMinShrinkSize
		}
		if bufSize != initialSize {
			t.Fatalf("Incorrect initial size, should be %v, got %v", initialSize, bufSize)
		}

		// Fill up the buffer (meaningfull only when buffer is used)
		fillBuffer := func() {
			total := 0
			for i := 0; i < 1000; i++ {
				ms.RLock()
				before := ms.bw.buf.Buffered()
				ms.RUnlock()
				storeMsg(t, fs, "foo", []byte("hello"))
				ms.RLock()
				if ms.bw.buf.Buffered() > before {
					total += ms.bw.buf.Buffered() - before
				} else {
					total += ms.bw.buf.Buffered()
				}
				ms.RUnlock()
				// Stop when we have persisted at least 2 times the max buffer size
				if total >= 2*size {
					// We should have caused buffer to be flushed by now
					break
				}
			}
			if total < 2*size {
				t.Fatalf("Did not reach target total (%v, got %v) after limit iterations", 2*size, total)
			}
		}
		if size > 0 {
			fillBuffer()
		} else {
			// Just write a bunch of stuff
			for i := 0; i < 50; i++ {
				storeMsg(t, fs, "foo", []byte("hello"))
			}
		}

		ms.RLock()
		bufSize = 0
		if size > 0 {
			bufSize = ms.bw.bufSize
		}
		ms.RUnlock()
		if size == 0 {
			if bufSize != 0 {
				t.Fatalf("BufferSize is 0, so ss.bufSize should be 0, got %v", bufSize)
			}
		} else if size < msgBufMinShrinkSize {
			// If size is smaller than min shrink size, the buffer should not have
			// increased in size
			if bufSize > msgBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - ss.bw size should at or below %v, got %v", size, msgBufMinShrinkSize, bufSize)
			}
		} else {
			// We should have started at min size, and now size should have been increased.
			if bufSize < msgBufMinShrinkSize || bufSize > size {
				t.Fatalf("BufferSize=%v - ss.bw size should have increased but no more than %v, got %v", size, size, bufSize)
			}
		}

		// When buffer size is greater than min size, see if it shrinks
		if size > msgBufMinShrinkSize {
			// Invoke the timer callback manually (so we don't have to wait)
			// Call many times and make sure size never goes down too low.
			for i := 0; i < 14; i++ {
				ms.Lock()
				ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
				ms.writer = ms.bw.buf
				ms.Unlock()
			}
			// Now check
			ms.RLock()
			bufSizeNow := ms.bw.bufSize
			ms.RUnlock()
			if bufSizeNow >= bufSize {
				t.Fatalf("Buffer size expected to decrease, got: %v", bufSizeNow)
			}
			if bufSizeNow < msgBufMinShrinkSize {
				t.Fatalf("Buffer should not go below %v, got %v", msgBufMinShrinkSize, bufSizeNow)
			}

			// Check that the request to shrink is canceled if more data arrive
			// First make buffer expand.
			fillBuffer()
			// Flush to empty it
			ms.Flush()
			// Invoke shrink
			ms.Lock()
			ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
			ms.Unlock()
			// Check that request is set
			ms.RLock()
			shrinkReq := ms.bw.shrinkReq
			ms.RUnlock()
			if !shrinkReq {
				t.Fatal("Shrink request should be true")
			}
			// Cause buffer to expand again
			fillBuffer()
			// Check that request should have been cancelled.
			ms.RLock()
			shrinkReq = ms.bw.shrinkReq
			ms.RUnlock()
			if shrinkReq {
				t.Fatal("Shrink request should be false")
			}
		}
		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
}

func TestFSPerChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testPerChannelLimits(t, fs)
}

func TestFSArchiveScript(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	tmpDir, err := ioutil.TempDir(".", "")
	if err != nil {
		t.Fatalf("Unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Unable to get current directory: %v", err)
	}
	scriptFile := ""
	content := ""
	if runtime.GOOS == "windows" {
		scriptFile = fmt.Sprintf("%s\\script_%v.bat", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("mkdir %s\\%s\\%%1\nmove %%2 %s\\%s\\%%1\nmove %%3 %s\\%s\\%%1", pwd, tmpDir, pwd, tmpDir, pwd, tmpDir)
	} else {
		scriptFile = fmt.Sprintf("%s/script_%v.sh", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("#!/bin/bash\nmkdir -p %s/%s/$1\nmv $2 $3 %s/%s/$1\n", pwd, tmpDir, pwd, tmpDir)
	}
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	limits := DefaultStoreLimits
	limits.MaxMsgs = 1
	fs.SetLimits(&limits)

	// Store one message
	storeMsg(t, fs, "foo", []byte("msg1"))

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	fileName := ms.files[1].file.name
	ms.RUnlock()

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, fs, "foo", []byte("msg2"))

	// Original file should not be present
	ok := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// File should have been moved to tmpDir by script
	ok = false
	bakFile := fmt.Sprintf("%s/foo/%s1%s%s", tmpDir, msgFilesPrefix, datSuffix, bakSuffix)
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s != nil && serr == nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File should have been moved to tmp dir")
	}
	// Close store
	fs.Close()
	// Cleanup datastore
	cleanupDatastore(t, defaultDataStore)

	// Create a script that will error out
	os.Remove(scriptFile)
	content = "xxx"
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs = createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	fs.SetLimits(&limits)

	// Store one message
	storeMsg(t, fs, "foo", []byte("msg1"))

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, fs, "foo", []byte("msg2"))

	// Original file should not be present
	ok = false
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// Since script should fail, .bak file should still be in channel's directory
	ok = true
	timeout = time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s == nil || serr != nil {
			ok = false
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}
}

func TestFSNoSliceLimitAndNoChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// No slice limit
	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	// And no channel limit
	limits := StoreLimits{}
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	total := 1000
	msg := []byte("msg")
	for i := 0; i < total; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	lastFileSeq := ms.lastFSlSeq
	ms.RUnlock()

	if numFiles != 1 || firstFileSeq != 1 || lastFileSeq != 1 {
		t.Fatalf("Expected numFiles, firstFileSeq and lastFileSeq to be all 1, got %v, %v and %v",
			numFiles, firstFileSeq, lastFileSeq)
	}
}

func TestFSMsgRemovedWhileBuffered(t *testing.T) {
	// Test is irrelevant if no buffering used
	if disableBufferWriters {
		t.SkipNow()
	}
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxMsgs = 10
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", &limits)
	}

	total := 1000
	msg := []byte("msg")
	for i := 0; i < total; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	fs.Close()

	fs, state, err := newFileStore(t, defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Unexpected error opening store: %v", err)
	}
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected to recover a state")
	}
}

func TestFSSliceLimitsBasedOnChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	// First check that with low channel limits, we have at least
	// a slice of 1.
	limits := DefaultStoreLimits
	limits.MaxMsgs = 3
	limits.MaxBytes = 3
	limits.MaxAge = 3 * time.Second
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	storeMsg(t, fs, "foo", []byte("msg"))

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	slCount := ms.slCountLim
	slSize := ms.slSizeLim
	slAge := ms.slAgeLim
	ms.RUnlock()

	if slCount != 1 {
		t.Fatalf("Expected slice limit count to be 1, got %v", slCount)
	}
	if slSize != 1 {
		t.Fatalf("Expected slice limit size to be 1, got %v", slSize)
	}
	if slAge != int64(time.Second) {
		t.Fatalf("Expected slice limit age to be 1sec, got %v", time.Duration(slAge))
	}
	fs.Close()
	cleanupDatastore(t, defaultDataStore)

	// Open with different limits
	limits.MaxMsgs = 100
	limits.MaxBytes = 100
	limits.MaxAge = 20 * time.Second
	fs = createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	storeMsg(t, fs, "foo", []byte("msg"))

	cs = fs.LookupChannel("foo")
	ms = cs.Msgs.(*FileMsgStore)
	ms.RLock()
	slCount = ms.slCountLim
	slSize = ms.slSizeLim
	slAge = ms.slAgeLim
	ms.RUnlock()

	if slCount != 25 {
		t.Fatalf("Expected slice limit count to be 25, got %v", slCount)
	}
	if slSize != 25 {
		t.Fatalf("Expected slice limit size to be 25, got %v", slSize)
	}
	if slAge != int64(5*time.Second) {
		t.Fatalf("Expected slice limit age to be 5sec, got %v", time.Duration(slAge))
	}
}

func TestFSRecoverSlicesOutOfOrder(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Make a slice hold only 1 message
	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	msg := []byte("msg")
	total := 200
	// Create slices
	for i := 0; i < total; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	firstFileSeq, lastFileSeq := ms.firstFSlSeq, ms.lastFSlSeq
	first, last := ms.first, ms.last
	wOffset := ms.wOffset
	ms.RUnlock()

	if first != 1 || last != uint64(total) {
		t.Fatalf("Expected first and last to be (1,%v), got (%v,%v)", total, first, last)
	}
	if firstFileSeq != 1 || lastFileSeq != total {
		t.Fatalf("Expected first and last file sequence to be (1,%v), got (%v,%v)", total, firstFileSeq, lastFileSeq)
	}

	fs.Close()

	fs, _ = openDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	cs = fs.LookupChannel("foo")
	ms = cs.Msgs.(*FileMsgStore)
	ms.RLock()
	firstFileSeq, lastFileSeq = ms.firstFSlSeq, ms.lastFSlSeq
	first, last = ms.first, ms.last
	writeSlice := ms.writeSlice
	recoveredWOffset := ms.wOffset
	ms.RUnlock()

	if first != 1 || last != uint64(total) {
		t.Fatalf("Expected first and last to be (1,%v), got (%v,%v)", total, first, last)
	}
	if firstFileSeq != 1 || lastFileSeq != total {
		t.Fatalf("Expected first and last file sequence to be (1,%v), got (%v,%v)", total, firstFileSeq, lastFileSeq)
	}
	if recoveredWOffset != wOffset {
		t.Fatalf("Write offset should be %v, got %v", wOffset, recoveredWOffset)
	}
	if writeSlice == nil || writeSlice.firstSeq != uint64(total) {
		t.Fatalf("Unexpected current slice: %v", writeSlice)
	}
}

func TestFirstAndLastMsg(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	limit := testDefaultStoreLimits
	limit.MaxAge = time.Second
	fs, _, err := newFileStore(t, defaultDataStore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	msg := []byte("msg")
	storeMsg(t, fs, "foo", msg)
	storeMsg(t, fs, "foo", msg)

	cs := fs.LookupChannel("foo")
	if cs.Msgs.FirstMsg().Sequence != 1 {
		t.Fatalf("Unexpected first message: %v", cs.Msgs.FirstMsg())
	}
	if cs.Msgs.LastMsg().Sequence != 2 {
		t.Fatalf("Unexpected last message: %v", cs.Msgs.LastMsg())
	}
	// Wait for all messages to expire
	timeout := time.Now().Add(3 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if n, _, _ := cs.Msgs.State(); n == 0 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("Timed-out waiting for messages to expire")
	}
	ms := cs.Msgs.(*FileMsgStore)
	// By-pass the FirstMsg() and LastMsg() API to make sure that
	// we don't update based on lookup
	ms.RLock()
	firstMsg := ms.firstMsg
	lastMsg := ms.lastMsg
	ms.RUnlock()
	if firstMsg != nil {
		t.Fatalf("Unexpected first message: %v", firstMsg)
	}
	if lastMsg != nil {
		t.Fatalf("Unexpected last message: %v", lastMsg)
	}
	// Store two new messages and check first/last updated correctly
	storeMsg(t, fs, "foo", msg)
	storeMsg(t, fs, "foo", msg)
	ms.RLock()
	firstMsg = ms.firstMsg
	lastMsg = ms.lastMsg
	ms.RUnlock()
	if firstMsg == nil || firstMsg.Sequence != 3 {
		t.Fatalf("Unexpected first message: %v", firstMsg)
	}
	if lastMsg == nil || lastMsg.Sequence != 4 {
		t.Fatalf("Unexpected last message: %v", lastMsg)
	}
}

func TestBufShrink(t *testing.T) {
	if disableBufferWriters {
		t.SkipNow()
	}
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// For this test, reduce the buffer shrink interval
	bufShrinkInterval = time.Second
	defer func() {
		bufShrinkInterval = defaultBufShrinkInterval
	}()

	fs := createDefaultFileStore(t, BufferSize(5*1024*1024))
	defer fs.Close()

	msg := make([]byte, 1024*1024)
	storeMsg(t, fs, "foo", msg)

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	// Check that buffer size is at least 1MB
	ms.RLock()
	bufSize := ms.bw.bufSize
	ms.RUnlock()
	if bufSize < 1024*1024 {
		t.Fatalf("Expected buffer to be at least 1MB, got %v", bufSize)
	}
	// Flush the store to empty the buffer
	if err := cs.Msgs.Flush(); err != nil {
		t.Fatalf("Error flushing store: %v", err)
	}
	// Ensure that buffer shrinks
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		ms.RLock()
		newBufSize := ms.bw.bufSize
		ms.RUnlock()
		if newBufSize < bufSize {
			ok = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Buffer did not shrink")
	}
}

func TestFSCacheList(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Increase cacheTTL so eviction does not happen while we test content of list
	cacheTTL = int64(10 * time.Second)
	defer func() {
		cacheTTL = int64(defaultCacheTTL)
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	msg := []byte("hello")
	// Store messages 1, 2, 3
	for i := 0; i < 3; i++ {
		storeMsg(t, fs, "foo", msg)
	}

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)

	// Check list content
	checkList := func(expectedSeqs ...uint64) {
		ms.RLock()
		c := ms.cache
		cMsg := c.head
		i := 0
		good := 0
		gotStr := ""
		for cMsg != nil {
			gotStr = fmt.Sprintf("%v%v ", gotStr, cMsg.msg.Sequence)
			if cMsg.msg.Sequence == expectedSeqs[i] {
				good++
			}
			i++
			cMsg = cMsg.next
		}
		ms.RUnlock()
		if i != len(expectedSeqs) || good != len(expectedSeqs) {
			expectedStr := ""
			for i := 0; i < len(expectedSeqs); i++ {
				expectedStr = fmt.Sprintf("%v%v ", expectedStr, expectedSeqs[i])
			}
			stackFatalf(t, "Expected sequences: %q, got %q", expectedStr, gotStr)
		}
	}
	// Check that we should have 1, 2, 3
	checkList(1, 2, 3)
	// Lookup first, should be moved to end of list
	ms.Lookup(1)
	checkList(2, 3, 1)
	// Repeat...
	ms.lookup(2)
	checkList(3, 1, 2)
	ms.Lookup(3)
	checkList(1, 2, 3)
	// Lookup last should leave it there
	ms.Lookup(3)
	checkList(1, 2, 3)
}

func TestFSMsgCache(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// For this test, decrease some filestore values
	bkgTasksSleepDuration = 100 * time.Millisecond
	cacheTTL = int64(250 * time.Millisecond)
	defer func() {
		bkgTasksSleepDuration = defaultBkgTasksSleepDuration
		cacheTTL = int64(defaultCacheTTL)
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	payload := []byte("data")
	msg := storeMsg(t, fs, "foo", payload)

	cs := fs.LookupChannel("foo")
	ms := cs.Msgs.(*FileMsgStore)
	// Wait for stored message to be removed from cache
	timeout := time.Now().Add(time.Second)
	empty := false
	for time.Now().Before(timeout) {
		ms.RLock()
		empty = len(ms.cache.seqMaps) == 0
		ms.RUnlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !empty {
		t.Fatal("Message not removed from cache")
	}
	// First lookup
	lm := ms.Lookup(msg.Sequence)
	if !reflect.DeepEqual(msg, lm) {
		t.Fatalf("Expected lookup message to be %v, got %v", msg, lm)
	}
	// Flush store so we removed from buffered messages
	ms.Flush()
	// As long as we call lookup, message should stay in cache
	closeFile := true
	end := time.Now().Add(2 * time.Duration(cacheTTL))
	for time.Now().Before(end) {
		lm = ms.Lookup(msg.Sequence)
		if !reflect.DeepEqual(msg, lm) {
			t.Fatalf("Expected lookup message to be %v, got %v", msg, lm)
		}
		if closeFile {
			ms.Lock()
			ms.writeSlice.file.handle.Close()
			ms.Unlock()
			closeFile = false
		} else {
			time.Sleep(15 * time.Millisecond)
		}
	}
	// Wait for a bit.
	time.Sleep(bkgTasksSleepDuration + time.Duration(cacheTTL) + 500*time.Millisecond)
	// Now a lookup should return nil because message
	// should have been evicted and file is closed
	lm = ms.Lookup(msg.Sequence)
	if lm != nil {
		t.Fatalf("Unexpected message: %v", lm)
	}

	// Use another channel
	end = time.Now().Add(2 * bkgTasksSleepDuration)
	i := 0
	for time.Now().Before(end) {
		storeMsg(t, fs, "bar", payload)
		i++
		if i == 100 {
			time.Sleep(15 * time.Millisecond)
		}
	}
	time.Sleep(bkgTasksSleepDuration)
	// Cache should be empty now
	ms.RLock()
	empty = len(ms.cache.seqMaps) == 0
	ms.RUnlock()
	if !empty {
		t.Fatal("Cache should be empty")
	}
}

func TestFSMsgStoreBackgroundTaskCrash(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// For this test, reduce the buffer shrink interval
	bufShrinkInterval = time.Second
	defer func() {
		bufShrinkInterval = defaultBufShrinkInterval
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	fs.CreateChannel("foo", nil)
	// Wait for background task to execute
	time.Sleep(1500 * time.Millisecond)
	// It should not have crashed.
}

// Test with 2 processes trying to acquire the lock are tested
// in the server package (FT tests) with coverpkg set to stores
// for code coverage.
func TestFSGetExclusiveLock(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()
	for i := 0; i < 2; i++ {
		// GetExclusiveLock should return true even if called more than once
		locked, err := fs.GetExclusiveLock()
		if err != nil {
			t.Fatalf("Error getting exclusing lock: %v", err)
		}
		if !locked {
			t.Fatal("Should have been locked")
		}
	}
	fs.RLock()
	lockFile := fs.lockFile
	fs.RUnlock()
	// Close store
	fs.Close()
	// This should release the lock.
	if !lockFile.IsClosed() {
		t.Fatal("LockFile should have been closed")
	}

	fLockName := filepath.Join(defaultDataStore, "ft.lck")
	defer os.Chmod(fLockName, 0666)
	os.Chmod(fLockName, 0400)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	locked, err := fs.GetExclusiveLock()
	if err == nil {
		t.Fatal("Expected error getting the store lock, got none")
	}
	if locked {
		t.Fatal("Locked should be false")
	}
}
