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
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

var (
	testFSDefaultDatastore     string
	testFSDisableBufferWriters bool
	testFSSetFDsLimit          bool
)

var testFDsLimit = int64(5)

func init() {
	tmpDir, err := ioutil.TempDir(".", "data_stores_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	testFSDefaultDatastore = tmpDir
	FileStoreTestSetBackgroundTaskInterval(testFSDefaultBackgroundTaskInterval)
}

func cleanupFSDatastore(t tLogger) {
	if err := os.RemoveAll(testFSDefaultDatastore); err != nil {
		stackFatalf(t, "Error cleaning up datastore: %v", err)
	}
}

func newFileStore(t tLogger, dataStore string, limits *StoreLimits, options ...FileStoreOption) (*FileStore, *RecoveredState) {
	opts := DefaultFileStoreOptions
	// Set those options based on command line parameters.
	// Each test may override those.
	if testFSDisableBufferWriters {
		opts.BufferSize = 0
	}
	if testFSSetFDsLimit {
		opts.FileDescriptorsLimit = testFDsLimit
	}
	// Apply the provided options
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			stackFatalf(t, "Error creating file store: %v", err)
		}
	}
	fs, err := NewFileStore(testLogger, dataStore, limits, AllOptions(&opts))
	if err != nil {
		stackFatalf(t, "Error creating file store: %v", err)
	}
	state, err := fs.Recover()
	if err != nil {
		fs.Close()
		stackFatalf(t, "Error recovering file store: %v", err)
	}
	return fs, state
}

func createDefaultFileStore(t tLogger, options ...FileStoreOption) *FileStore {
	limits := testDefaultStoreLimits
	fs, state := newFileStore(t, testFSDefaultDatastore, &limits, options...)
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs
}

func openDefaultFileStore(t *testing.T, options ...FileStoreOption) (*FileStore, *RecoveredState) {
	return openDefaultFileStoreWithLimits(t, nil, options...)
}

func openDefaultFileStoreWithLimits(t tLogger, limits *StoreLimits, options ...FileStoreOption) (*FileStore, *RecoveredState) {
	if limits == nil {
		l := testDefaultStoreLimits
		limits = &l
	}
	return newFileStore(t, testFSDefaultDatastore, limits, options...)
}

func expectedErrorOpeningDefaultFileStore(t *testing.T) error {
	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &limits)
	if err == nil {
		_, err = fs.Recover()
		fs.Close()
	}
	if err == nil {
		stackFatalf(t, "Expected an error opening the FileStore, got none")
	}
	return err
}

func TestFSFilesManager(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	if err := os.MkdirAll(testFSDefaultDatastore, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		t.Fatalf("Unable to create the root directory [%s]: %v", testFSDefaultDatastore, err)
	}

	fm := createFilesManager(testFSDefaultDatastore, 4)
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
	// The callback cannot be checked, we set it to nil for the DeepEqual call.
	firstFile.beforeClose = nil
	expectedFile := file{
		id:     fileID(1),
		handle: firstFile.handle, // we cannot know what to expect here, so use value returned
		name:   filepath.Join(testFSDefaultDatastore, "foo"),
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
	fm = createFilesManager(testFSDefaultDatastore, 0)
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
	// Trying to open a removed file should fail.
	if err := fm.openFile(testcloseOrOpenedFile); err == nil {
		t.Fatal("Should have been unable to open a removed file")
	}
	// Try to do concurrent lock/unlock while file is being removed
	for i := 0; i < 20; i++ {
		fileToRemove, err := fm.createFile("concurrentremove", defaultFileFlags, nil)
		if err != nil {
			t.Fatalf("Error creating file: %v", err)
		}
		fm.unlockFile(fileToRemove)
		wg := sync.WaitGroup{}
		wg.Add(1)
		ch := make(chan bool)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch:
					return
				default:
					if _, err := fm.lockFile(fileToRemove); err == nil {
						fm.unlockFile(fileToRemove)
					}
				}
			}
		}()
		time.Sleep(time.Duration(rand.Intn(45)+5) * time.Millisecond)
		removed := fm.remove(fileToRemove)
		ch <- true
		wg.Wait()
		if !removed {
			fm.remove(fileToRemove)
		}
		fileToRemove.handle.Close()
		os.Remove(fileToRemove.name)
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

func TestFSNoDirectoryError(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs, err := NewFileStore(nil, "", nil)
	if err == nil || !strings.Contains(err.Error(), "specified") {
		if fs != nil {
			fs.Close()
		}
		t.Fatalf("Expected error about missing root directory, got: %v", err)
	}
}

func TestFSUseDefaultLimits(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)
	fs, _ := newFileStore(t, testFSDefaultDatastore, nil)
	defer fs.Close()
	if !reflect.DeepEqual(*fs.limits, DefaultStoreLimits) {
		t.Fatalf("Default limits are not used: %v\n", *fs.limits)
	}
}

func TestFSUnsupportedFileVersion(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()
	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", 1, []byte("test"))
	storeSub(t, cs, "foo")

	// Close store
	fs.Close()

	// Overwrite the file version of a message store to an unsupported version
	writeVersion(t, filepath.Join(testFSDefaultDatastore, "foo", msgFilesPrefix+"1"+datSuffix), fileVersion+1)

	// Recover store (should fail)
	err := expectedErrorOpeningDefaultFileStore(t)
	fileVerStr := fmt.Sprintf("%d", (fileVersion + 1))
	if !strings.Contains(err.Error(), fileVerStr) {
		t.Fatalf("Expected error to report unsupported file version %q, got %v", fileVerStr, err)
	}

	// Restore the correct version.
	writeVersion(t, filepath.Join(testFSDefaultDatastore, "foo", msgFilesPrefix+"1"+datSuffix), fileVersion)

	// Overwrite the file version of the subscriptions store to an unsupported version
	writeVersion(t, filepath.Join(testFSDefaultDatastore, "foo", subsFileName), fileVersion+1)

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
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

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
	if testFSDisableBufferWriters {
		expected.BufferSize = 0
	}
	if testFSSetFDsLimit {
		expected.FileDescriptorsLimit = testFDsLimit
	}
	checkOpts(expected, opts)

	cs := storeCreateChannel(t, fs, "foo")
	ss := cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	// Now try to set the options in the constructor
	fs.Close()
	cleanupFSDatastore(t)

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
		ParallelRecovery:     5,
	}
	// Create the file with custom options
	fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &testDefaultStoreLimits,
		BufferSize(expected.BufferSize),
		CompactEnabled(expected.CompactEnabled),
		CompactFragmentation(expected.CompactFragmentation),
		CompactInterval(expected.CompactInterval),
		CompactMinFileSize(expected.CompactMinFileSize),
		DoCRC(expected.DoCRC),
		CRCPolynomial(expected.CRCPolynomial),
		DoSync(expected.DoSync),
		SliceConfig(100, 1024*1024, time.Second, "myscript.sh"),
		FileDescriptorsLimit(20),
		ParallelRecovery(5))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	fs.Close()
	fs, err = NewFileStore(testLogger, testFSDefaultDatastore, &testDefaultStoreLimits,
		AllOptions(&expected))
	if err != nil {
		t.Fatalf("Unexpected error on file store create: %v", err)
	}
	defer fs.Close()
	fs.RLock()
	opts = fs.opts
	fs.RUnlock()
	checkOpts(expected, opts)

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)

	fs.Close()
	cleanupFSDatastore(t)
	// Create the file with custom options, pass all of them at once
	fs, err = NewFileStore(testLogger, testFSDefaultDatastore, &testDefaultStoreLimits, AllOptions(&expected))
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

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	ss.RLock()
	opts = *ss.opts
	ss.RUnlock()
	checkOpts(expected, opts)
	fs.Close()

	expectError := func(opts *FileStoreOptions, errTxt string) {
		f, err := NewFileStore(testLogger, testFSDefaultDatastore, &testDefaultStoreLimits, AllOptions(opts))
		if f != nil {
			f.Close()
		}
		if err == nil || !strings.Contains(err.Error(), errTxt) {
			stackFatalf(t, "Expected error to contain %q, got %v", errTxt, err)
		}
	}
	badOpts := DefaultFileStoreOptions
	badOpts.BufferSize = -1
	expectError(&badOpts, "buffer size")
	badOpts = DefaultFileStoreOptions
	badOpts.CompactFragmentation = -1
	expectError(&badOpts, "compact fragmentation")
	badOpts.CompactFragmentation = 0
	expectError(&badOpts, "compact fragmentation")
	badOpts = DefaultFileStoreOptions
	badOpts.CompactInterval = -1
	expectError(&badOpts, "compact interval")
	badOpts.CompactInterval = 0
	expectError(&badOpts, "compact interval")
	badOpts = DefaultFileStoreOptions
	badOpts.CompactMinFileSize = -1
	expectError(&badOpts, "compact minimum file size")
	badOpts = DefaultFileStoreOptions
	badOpts.CRCPolynomial = 0
	expectError(&badOpts, "crc polynomial")
	badOpts.CRCPolynomial = 0xFFFFFFFF + 10
	expectError(&badOpts, "crc polynomial")
	badOpts = DefaultFileStoreOptions
	badOpts.FileDescriptorsLimit = -1
	expectError(&badOpts, "file descriptor")
	badOpts = DefaultFileStoreOptions
	badOpts.ParallelRecovery = -1
	expectError(&badOpts, "parallel recovery")
	badOpts.ParallelRecovery = 0
	expectError(&badOpts, "parallel recovery")
	badOpts = DefaultFileStoreOptions
	badOpts.SliceMaxMsgs = -1
	expectError(&badOpts, "slice max values")
	badOpts = DefaultFileStoreOptions
	badOpts.SliceMaxBytes = -1
	expectError(&badOpts, "slice max values")
	badOpts = DefaultFileStoreOptions
	badOpts.SliceMaxAge = -1
	expectError(&badOpts, "slice max values")
}

func TestFSLimitsOnRecovery(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

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
		cs := storeCreateChannel(t, fs, channelName)
		seq := uint64(1)

		// Create several subscriptions per channel.
		for s := 0; s < subsCount; s++ {
			storeSub(t, cs, channelName)
		}

		for m := 0; m < msgCount; m++ {
			msg := storeMsg(t, cs, channelName, seq, payload)
			seq++
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
	fs, state := newFileStore(t, testFSDefaultDatastore, &limit, SliceConfig(1, int64(maxMsgsAfterRecovery), 0, ""))
	defer fs.Close()

	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	rcs := state.Channels

	// Make sure that all our channels are recovered.
	if len(rcs) != chanCount {
		t.Fatalf("Unexpected count of recovered channels. Expected %v, got %v", chanCount, len(rcs))
	}
	var (
		recMsg   int
		recBytes uint64
	)
	// Make sure that all our subscriptions are recovered.
	for _, rc := range rcs {
		if len(rc.Subscriptions) != subsCount {
			t.Fatalf("Unexpected count of recovered subs. Expected %v, got %v", subsCount, len(rc.Subscriptions))
		}
		m, b := msgStoreState(t, rc.Channel.Msgs)
		recMsg += m
		recBytes += b
	}
	// Messages limits, however, are enforced on restart.
	if recMsg != chanCount*maxMsgsAfterRecovery {
		t.Fatalf("Unexpected count of recovered msgs. Expected %v, got %v", chanCount*maxMsgsAfterRecovery, recMsg)
	}
	if recBytes != uint64(chanCount)*expectedMsgBytesAfterRecovery {
		t.Fatalf("Unexpected count of recovered bytes: Expected %v, got %v", uint64(chanCount)*expectedMsgBytesAfterRecovery, recBytes)
	}

	// Now check that any new addition would be rejected
	if _, err := fs.CreateChannel("new.channel"); err == nil {
		t.Fatal("Expected trying to create a new channel to fail")
	}
	rc := rcs["channel.1"]
	if rc == nil {
		t.Fatal("Expected channel.1 to exist")
	}
	channelOne := rc.Channel
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         "inbox",
		AckInbox:      "ackinbox",
		AckWaitInSecs: 10,
	}
	if err := channelOne.Subs.CreateSub(sub); err == nil {
		t.Fatal("Expected trying to create a new subscription to fail")
	}

	// Store one message
	lastMsg := storeMsg(t, channelOne, "channel.1", uint64(msgCount+1), payload)

	// Check limits (should be 4 msgs)
	recMsg, recBytes = msgStoreState(t, channelOne.Msgs)
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
	fs, state = newFileStore(t, testFSDefaultDatastore, &limit)
	defer fs.Close()
	for _, rc := range state.Channels {
		recMsg, recBytes = msgStoreState(t, rc.Channel.Msgs)
		if recMsg != 0 || recBytes != 0 {
			t.Fatalf("There should be no message recovered, got %v, %v bytes", recMsg, recBytes)
		}
	}
}

func TestFSBadClientFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the client's file
	fileName := filepath.Join(testFSDefaultDatastore, clientsFileName)
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

func TestFSBadServerFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Create a valid store file first
	fs := createDefaultFileStore(t)
	// Close it
	fs.Close()

	// Delete the server's file
	fileName := filepath.Join(testFSDefaultDatastore, serverFileName)
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

func TestFSAddClientError(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Test failure of AddClient (generic tested in common_test.go)
	// Close the client file to cause error
	fs.clientsFile.handle.Close()
	// Should fail
	if c, err := fs.AddClient(&spb.ClientInfo{ID: "c1", HbInbox: "hbInbox"}); err == nil {
		t.Fatal("Expected error, got none")
	} else if c != nil {
		t.Fatalf("Should not have gotten a client back, got %v", c)
	}
}

func TestFSCompactClientFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

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
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete half.
	for i := 0; i < threshold-1; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
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
	storeDeleteClient(t, fs, cid)
	// One client less, 0 del records after a compaction
	check(fs, threshold, 0)

	// Make sure we don't compact too often
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Delete almost all of them
	for i := 0; i < total-1; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		storeDeleteClient(t, fs, cid)
	}
	// The number of clients should be same than before + 1,
	// and lots of delete
	check(fs, threshold+1, total-1)
	// Now wait for the interval and a bit more
	time.Sleep(1500 * time.Millisecond)
	// Delete one more, compaction should occur
	cid = fmt.Sprintf("cid_%d", 2*total)
	storeDeleteClient(t, fs, cid)
	// One less client, 0 delete records after compaction
	check(fs, threshold, 0)

	fs.Close()
	// Wipe out
	cleanupFSDatastore(t)
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
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)

	fs.Close()
	// Wipe out
	cleanupFSDatastore(t)
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
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)
}

func TestFSDoSync(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	total := 100
	dur := [2]time.Duration{}
	seq := uint64(1)

	for i := 0; i < 2; i++ {
		sOpts := DefaultFileStoreOptions
		sOpts.DoSync = true
		if i == 1 {
			sOpts.DoSync = false
		}
		fs, _ := newFileStore(t, testFSDefaultDatastore, &testDefaultStoreLimits, AllOptions(&sOpts))
		defer fs.Close()

		cs := storeCreateChannel(t, fs, "foo")
		subID := storeSub(t, cs, "foo")

		msg := make([]byte, 1024)
		start := time.Now()
		// Send more message when fsync is disabled. It should still be faster,
		// and would catch if bug in code where we always do fsync, regardless
		// of option.
		for j := 0; j < total+(i*total/10); j++ {
			m := storeMsg(t, cs, "foo", seq, msg)
			seq++
			cs.Msgs.Flush()
			storeSubPending(t, cs, "foo", subID, m.Sequence)
			cs.Subs.Flush()
		}
		dur[i] = time.Since(start)

		fs.Close()
		cleanupFSDatastore(t)
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
	return len(p), nil
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
	if !strings.Contains(err.Error(), errReturned.Error()) {
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

	// Append zeros to the end of buffer
	for i := 0; i < recordHeaderSize+2; i++ {
		b = append(b, 0)
	}
	// Don't call setContent since this would reset the read position
	r.content = b
	_, _, _, err = readRecord(r, buf, true, crc32.IEEETable, true)
	if err != errNeedRewind {
		t.Fatalf("Expected error %v, got %v", errNeedRewind, err)
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

	// Check for marshaling error
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

// Test with 2 processes trying to acquire the lock are tested
// in the server package (FT tests) with coverpkg set to stores
// for code coverage.
func TestFSGetExclusiveLock(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

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

	fLockName := filepath.Join(testFSDefaultDatastore, lockFileName)
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

func TestFSNegativeLimitsOnCreate(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	limits := DefaultStoreLimits
	limits.MaxMsgs = -1000
	if fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &limits); fs != nil || err == nil {
		if fs != nil {
			fs.Close()
		}
		t.Fatal("Should have failed to create store with a negative limit")
	}
}

func TestFSParallelRecovery(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, FileDescriptorsLimit(50))
	defer fs.Close()
	numChannels := 100
	numMsgsPerChannel := 1000
	msg := []byte("msg")
	seq := uint64(1)
	for i := 0; i < numChannels; i++ {
		chanName := fmt.Sprintf("foo.%v", i)
		cs := storeCreateChannel(t, fs, chanName)
		for j := 0; j < numMsgsPerChannel; j++ {
			storeMsg(t, cs, chanName, seq, msg)
			seq++
		}
	}
	fs.Close()

	fs, state := openDefaultFileStore(t, ParallelRecovery(10))
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state, got none")
	}
	n := 0
	for _, rc := range state.Channels {
		m, _ := msgStoreState(t, rc.Channel.Msgs)
		n += m
	}
	if n != numChannels*numMsgsPerChannel {
		t.Fatalf("Expected %v messages, got %v", numChannels*numMsgsPerChannel, n)
	}
	fs.Close()

	numRoutines := runtime.NumGoroutine()
	// Make several channels fail to recover
	fname := fmt.Sprintf("%s.1.%s", msgFilesPrefix, datSuffix)
	ioutil.WriteFile(filepath.Join(testFSDefaultDatastore, "foo.50", fname), []byte("dummy"), 0666)
	ioutil.WriteFile(filepath.Join(testFSDefaultDatastore, "foo.51", fname), []byte("dummy"), 0666)
	s, err := NewFileStore(testLogger, testFSDefaultDatastore, &testDefaultStoreLimits, ParallelRecovery(10))
	if err == nil {
		_, err = s.Recover()
		s.Close()
	}
	if err == nil {
		t.Fatalf("Recovery should have failed")
	}
	numRoutinesAfter := runtime.NumGoroutine()
	// A defect was causing properly recovered channels to not be closed
	// resulting in go routines left running. Check that at least some
	// are being closed (not using strict > 0 to avoid flapping tests).
	if left := numRoutinesAfter - numRoutines; left > 10 {
		t.Fatalf("Too many go routines left: %v", left)
	}
}

func TestFSFilesClosedOnRecovery(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer s.Close()

	limits := testDefaultStoreLimits
	limits.MaxAge = 15 * time.Millisecond
	if err := s.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", err)
	}

	seq := uint64(1)
	for i := 0; i < 5; i++ {
		cname := fmt.Sprintf("foo_%d", (i + 1))
		cs := storeCreateChannel(t, s, cname)
		m1 := storeMsg(t, cs, cname, seq, []byte("hello"))
		seq++
		m2 := storeMsg(t, cs, cname, seq, []byte("hello"))
		seq++
		subid := storeSub(t, cs, "foo")
		storeSubPending(t, cs, cname, subid, m1.Sequence, m2.Sequence)
	}
	s.Close()

	// Wait more than expiration time
	time.Sleep(50 * time.Millisecond)

	s, _ = openDefaultFileStoreWithLimits(t, &limits)
	defer s.Close()
	s.fm.Lock()
	for _, f := range s.fm.files {
		// Server and clients file are expected to be opened, others not.
		if strings.HasSuffix(f.name, serverFileName) || strings.HasSuffix(f.name, clientsFileName) {
			continue
		}
		if state := atomic.LoadInt32(&f.state); state != fileClosed {
			s.fm.Unlock()
			t.Fatalf("Expected file %q state to be %v, it was %v", f.name, fileClosed, state)
		}
	}
	s.fm.Unlock()
}

func TestFSClientFileWithExtraZeros(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c1, err := s.AddClient(&spb.ClientInfo{ID: "me", HbInbox: "hbInbox"})
	if err != nil {
		t.Fatalf("Error adding client: %v", err)
	}
	s.RLock()
	fname := s.clientsFile.name
	s.RUnlock()

	s.Close()

	f, err := openFileWithFlags(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	b := make([]byte, recordHeaderSize)
	if _, err := f.Write(b); err != nil {
		t.Fatalf("Error adding zeros: %v", err)
	}
	f.Close()

	// Reopen file store
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	if rs == nil {
		t.Fatal("Expected to recover state")
	}
	if len(rs.Clients) != 1 {
		t.Fatalf("Expected to recover 1 client, got %v", len(rs.Clients))
	}
	rc := rs.Clients[0]
	if !reflect.DeepEqual(rc, c1) {
		t.Fatalf("Expected client %v, got %v", c1, rc)
	}
	// Add one more client
	c2, err := s.AddClient(&spb.ClientInfo{ID: "me2", HbInbox: "hbInbox2"})
	if err != nil {
		t.Fatalf("Error adding client: %v", err)
	}
	s.Close()

	// Reopen file store
	s, rs = openDefaultFileStore(t)
	defer s.Close()
	if rs == nil {
		t.Fatal("Expected to recover state")
	}
	if len(rs.Clients) != 2 {
		t.Fatalf("Expected to recover 2 client, got %v", len(rs.Clients))
	}
	for _, rc := range rs.Clients {
		// rs.Clients is an array but created from a map, so order is
		// not guaranteed.
		var c *Client
		if rc.ID == "me" {
			c = c1
		} else {
			c = c2
		}
		if !reflect.DeepEqual(rc, c) {
			t.Fatalf("Expected client %v, got %v", c, rc)
		}
	}
}

func TestFSDeleteChannel(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	checkDir := func(channelName string, shouldExist bool) {
		_, err := os.Stat(path.Join(testFSDefaultDatastore, channelName))
		if shouldExist && err != nil {
			stackFatalf(t, "Directory %q should exist", channelName)
		} else if !shouldExist && err == nil {
			stackFatalf(t, "Directory %q should not exist", channelName)
		}
	}

	// Create 2 channels
	storeCreateChannel(t, s, "foo")
	checkDir("foo", true)
	storeCreateChannel(t, s, "bar")
	checkDir("bar", true)

	if err := s.DeleteChannel("foo"); err != nil {
		t.Fatalf("Error deleting channel: %v", err)
	}
	// Directory "foo" should no longer exist
	checkDir("foo", false)
	// But bar should still be there
	checkDir("bar", true)

	// Should be able to recreate same channel
	storeCreateChannel(t, s, "foo")
	checkDir("foo", true)
}

func TestFSTruncateOnUnexpectedEOFLock(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// When opening with TruncateUnexpectedEOF(true), the store
	// will create a special file to know that it was last opened
	// with that. We want the user to no use that option as
	// a default param, so a restart of the store should not have
	// it. Only then that special file will be deleted.
	s := createDefaultFileStore(t, TruncateUnexpectedEOF(true))
	s.Close()

	// Restarting the server with that option should fail
	s, err := NewFileStore(testLogger, testFSDefaultDatastore, nil, TruncateUnexpectedEOF(true))
	if err == nil || s != nil {
		s.Close()
		t.Fatalf("Expected error opening the store")
	}

	// Open without the option should work ok.
	s, _ = openDefaultFileStore(t)
	s.Close()

	// Now one can use the option again
	s, _ = openDefaultFileStore(t, TruncateUnexpectedEOF(true))
	s.Close()
}
