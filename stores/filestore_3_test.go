// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

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

	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", []byte("msg"))

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

	cs = storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", []byte("msg"))

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
	cs := storeCreateChannel(t, fs, "foo")
	// Create slices
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", msg)
	}

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

	fs, state := openDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	cs = getRecoveredChannel(t, state, "foo")
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
	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", msg)
	storeMsg(t, cs, "foo", msg)

	if m := msgStoreFirstMsg(t, cs.Msgs); m.Sequence != 1 {
		t.Fatalf("Unexpected first message: %v", m)
	}
	if m := msgStoreLastMsg(t, cs.Msgs); m.Sequence != 2 {
		t.Fatalf("Unexpected last message: %v", m)
	}
	// Wait for all messages to expire
	timeout := time.Now().Add(3 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if n, _ := msgStoreState(t, cs.Msgs); n == 0 {
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
	storeMsg(t, cs, "foo", msg)
	storeMsg(t, cs, "foo", msg)
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
	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", msg)

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
	cs := storeCreateChannel(t, fs, "foo")
	// Store messages 1, 2, 3
	for i := 0; i < 3; i++ {
		storeMsg(t, cs, "foo", msg)
	}

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
	cs := storeCreateChannel(t, fs, "foo")
	msg := storeMsg(t, cs, "foo", payload)

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
	lm := msgStoreLookup(t, ms, msg.Sequence)
	if !reflect.DeepEqual(msg, lm) {
		t.Fatalf("Expected lookup message to be %v, got %v", msg, lm)
	}
	// Flush store so we removed from buffered messages
	ms.Flush()
	// As long as we call lookup, message should stay in cache
	closeFile := true
	end := time.Now().Add(2 * time.Duration(cacheTTL))
	for time.Now().Before(end) {
		lm := msgStoreLookup(t, ms, msg.Sequence)
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
	lm, err := ms.Lookup(msg.Sequence)
	if lm != nil || err == nil {
		t.Fatalf("Unexpected message: %v", lm)
	}

	// Use another channel
	end = time.Now().Add(2 * bkgTasksSleepDuration)
	i := 0
	cs = storeCreateChannel(t, fs, "bar")
	for time.Now().Before(end) {
		storeMsg(t, cs, "bar", payload)
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

	storeCreateChannel(t, fs, "foo")
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

	fLockName := filepath.Join(defaultDataStore, lockFileName)
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

func TestFSNegativeLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	limits := DefaultStoreLimits
	limits.MaxMsgs = -1000
	if fs, err := NewFileStore(testLogger, defaultDataStore, &limits); fs != nil || err == nil {
		if fs != nil {
			fs.Close()
		}
		t.Fatal("Should have failed to create store with a negative limit")
	}
	fs := createDefaultFileStore(t)
	defer fs.Close()

	testNegativeLimit(t, fs)
}

func TestFSLimitWithWildcardsInConfig(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)
	fs := createDefaultFileStore(t)
	defer fs.Close()
	testLimitWithWildcardsInConfig(t, fs)
}

func TestFSParallelRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, FileDescriptorsLimit(50))
	defer fs.Close()
	numChannels := 100
	numMsgsPerChannel := 1000
	msg := []byte("msg")
	for i := 0; i < numChannels; i++ {
		chanName := fmt.Sprintf("foo.%v", i)
		cs := storeCreateChannel(t, fs, chanName)
		for j := 0; j < numMsgsPerChannel; j++ {
			storeMsg(t, cs, chanName, msg)
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

	// Make several channels fail to recover
	fname := fmt.Sprintf("%s.1.%s", msgFilesPrefix, datSuffix)
	ioutil.WriteFile(filepath.Join(defaultDataStore, "foo.50", fname), []byte("dummy"), 0666)
	ioutil.WriteFile(filepath.Join(defaultDataStore, "foo.51", fname), []byte("dummy"), 0666)
	if _, _, err := newFileStore(t, defaultDataStore, &testDefaultStoreLimits, ParallelRecovery(10)); err == nil {
		t.Fatalf("Recovery should have failed")
	}
}

func TestFSDeleteSubError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// No buffer for this test
	fs := createDefaultFileStore(t, BufferSize(0))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	subid := storeSub(t, cs, "foo")
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	ss.Unlock()

	if err := ss.DeleteSub(subid); err == nil {
		t.Fatal("Expected error on sub delete, got none")
	}
}
