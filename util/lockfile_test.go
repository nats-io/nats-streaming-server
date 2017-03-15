package util

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

var lockFileName = flag.String("lockFile", "", "")

func TestLockFile(t *testing.T) {
	// To test locking, we need to spawn a new process to check that
	// the file cannot be locked by another process.
	// We have the parent lock the file and try to have a child process
	// lock it, then we reverse the test: this is just for code coverage
	// that would otherwise not show the code coverage for when the
	// file is already locked.

	fname := *lockFileName
	parentProcess := fname == ""
	if fname == "" {
		fname = "test.lck"
		defer os.Remove(fname)
	}

	// First test with invalid file
	if parentProcess {
		f, err := CreateLockFile("dummy/test.lck")
		if err == nil {
			f.Close()
			t.Fatal("Expected to fail for invalid file name")
		}
	}

	f, err := CreateLockFile(fname)
	if err != nil {
		t.Fatalf("Error creating lock file: %v", err)
	}
	defer f.Close()

	// If we are the parent process, try to spawn a new process
	// that will try to grab the lock for same file
	if parentProcess {
		out, err := exec.Command(os.Args[0], "-lockFile", fname,
			"-test.v", "-test.run=TestLockFile$").CombinedOutput()
		if err == nil {
			t.Fatal("CreateLockFile should have failed while other process holds lock")
		}
		if !strings.Contains(string(out), ErrAlreadyLocked.Error()) {
			t.Fatalf("Error should contain: %q, got %q", ErrAlreadyLocked.Error(), string(out))
		}
	} else {
		// If the child process, wait a bit while parent process
		// tries to get the lock.
		time.Sleep(2 * time.Second)
	}
	// Release the lock
	if err := f.Close(); err != nil {
		t.Fatalf("Unexpected error on close: %v", err)
	}
	// Check state
	if !f.IsClosed() {
		t.Fatal("File should be closed")
	}
	if parentProcess {
		// Try again with different process, it should work now.
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := exec.Command(os.Args[0], "-lockFile", fname,
				"-test.v", "-test.run=TestLockFile$").CombinedOutput()
			if err != nil {
				t.Fatalf("Other process should have been able to get the lock, got %v - %v",
					err, out)
			}
		}()
		// Give a chance for the child process to run
		time.Sleep(time.Second)
		// The parent process should now be the one that fails
		f, err := CreateLockFile(fname)
		if err == nil {
			f.Close()
			t.Fatal("Expected CreateLockFile to fail, it did not")
		}
		wg.Wait()
	}
}
