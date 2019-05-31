// Copyright 2017-2019 The NATS Authors
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

package util

import (
	"flag"
	"fmt"
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
	if !parentProcess {
		// As a child process that got the lock, create a file
		// that the parent can check the existence of to proceed.
		wf, err := os.Create("test_child.txt")
		if err != nil {
			t.Fatalf("Child process unable to create file: %v", err)
		}
		wf.Close()
		defer os.Remove(wf.Name())
	}

	// If we are the parent process, try to spawn a new process
	// that will try to grab the lock for same file
	if parentProcess {
		out, err := exec.Command(os.Args[0], "-lockFile", fname,
			"-test.v", "-test.run=TestLockFile$").CombinedOutput()
		if err == nil {
			t.Fatal("CreateLockFile should have failed while other process holds lock")
		}
		if !strings.Contains(string(out), ErrUnableToLockNow.Error()) {
			t.Fatalf("Error should contain: %q, got %q", ErrUnableToLockNow.Error(), string(out))
		}
	} else {
		// If the child process, wait a bit while parent process
		// tries to get the lock.
		for {
			if _, err := os.Stat("test_parent.txt"); err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
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
		errCh := make(chan error, 1)
		go func() {
			defer wg.Done()
			out, err := exec.Command(os.Args[0], "-lockFile", fname,
				"-test.v", "-test.run=TestLockFile$").CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("Other process should have been able to get the lock, got %v - %v",
					err, out)
			}
		}()
		// Give a chance for the child process to run
		for {
			if _, err := os.Stat("test_child.txt"); err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
		// The parent process should now be the one that fails
		f, err := CreateLockFile(fname)
		if err == nil {
			f.Close()
			t.Fatal("Expected CreateLockFile to fail, it did not")
		}
		// Write a file to indicate to the child process that we are done
		wf, err := os.Create("test_parent.txt")
		if err != nil {
			t.Fatalf("Parent process unable to create file: %v", err)
		}
		wf.Close()
		defer os.Remove(wf.Name())
		wg.Wait()
		select {
		case e := <-errCh:
			t.Fatal(e.Error())
		default:
		}
	}
}

func TestLockFileFatalErrors(t *testing.T) {
	// Specifying a wrong path should return an error
	lf, err := CreateLockFile("dummy/lock.lck")
	if lf != nil || err == nil {
		if lf != nil {
			lf.Close()
		}
		t.Fatalf("Expected no file and error, got %v, %v", lf, err)
	}

	// Try with permission error. First, create a file
	fileName := "test.lck"
	defer os.Remove(fileName)
	defer os.Chmod(fileName, 0666)
	file, err := os.Create(fileName)
	if err != nil {
		t.Fatalf("Unable to create file: %v", err)
	}
	file.Close()
	os.Chmod(fileName, 0400)
	lf, err = CreateLockFile(fileName)
	if lf != nil || err == nil {
		if lf != nil {
			lf.Close()
		}
		t.Fatalf("Expected no file and error, got %v, %v", lf, err)
	}
}
