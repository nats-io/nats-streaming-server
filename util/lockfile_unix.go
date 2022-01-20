// Copyright 2017-2022 The NATS Authors
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

//go:build !windows
// +build !windows

package util

import (
	"io"
	"os"
	"sync"
	"syscall"
)

type lockFile struct {
	sync.Mutex
	f *os.File
}

// CreateLockFile attempt to lock the given file, creating it
// if necessary. On success, the file is returned, otherwise
// an error is returned.
// The file returned should be closed to release the lock
// quicker than if left to the operating system.
func CreateLockFile(file string) (LockFile, error) {
	f, err := os.Create(file)
	if err != nil {
		// Consider those fatal, others may be considered transient
		// (for instance FD limit reached, etc...)
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, err
		}
		return nil, ErrUnableToLockNow
	}
	spec := &syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, spec); err != nil {
		// Try to gather all errors that we deem transient and return
		// ErrUnableToLockNow in this case to indicate the caller that
		// the lock could not be acquired at this time but it could
		// try later.
		// Basing this from possible ERRORS from this page:
		// http://pubs.opengroup.org/onlinepubs/009695399/functions/fcntl.html
		if err == syscall.EAGAIN || err == syscall.EACCES ||
			err == syscall.EINTR || err == syscall.ENOLCK {
			err = ErrUnableToLockNow
		}
		// TODO: If error is not ErrUnableToLockNow, it may mean that
		// the call is not supported on that platform, etc...
		// We should have another level of verification, for instance
		// check content of the lockfile is not being updated by the
		// owner of the file, etc...
		f.Close()
		return nil, err
	}
	return &lockFile{f: f}, nil
}

// Close implements the LockFile interface
func (lf *lockFile) Close() error {
	lf.Lock()
	defer lf.Unlock()
	if lf.f == nil {
		return nil
	}
	spec := &syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
	}
	err := syscall.FcntlFlock(lf.f.Fd(), syscall.F_SETLK, spec)
	err = CloseFile(err, lf.f)
	lf.f = nil
	return err
}

// IsClosed implements the LockFile interface
func (lf *lockFile) IsClosed() bool {
	lf.Lock()
	defer lf.Unlock()
	return lf.f == nil
}
