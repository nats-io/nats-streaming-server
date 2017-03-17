// Copyright 2017 Apcera Inc. All rights reserved.
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
// quicker than if left to the operating systen.
func CreateLockFile(file string) (LockFile, error) {
	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}
	spec := &syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, spec); err != nil {
		if err == syscall.EAGAIN || err == syscall.EACCES {
			err = ErrAlreadyLocked
		}
		// TODO: If error is not ErrAlreadyLocked, it may mean that
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
