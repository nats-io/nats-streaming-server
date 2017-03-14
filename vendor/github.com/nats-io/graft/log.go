// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"io/ioutil"
	"os"
)

type envelope struct {
	SHA, Data []byte
}
type persistentState struct {
	CurrentTerm uint64
	VotedFor    string
}

func (n *Node) initLog(path string) error {
	if log, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0660); err != nil {
		return err
	} else {
		log.Close()
	}

	n.logPath = path

	ps, err := n.readState(path)
	if err != nil && err != LogNoStateErr {
		return err
	}

	if ps != nil {
		n.setTerm(ps.CurrentTerm)
		n.setVote(ps.VotedFor)
	}

	return nil
}

func (n *Node) closeLog() error {
	err := os.Remove(n.logPath)
	n.logPath = ""
	return err
}

func (n *Node) writeState() error {
	n.mu.Lock()
	ps := persistentState{
		CurrentTerm: n.term,
		VotedFor:    n.vote,
	}
	logPath := n.logPath
	n.mu.Unlock()

	buf, err := json.Marshal(ps)
	if err != nil {
		return err
	}

	// Set a SHA1 to test for corruption on read
	env := envelope{
		SHA:  sha1.New().Sum(buf),
		Data: buf,
	}

	toWrite, err := json.Marshal(env)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(logPath, toWrite, 0660)
}

func (n *Node) readState(path string) (*persistentState, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) <= 0 {
		return nil, LogNoStateErr
	}

	env := &envelope{}
	if err := json.Unmarshal(buf, env); err != nil {
		return nil, err
	}

	// Test for corruption
	sha := sha1.New().Sum(env.Data)
	if !bytes.Equal(sha, env.SHA) {
		return nil, LogCorruptErr
	}

	ps := &persistentState{}
	if err := json.Unmarshal(env.Data, ps); err != nil {
		return nil, err
	}
	return ps, nil
}
