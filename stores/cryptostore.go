// Copyright 2018 The NATS Authors
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
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"sync"

	"github.com/nats-io/go-nats-streaming/pb"
	"golang.org/x/crypto/chacha20poly1305"
)

// CryptStore specific errors
var (
	ErrCryptoStoreRequiresKey = errors.New("encryption key required")
)

const (
	// CryptoStoreEnvKeyName is the environment variable name
	// that the CryptoStore looks up if no key is passed as
	// a parameter.
	CryptoStoreEnvKeyName = "NATS_STREAMING_ENCRYPTION_KEY"
)

// CryptoStore is a store wrapping a store implementation
// and adds encryption support.
type CryptoStore struct {
	sync.Mutex
	Store

	// These are set when the store is created. They are then
	// passed to a CryptoMsgStore so that there is no need
	// to reference back to these.
	gcm            cipher.AEAD
	nonceSize      int
	cryptoOverhead int
}

// CryptoMsgStore is a store wrappeing a SubStore implementation
// and adds encryption support.
type CryptoMsgStore struct {
	sync.Mutex
	MsgStore
	gcm            cipher.AEAD
	cryptoOverhead int
	nonce          []byte
	nonceSize      int
	nonceUsed      int64
	nonceLimit     int64
}

// NewCryptoStore returns a CryptoStore instance with
// given underlying store.
func NewCryptoStore(s Store, encryptionKey []byte) (*CryptoStore, error) {
	// Always check env variable first
	key := []byte(os.Getenv(CryptoStoreEnvKeyName))
	if len(key) == 0 {
		key = encryptionKey
		if len(key) == 0 {
			return nil, ErrCryptoStoreRequiresKey
		}
	}
	defer func() {
		for i := 0; i < len(encryptionKey); i++ {
			encryptionKey[i] = 'x'
		}
	}()

	cs := &CryptoStore{Store: s}

	h := sha256.New()
	h.Write(key)
	keyHash := h.Sum(nil)
	gcm, err := chacha20poly1305.New(keyHash)
	if err != nil {
		return nil, err
	}
	cs.gcm = gcm
	cs.cryptoOverhead = gcm.Overhead()
	cs.nonceSize = gcm.NonceSize()
	return cs, nil
}

func (cs *CryptoStore) newCryptoMsgStore(ms MsgStore) *CryptoMsgStore {
	cms := &CryptoMsgStore{
		MsgStore:       ms,
		gcm:            cs.gcm,
		nonceSize:      cs.nonceSize,
		cryptoOverhead: cs.cryptoOverhead,
	}
	cms.generateNewNonce()
	return cms
}

// Recover implements the Store interface
func (cs *CryptoStore) Recover() (*RecoveredState, error) {
	cs.Lock()
	defer cs.Unlock()
	rs, err := cs.Store.Recover()
	if rs == nil || err != nil {
		return rs, err
	}
	for _, rc := range rs.Channels {
		rc.Channel.Msgs = cs.newCryptoMsgStore(rc.Channel.Msgs)
	}
	return rs, nil
}

// CreateChannel implements the Store interface
func (cs *CryptoStore) CreateChannel(channel string) (*Channel, error) {
	cs.Lock()
	defer cs.Unlock()

	c, err := cs.Store.CreateChannel(channel)
	if err != nil {
		return nil, err
	}
	c.Msgs = cs.newCryptoMsgStore(c.Msgs)
	return c, nil
}

// Store implements the MsgStore interface
func (cms *CryptoMsgStore) Store(msg *pb.MsgProto) (uint64, error) {
	if len(msg.Data) == 0 {
		return cms.MsgStore.Store(msg)
	}
	ed, err := cms.encrypt(msg.Data)
	if err != nil {
		return 0, err
	}
	msg.Data = ed
	return cms.MsgStore.Store(msg)
}

func (cms *CryptoMsgStore) generateNewNonce() error {
	cms.nonce = make([]byte, cms.nonceSize)
	if _, err := io.ReadFull(rand.Reader, cms.nonce); err != nil {
		return err
	}
	cms.nonceUsed = 0
	cms.nonceLimit = mrand.Int63n(1e6) + 100000
	return nil
}

func (cms *CryptoMsgStore) encrypt(data []byte) ([]byte, error) {
	// We can't reuse a buffer since when we pass the data to
	// the underlying store, we don't know if this is retained
	// in some cache, etc..
	buf := make([]byte, cms.nonceSize+cms.cryptoOverhead+len(data))
	cms.Lock()
	copy(buf, cms.nonce)
	copy(buf[cms.nonceSize:], data)
	dst := buf[cms.nonceSize : cms.nonceSize+len(data)]
	ed := cms.gcm.Seal(dst[:0], cms.nonce, dst, nil)
	cms.nonceUsed++
	if cms.nonceUsed >= cms.nonceLimit {
		cms.generateNewNonce()
	}
	cms.Unlock()
	return buf[:cms.nonceSize+len(ed)], nil
}

func (cms *CryptoMsgStore) decryptedMsg(m *pb.MsgProto) (*pb.MsgProto, error) {
	if len(m.Data) <= cms.nonceSize {
		return nil, fmt.Errorf("trying to decrypt data that is not (len=%v)", len(m.Data))
	}
	// When decrypting we can't do it in the original buffer because
	// the store's copy may be in a cache and so this would decipher
	// the encrypted copy and during the next call to decryptedMsg()
	// for the same message, there would be attempt to decrypt something
	// that is not, which would fail.
	dd, err := cms.gcm.Open(nil, m.Data[:cms.nonceSize], m.Data[cms.nonceSize:], nil)
	if err != nil {
		return nil, err
	}
	// Store owns the message, so make a copy before returning
	retMsg := *m
	retMsg.Data = dd
	return &retMsg, nil
}

// Lookup implements the MsgStore interface
func (cms *CryptoMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	m, err := cms.MsgStore.Lookup(seq)
	if m == nil || m.Data == nil || err != nil {
		return m, err
	}
	return cms.decryptedMsg(m)
}

// FirstMsg implements the MsgStore interface
func (cms *CryptoMsgStore) FirstMsg() (*pb.MsgProto, error) {
	m, err := cms.MsgStore.FirstMsg()
	if m == nil || m.Data == nil || err != nil {
		return m, err
	}
	return cms.decryptedMsg(m)
}

// LastMsg implements the MsgStore interface
func (cms *CryptoMsgStore) LastMsg() (*pb.MsgProto, error) {
	m, err := cms.MsgStore.LastMsg()
	if m == nil || m.Data == nil || err != nil {
		return m, err
	}
	return cms.decryptedMsg(m)
}
