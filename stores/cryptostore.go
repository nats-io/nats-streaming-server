// Copyright 2018-2019 The NATS Authors
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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/stan.go/pb"
	"golang.org/x/crypto/chacha20poly1305"
)

// CryptStore specific errors
var (
	ErrCryptoStoreRequiresKey = errors.New("encryption key required")
	ErrCipherNotSupported     = errors.New("encryption cipher not supported")
)

const (
	// CryptoStoreEnvKeyName is the environment variable name
	// that the CryptoStore looks up if no key is passed as
	// a parameter.
	CryptoStoreEnvKeyName = "NATS_STREAMING_ENCRYPTION_KEY"

	// CryptoCipherAES is the name of the AES cipher to use for encryption
	CryptoCipherAES = "AES"

	// CryptoCipherChaChaPoly is the name of the ChaChaPoly cipher to use for encryption
	CryptoCipherChaChaPoly = "CHACHA"

	// CryptoCipherAutoSelect if passed to NewCryptoStore() will cause the cipher to
	// be auto-selected based on the platform the executable is built for.
	CryptoCipherAutoSelect = ""
)

// These constants define a code for each of the supported ciphers
const (
	CryptoCodeAES    = byte(1)
	CryptoCodeChaCha = byte(2)
)

// CryptoStore is a store wrapping a store implementation
// and adds encryption support.
type CryptoStore struct {
	sync.Mutex
	Store
	code byte
	mkh  []byte
}

// CryptoMsgStore is a store wrappeing a SubStore implementation
// and adds encryption support.
type CryptoMsgStore struct {
	sync.Mutex
	MsgStore
	eds *EDStore
}

// EDStore provides encryption and decryption of data
type EDStore struct {
	code           byte
	gcm            cipher.AEAD // Use this one to encrypt
	aesgcm         cipher.AEAD // This is to decrypt data encrypted with this AES cipher
	chachagcm      cipher.AEAD // This is to decrypt data encrypted with this Chacha cipher
	cryptoOverhead int
	nonce          []byte
	nonceSize      int
}

// NewEDStore returns an instance of EDStore that adds Encrypt/Decrypt
// capabilities.
func NewEDStore(encryptionCipher string, encryptionKey []byte, idx uint64) (*EDStore, error) {
	code, keyHash, err := createMasterKeyHash(encryptionCipher, encryptionKey)
	if err != nil {
		return nil, err
	}
	s, err := newEDStore(code, keyHash, idx)
	if err != nil {
		return nil, err
	}
	// On success, erase the key
	for i := 0; i < len(encryptionKey); i++ {
		encryptionKey[i] = 'x'
	}
	return s, nil
}

func createMasterKeyHash(encryptionCipher string, encryptionKey []byte) (byte, []byte, error) {
	var code byte
	if encryptionCipher != CryptoCipherAutoSelect {
		switch strings.ToUpper(encryptionCipher) {
		case CryptoCipherAES:
			code = CryptoCodeAES
		case CryptoCipherChaChaPoly:
			code = CryptoCodeChaCha
		default:
			return 0, nil, ErrCipherNotSupported
		}
	} else {
		// Otherwise default to AES on intel (there is hardware
		// acceleration for that) and chacha20poly1305 on ARM
		// (the two arch'es that we build docker images for).
		if runtime.GOARCH == "amd64" || runtime.GOARCH == "386" {
			code = CryptoCodeAES
		} else {
			code = CryptoCodeChaCha
		}
	}
	// Always check env variable first
	key := []byte(os.Getenv(CryptoStoreEnvKeyName))
	if len(key) == 0 {
		key = encryptionKey
		if len(key) == 0 {
			return 0, nil, ErrCryptoStoreRequiresKey
		}
	}
	h := sha256.New()
	h.Write(key)
	return code, h.Sum(nil), nil
}

func newEDStore(cipherCode byte, keyHash []byte, idx uint64) (*EDStore, error) {
	s := &EDStore{code: cipherCode}

	block, err := aes.NewCipher(keyHash)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	s.aesgcm = gcm

	s.nonceSize = gcm.NonceSize()
	if s.nonceSize < 8 {
		return nil, fmt.Errorf("nonce size too small: %v", s.nonceSize)
	}
	s.cryptoOverhead = gcm.Overhead()

	gcm, err = chacha20poly1305.New(keyHash)
	if err != nil {
		return nil, err
	}
	s.chachagcm = gcm
	if gcm.NonceSize() != s.nonceSize {
		return nil, fmt.Errorf("chacha nonce size different than aes (%v vs %v)",
			gcm.NonceSize(), s.nonceSize)
	}

	switch s.code {
	case CryptoCodeAES:
		s.gcm = s.aesgcm
	case CryptoCodeChaCha:
		s.gcm = s.chachagcm
	}

	s.nonce = make([]byte, s.nonceSize)
	if _, err := io.ReadFull(rand.Reader, s.nonce); err != nil {
		return nil, err
	}
	idx++
	pos := s.nonceSize - 8
	b := uint8(56)
	for i := 0; i < 8; i++ {
		s.nonce[pos] = byte(idx >> b)
		pos++
		b -= 8
	}
	return s, nil
}

// EncryptionOffset returns the encrypted data actually starts in
// an encrypted buffer.
func (s *EDStore) EncryptionOffset() int {
	return 1 + s.nonceSize
}

// Encrypt returns the encrypted data or an error
func (s *EDStore) Encrypt(pbuf *[]byte, data []byte) ([]byte, error) {
	var buf []byte
	// If given a buffer, use that one
	if pbuf != nil {
		buf = *pbuf
	}
	// Make sure size is ok, expand if necessary
	buf = util.EnsureBufBigEnough(buf, 1+s.nonceSize+s.cryptoOverhead+len(data))
	// If buffer was passed, update the reference
	if pbuf != nil {
		*pbuf = buf
	}
	buf[0] = s.code
	copy(buf[1:], s.nonce)
	copy(buf[1+s.nonceSize:], data)
	dst := buf[1+s.nonceSize : 1+s.nonceSize+len(data)]
	ed := s.gcm.Seal(dst[:0], s.nonce, dst, nil)
	for i := s.nonceSize - 1; i >= 0; i-- {
		s.nonce[i]++
		if s.nonce[i] != 0 {
			break
		}
	}
	return buf[:1+s.nonceSize+len(ed)], nil
}

// Decrypt returns the decrypted data or an error
func (s *EDStore) Decrypt(dst []byte, cipherText []byte) ([]byte, error) {
	var gcm cipher.AEAD
	if len(cipherText) > 0 {
		switch cipherText[0] {
		case CryptoCodeAES:
			gcm = s.aesgcm
		case CryptoCodeChaCha:
			gcm = s.chachagcm
		default:
			// Anything else, assume no algo or something we don't know how to decrypt.
			return cipherText, nil
		}
	}
	if len(cipherText) <= 1+s.nonceSize {
		return nil, fmt.Errorf("trying to decrypt data that is not (len=%v)", len(cipherText))
	}
	dd, err := gcm.Open(dst, cipherText[1:1+s.nonceSize], cipherText[1+s.nonceSize:], nil)
	if err != nil {
		return nil, err
	}
	return dd, nil
}

// NewCryptoStore returns a CryptoStore instance with
// given underlying store.
func NewCryptoStore(s Store, encryptionCipher string, encryptionKey []byte) (*CryptoStore, error) {
	code, mkh, err := createMasterKeyHash(encryptionCipher, encryptionKey)
	if err != nil {
		return nil, err
	}
	cs := &CryptoStore{
		Store: s,
		code:  code,
		mkh:   mkh,
	}
	// On success, erase the key
	for i := 0; i < len(encryptionKey); i++ {
		encryptionKey[i] = 'x'
	}
	return cs, nil
}

func (cs *CryptoStore) newCryptoMsgStore(channel string, ms MsgStore) (*CryptoMsgStore, error) {
	idx, err := ms.LastSequence()
	if err != nil {
		return nil, err
	}
	// Construct a key based of the master key hash and
	// the channel name, then get the hash for that.
	key := make([]byte, len(cs.mkh)+1+len(channel))
	key = append(key, cs.mkh...)
	key = append(key, '.')
	key = append(key, channel...)
	h := sha256.New()
	h.Write(key)
	keyHash := h.Sum(nil)
	for i := 0; i < len(key); i++ {
		key[i] = 'x'
	}
	eds, err := newEDStore(cs.code, keyHash, idx)
	if err != nil {
		return nil, err
	}
	return &CryptoMsgStore{MsgStore: ms, eds: eds}, nil
}

// Recover implements the Store interface
func (cs *CryptoStore) Recover() (*RecoveredState, error) {
	cs.Lock()
	defer cs.Unlock()
	rs, err := cs.Store.Recover()
	if rs == nil || err != nil {
		return nil, err
	}
	for cn, rc := range rs.Channels {
		cms, err := cs.newCryptoMsgStore(cn, rc.Channel.Msgs)
		if err != nil {
			return nil, err
		}
		rc.Channel.Msgs = cms
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
	cms, err := cs.newCryptoMsgStore(channel, c.Msgs)
	if err != nil {
		return nil, err
	}
	c.Msgs = cms
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

func (cms *CryptoMsgStore) encrypt(data []byte) ([]byte, error) {
	cms.Lock()
	// We can't reuse a buffer since when we pass the data to
	// the underlying store, we don't know if this is retained
	// in some cache, etc..
	ed, err := cms.eds.Encrypt(nil, data)
	cms.Unlock()
	return ed, err
}

func (cms *CryptoMsgStore) decryptedMsg(m *pb.MsgProto) (*pb.MsgProto, error) {
	// When decrypting we can't do it in the original buffer because
	// the store's copy may be in a cache and so this would decipher
	// the encrypted copy and during the next call to decryptedMsg()
	// for the same message, there would be attempt to decrypt something
	// that is not, which would fail.
	dd, err := cms.eds.Decrypt(nil, m.Data)
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
