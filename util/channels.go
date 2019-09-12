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
	"errors"
	"fmt"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats.go"
)

// Number of bytes used to encode a channel name
const encodedChannelLen = 2

// SendChannelsList sends the list of channels to the given subject, possibly
// splitting the list in several requests if it cannot fit in a single message.
func SendChannelsList(channels []string, sendInbox, replyInbox string, nc *nats.Conn, serverID string) error {
	// Since the NATS message payload is limited, we need to repeat
	// requests if all channels can't fit in a request.
	maxPayload := int(nc.MaxPayload())
	// Reuse this request object to send the (possibly many) protocol message(s).
	header := &spb.CtrlMsg{
		ServerID: serverID,
		MsgType:  spb.CtrlMsg_Partitioning,
	}
	// The Data field (a byte array) will require 1+len(array)+(encoded size of array).
	// To be conservative, let's just use a 8 bytes integer
	headerSize := header.Size() + 1 + 8
	var (
		bytes []byte // Reused buffer in which the request is to marshal info
		n     int    // Size of the serialized request in the above buffer
		count int    // Number of channels added to the request
	)
	for start := 0; start != len(channels); start += count {
		bytes, n, count = encodeChannelsRequest(header, channels, bytes, headerSize, maxPayload, start)
		if count == 0 {
			return errors.New("message payload too small to send channels list")
		}
		if err := nc.PublishRequest(sendInbox, replyInbox, bytes[:n]); err != nil {
			return err
		}
	}
	return nc.Flush()
}

// DecodeChannels decodes from the given byte array the list of channel names
// and return them as an array of strings.
func DecodeChannels(data []byte) ([]string, error) {
	channels := []string{}
	pos := 0
	for pos < len(data) {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("unable to decode size, pos=%v len=%v", pos, len(data))
		}
		cl := int(ByteOrder.Uint16(data[pos:]))
		pos += encodedChannelLen
		end := pos + cl
		if end > len(data) {
			return nil, fmt.Errorf("unable to decode channel, pos=%v len=%v max=%v (string=%v)",
				pos, cl, len(data), string(data[pos:]))
		}
		c := string(data[pos:end])
		channels = append(channels, c)
		pos = end
	}
	return channels, nil
}

// Adds as much channels as possible (based on the NATS max message payload) and
// returns a serialized request. The buffer `reqBytes` is passed (and returned) so
// that it can be reused if more than one request is needed. This call will
// expand the size as needed. The number of bytes used in this buffer is returned
// along with the number of encoded channels.
func encodeChannelsRequest(request *spb.CtrlMsg, channels []string, reqBytes []byte,
	headerSize, maxPayload, start int) ([]byte, int, int) {

	// Each string will be encoded in the form:
	// - length (2 bytes)
	// - string as a byte array.
	var _encodedSize = [encodedChannelLen]byte{}
	encodedSize := _encodedSize[:]
	// We are going to encode the channels in this buffer
	chanBuf := make([]byte, 0, maxPayload)
	var (
		count         int          // Number of encoded channels
		estimatedSize = headerSize // This is not an overestimation of the total size
		numBytes      int          // This is what is returned by MarshalTo
	)
	for i := start; i < len(channels); i++ {
		c := []byte(channels[i])
		cl := len(c)
		needed := encodedChannelLen + cl
		// Check if adding this channel to current buffer makes us go over
		if estimatedSize+needed > maxPayload {
			// Special case if we cannot even encode 1 channel
			if count == 0 {
				return reqBytes, 0, 0
			}
			break
		}
		// Encoding the channel here. First the size, then the channel name as byte array.
		ByteOrder.PutUint16(encodedSize, uint16(cl))
		chanBuf = append(chanBuf, encodedSize...)
		chanBuf = append(chanBuf, c...)
		count++
		estimatedSize += needed
	}
	if count > 0 {
		request.Data = chanBuf
		reqBytes = EnsureBufBigEnough(reqBytes, estimatedSize)
		numBytes, _ = request.MarshalTo(reqBytes)
		if numBytes > maxPayload {
			panic(fmt.Errorf("request size is %v (max payload is %v)", numBytes, maxPayload))
		}
	}
	return reqBytes, numBytes, count
}
