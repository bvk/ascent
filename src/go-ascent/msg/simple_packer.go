// Copyright (c) 2016 BVK Chaitanya
//
// This file is part of the Ascent Library.
//
// The Ascent Library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Ascent Library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with the Ascent Library.  If not, see <http://www.gnu.org/licenses/>.

//
// This file defines SimplePacker encoding scheme.
//
// THREAD SAFETY
//
// Packers are not expected keep state, so they are expected to be thread-safe.
//
// NOTES
//
// SimplePacker encodes message header and user data into a byte stream in the
// following order:
//
// 1. Header size is stored as a uint32 in big-endian form.
//
// 2. User data size is stored as a uint32 in big-endian form.
//
// 3. Message header protobuf is serialized to bytes.
//
// 4. User data, which is already in bytes, is appended.
//
// So a SimplePacker uses a minimum of 8 bytes to encode a packet. Since header
// is always mandatory, a zero header size is invalid, thus a packet is always
// more than 8 bytes in size.
//

package msg

import (
	"bufio"
	"encoding/binary"
	"fmt"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"

	thispb "proto-ascent/msg"
)

type simplePacker struct{}

var SimplePacker simplePacker

// PeekSize implements Packer.PeekSize function for SimplePacker.
func (simplePacker) PeekSize(reader *bufio.Reader) (int, error) {
	sizeBytes, errPeek := reader.Peek(8)
	if errPeek != nil {
		return -1, errPeek
	}
	headerSize := binary.BigEndian.Uint32(sizeBytes[0:4])
	dataSize := binary.BigEndian.Uint32(sizeBytes[4:8])
	return int(8 + headerSize + dataSize), nil
}

// Encode implements Packer.Encode function for SimplePacker.
func (simplePacker) Encode(header *thispb.Header, data []byte) ([]byte, error) {
	headerSize := proto.Size(header)
	headerBytes, errMarshal := proto.Marshal(header)
	if errMarshal != nil {
		fmt.Sprintf("could not marshal message header proto %s: %v", header,
			errMarshal)
		return nil, errMarshal
	}

	dataSize := len(data)
	packetSize := 8 + headerSize + dataSize

	packetBytes := make([]byte, packetSize)
	binary.BigEndian.PutUint32(packetBytes[0:4], uint32(headerSize))
	binary.BigEndian.PutUint32(packetBytes[4:8], uint32(dataSize))
	copy(packetBytes[8:8+headerSize], headerBytes)
	copy(packetBytes[8+headerSize:packetSize], data)
	return packetBytes, nil
}

// Decode implements Packer.Decode function for SimplePacker.
func (simplePacker) Decode(input []byte) (*thispb.Header, []byte, error) {
	if len(input) <= 8 {
		return nil, nil, errs.ErrCorrupt
	}
	headerSize := binary.BigEndian.Uint32(input[0:4])
	dataSize := binary.BigEndian.Uint32(input[4:8])
	packetSize := 8 + headerSize + dataSize
	if packetSize > uint32(len(input)) {
		return nil, nil, errs.ErrCorrupt
	}
	if headerSize == 0 {
		return nil, nil, errs.ErrCorrupt
	}
	header := &thispb.Header{}
	if err := proto.Unmarshal(input[8:8+headerSize], header); err != nil {
		return nil, nil, err
	}
	data := make([]byte, dataSize)
	copy(data, input[8+headerSize:packetSize])
	return header, data, nil
}
