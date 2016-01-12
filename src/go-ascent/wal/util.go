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
// This file defines helper functions for saving protobuf records to the wal.
//

package wal

import (
	"github.com/golang/protobuf/proto"
)

// QueueChangeProto queues a protobuf object as the change record.
func QueueChangeProto(wal WriteAheadLog, uid string, data proto.Message) (
	LSN, error) {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		wal.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return nil, errMarshal
	}

	lsn := wal.QueueChangeRecord(uid, raw)
	return lsn, nil
}

// AppendChangeProto appends a protobuf object as the change record.
func AppendChangeProto(wal WriteAheadLog, uid string, data proto.Message) (
	LSN, error) {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		wal.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return nil, errMarshal
	}

	return wal.AppendChangeRecord(uid, raw)
}

// SyncChangeProto synchronously appends a protobuf object as the change
// record.
func SyncChangeProto(wal WriteAheadLog, uid string, data proto.Message) (
	LSN, error) {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		wal.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return nil, errMarshal
	}

	return wal.SyncChangeRecord(uid, raw)
}

// AppendCheckpointProto appends a protobuf object as the checkpoint record.
func AppendCheckpointProto(wal WriteAheadLog, uid string,
	data proto.Message) error {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		wal.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return errMarshal
	}

	return wal.AppendCheckpointRecord(uid, raw)
}
