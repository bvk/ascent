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
// This file defines helper functions in using the Messenger interface.
//

package msg

import (
	"time"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"

	msgpb "proto-ascent/msg"
)

// SendAll sends a message to multiple nodes.
//
// msn: The Messenger.
//
// targetList: List of targets to send the message.
//
// header: The message header.
//
// data: User data for the message.
//
// Returns the number of targets where message is sent successfully.
func SendAll(msn Messenger, targetList []string, header *msgpb.Header,
	data []byte) (int, error) {

	count := 0
	var errSend error
	for _, target := range targetList {
		if err := msn.Send(target, header, data); err != nil {
			msn.Warningf("could not send %s to %s: %v", header, target, err)
			errSend = errs.MergeErrors(errSend, err)
			continue
		}
		count++
	}
	return count, errSend
}

// SendResponse sends a response message to the source of a request.
//
// msn: The Messenger.
//
// reqHeader: Message header for a request.
//
// data: User data to include in the response.
//
// Returns nil on success.
func SendResponse(msn Messenger, reqHeader *msgpb.Header, data []byte) error {
	if reqHeader.Request == nil {
		msn.Errorf("message header %s is not a request", reqHeader)
		return errs.ErrInvalid
	}
	resHeader := msn.NewResponse(reqHeader)
	return msn.Send(reqHeader.GetMessengerId(), resHeader, data)
}

// SendProto sends a message with protobuf object as the user payload.
func SendProto(msn Messenger, targetID string, header *msgpb.Header,
	data proto.Message) error {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		msn.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return errMarshal
	}

	return msn.Send(targetID, header, raw)
}

// SendAllProto sends a message to multiple nodes with protobuf object as the
// user payload.
func SendAllProto(msn Messenger, targetList []string, header *msgpb.Header,
	data proto.Message) (int, error) {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		msn.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return 0, errMarshal
	}

	return SendAll(msn, targetList, header, raw)
}

// SendResponseProto sends a response message with protobuf object as the user
// payload.
func SendResponseProto(msn Messenger, reqHeader *msgpb.Header,
	data proto.Message) error {

	raw, errMarshal := proto.Marshal(data)
	if errMarshal != nil {
		msn.Errorf("could not marshal protobuf change record: %v", errMarshal)
		return errMarshal
	}

	return SendResponse(msn, reqHeader, raw)
}

// ReceiveProto receives a protobuf object as the user data response to a
// request.
func ReceiveProto(msn Messenger, header *msgpb.Header, timeout time.Duration,
	data proto.Message) (*msgpb.Header, error) {

	resHeader, resData, errRecv := msn.Receive(header, timeout)
	if errRecv != nil {
		msn.Errorf("could not receive responses for %s: %v", header, errRecv)
		return nil, errRecv
	}

	// Check resHeader for errors.
	response := resHeader.GetResponse()
	if response.MessengerStatus != nil {
		err := errs.MakeErrorFromProto(response.GetMessengerStatus())
		return nil, err
	}

	if response.HandlerStatus != nil {
		err := errs.MakeErrorFromProto(response.GetHandlerStatus())
		return nil, err
	}

	remoteID := resHeader.GetMessengerId()
	if err := proto.Unmarshal(resData, data); err != nil {
		msn.Errorf("could not parse response from %s: %v", remoteID, err)
		return nil, err
	}
	return resHeader, nil
}
