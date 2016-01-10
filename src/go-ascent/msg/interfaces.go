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
// This file defines client interface for all Messengers. Messenger objects
// allow messages to be delivered between messenger instances.
//
// THREAD SAFETY
//
// All messenger operations are thread-safe.
//
// NOTES
//
// Message passing is always unreliable because a message could be delivered to
// remote messenger, but that does not guarantee that remote operation is
// actually invoked and completed.
//
// Since message passing is unreliable, users must take necessary actions to
// ensure that remote entity has completed the operation. For example, users
// may want to wait for a response from the remote entity.
//
// Message passing is also un-ordered.
//
// All messages created by a messenger can be uniquely identified using
// <messenger-id, message-id> pair, so that messages can be individually
// acknowledged and retried, etc.
//
// Individual messages are not tied to any socket or messenger. Messages can be
// sent between two nodes, using any available transport -- for example, a
// request message can be sent over a accept(2) http socket and response could
// be received from connect(2) ssh socket.
//
// Also, source of a message could be different from the sender of the
// message. A request sent to one messenger be completed by a response from a
// different messenger. This is useful when components can move across the
// nodes due to leadership changes, etc.
//
// TYPES OF MESSAGES
//
// Messenger objects support three types of messages: Requests, Responses and
// Posts.
//
// Requests are messages that expect a reply from the remote entity. They
// invoke a specific operation on the remote side and wait for specific
// duration for a response. When a messenger sends a request, it monitors
// incoming messages for possible responses, till user chooses to close the
// request message.
//
// Responses are replies to incoming requests received by the messenger. Users
// can choose to send multiple responses for a request, to indicate progress,
// etc. Also, responses may come from a different node than where it's request
// was originally targeted.
//
// Posts are raw messages that do not have any implicit semantics. When a Post
// message is sent to a remote side, messengers do not monitor for any replies
// -- except for possible acknowledgment. They are useful in implementing
// control messages between messenger instances themselves.
//

package msg

import (
	"bufio"
	"time"

	thispb "proto-ascent/msg"
)

// Handler defines the callback function interface for objects that wish to
// export their functions across messengers.
type Handler interface {
	// Dispatch function is the entry point for incoming requests. Objects
	// implementing this interface decide what operation to perform in response
	// to the incoming requests.
	//
	// header: Messenger defined header that provides details of the incoming
	//         request.
	//
	// data: User defined request parameters encoded into bytes.
	//
	// If this function returns a non-nil error, it will be sent as a result to
	// the caller automatically.
	Dispatch(header *thispb.Header, data []byte) error
}

// Packer interface defines functions for encoding and decoding messages.
type Packer interface {
	// PeekSize returns the size of incoming packet without advancing the reader.
	PeekSize(*bufio.Reader) (int, error)

	// Encode creates a byte packet from message header and the user data.
	Encode(*thispb.Header, []byte) ([]byte, error)

	// Decode takes a byte packet and returns message header and the user data.
	Decode([]byte) (*thispb.Header, []byte, error)
}

// Messenger defines functions and semantics for all messenger implementations.
type Messenger interface {
	// UID returns a globally unique, non-empty, id for the messenger instance.
	UID() string

	// NewPost creates a post message.
	NewPost() *thispb.Header

	// NewRequest creates a request to a remote entity identified by the classID,
	// objectID and methodName parameters.
	//
	// classID: A classID specifies rpc namespace. Resource accounting and
	//          throttling is performed at the namespace level. For example, each
	//          messengers may choose to limit number of outstanding operations
	//          per classID to an user configurable limit.
	//
	// objectID: An objectID, when non-empty, indicates the remote object for
	//           this request. This is useful, when a single rpc handler is
	//           handling rpcs for all objects of a specific type.
	//
	// methodName: Name of the request target method.
	NewRequest(classID, objectID, methodName string) *thispb.Header

	// NewResponse creates a response to an incoming request.
	NewResponse(request *thispb.Header) *thispb.Header

	// CloseMessage releases a message and its resources. If message is a
	// request, messengers stop monitoring for incoming responses.
	CloseMessage(*thispb.Header) error

	// Send queues a message for delivery. It will be flushed to the wire as soon
	// as possible, but no guarantees can be assumed. Input parameters are not
	// modified, so same message can be sent to other targets if necessary.
	//
	// targetID: Id for the target messenger instance. Local messenger may be
	//           configured separately with the addresses for the target
	//           messenger id.
	//
	// header: Message header specifying the type of the message.
	//
	// data: User data included along with the message.
	//
	// Returns non-nil error if messenger policy denies messages to the given
	// target.
	Send(targetID string, header *thispb.Header, data []byte) error

	// Receive waits for given duration for incoming responses to a request.
	// Request message should have been sent already. Responses can come from any
	// peer.
	//
	// request: Header for the request message.
	//
	// timeout: Timeout for waiting. Since incoming responses are queued, users
	//          can check for a response asynchronously using zero timeout.
	//
	// Returns nil with the incoming message or a non-nil error.
	Receive(request *thispb.Header, timeout time.Duration) (
		*thispb.Header, []byte, error)

	// RegisterClass exports functions using a handler. Either all methods are
	// exported or none are exported.
	//
	// classID: A namespace for the methods and the handler. Multiple
	//          handlers can be exported on a classID.
	//
	// handler: Request handler invoked with every incoming request.
	//
	// methodList: List of method names handled by the handler.
	//
	// Returns nil on success.
	RegisterClass(classID string, handler Handler, methodList ...string) error

	// UnregisterClass removes given methods from the exported method
	// list. Either all methods are removed or none are removed.
	//
	// classID: Namespace for the methods.
	//
	// methodList: List of method names to remove.
	//
	// Returns nil on success.
	UnregisterClass(classID string, methodList ...string) error
}
