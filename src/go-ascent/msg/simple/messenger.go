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
// This file defines the Messenger data type from msg/simple package.
//

package simple

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"
	"go-ascent/base/log"
	"go-ascent/msg"
	"go-ascent/thread/ctlr"

	msgpb "proto-ascent/msg"
	thispb "proto-ascent/msg/simple"
)

// Options defines user configuration items for the messenger.
type Options struct {
	// Max timeout for write operation when sending a packet.
	MaxWriteTimeout time.Duration

	// Maximum number of responses buffered waiting to be processed on a request.
	ResponseQueueSize int

	// Maximum number of messages buffered waiting to send to a remote.
	SendQueueSize int

	// Negotiation timeout for a network connection.
	NegotiationTimeout time.Duration

	// Retry timeout for sending pending messages.
	SendRetryTimeout time.Duration

	// Maximum number of requests to dispatch simultaneously.
	MaxDispatchRequests int

	// Time to wait to queue an incoming request.
	DispatchRequestTimeout time.Duration
}

// Validate checks for invalid user configuration settings.
func (this *Options) Validate() (status error) {
	if this.ResponseQueueSize < 1 {
		err := errs.NewErrInvalid("response queue size should be at least one")
		status = errs.MergeErrors(status, err)
	}
	if this.SendQueueSize < 1 {
		err := errs.NewErrInvalid("outbox queue size should at least one")
		status = errs.MergeErrors(status, err)
	}
	if this.NegotiationTimeout < time.Millisecond {
		err := errs.NewErrInvalid("connection negotiation timeout is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.SendRetryTimeout < 10*time.Millisecond {
		err := errs.NewErrInvalid("send retry timeout is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.MaxDispatchRequests < 1 {
		err := errs.NewErrInvalid("at least one request should be dispatched")
		status = errs.MergeErrors(status, err)
	}
	return status
}

///////////////////////////////////////////////////////////////////////////////

// Messenger implements a network messenger for msg.Messenger interface.
type Messenger struct {
	log.Logger

	// Wait group to wait for goroutines to finish.
	wg sync.WaitGroup

	// Controller for admission control and synchronization.
	ctlr ctlr.BasicController

	// Flag, when true, indicates that messenger is started.
	started bool

	// User configurable options.
	opts Options

	// Persistent state for the messenger.
	state thispb.PersistentState

	// Globally unique name for the messenger.
	uid string

	// Last message id issued by the messenger.
	lastMessageID int64

	// Mapping from listener addresses to the network listeners. Listener
	// addresses are considered unique for a listener.
	listenerMap map[string]net.Listener

	// Mapping from peer uid to its meta-data for all open peers.
	peerMap map[string]*Peer

	// Mapping from live request ids to its incoming response queue. A buffered
	// channel is used as the bounded response queue.
	requestMap map[int64]chan *Entry

	// All exported function names and their handlers map.
	exportMap map[string]map[string]msg.Handler

	// A buffered channel for dispatching incoming requests.
	requestCh chan *Entry
}

// Peer represents the meta data for a remote messenger.
type Peer struct {
	// Globally unique id for the remote messenger.
	peerID string

	// List of listener address for the remote messenger.
	addressList []string

	// Set of open transports to the remote messenger.
	transportMap map[*Transport]struct{}

	// Buffered channel with messages queued to the remote messenger.
	outCh chan *Entry
}

// Transport represents a bidirectional connection to a remote messenger
// instance.
type Transport struct {
	// A name for the transport.
	name string

	// Atomic status flag, when set indicates this transport had errors.
	bad int32

	// Connection to the remote messenger.
	connection net.Conn

	// Buffered reader over the connection.
	reader *bufio.Reader

	// Packet maker for the transport.
	packer msg.Packer

	// Remote messenger id as reported by the remote messenger instance.
	remoteID string

	// Some options reported by the remote messenger instance during the
	// transport negotiation.
	messageOptions   *msgpb.MessageOptions
	messengerOptions *msgpb.MessengerOptions
}

// Entry represents a message.
type Entry struct {
	header *msgpb.Header
	data   []byte
}

// Initialize configures a messenger object.
//
// opts: User configurable options.
//
// uid: Globally unique id for the messenger.
//
// Returns nil on success.
func (this *Messenger) Initialize(opts *Options, uid string) (status error) {
	if err := opts.Validate(); err != nil {
		this.Errorf("could not validate user options for messenger: %v", err)
		return err
	}

	this.uid = uid
	this.opts = *opts
	this.ctlr.Logger = this
	this.exportMap = make(map[string]map[string]msg.Handler)
	this.peerMap = make(map[string]*Peer)
	this.listenerMap = make(map[string]net.Listener)
	this.requestMap = make(map[int64]chan *Entry)
	this.Logger = this.NewLogger("simple-messenger:%s", uid)
	this.ctlr.Initialize(this)

	this.requestCh = make(chan *Entry, this.opts.MaxDispatchRequests)
	for ii := 0; ii < this.opts.MaxDispatchRequests; ii++ {
		this.wg.Add(1)
		go this.goDispatch()
	}
	return nil
}

// Close releases all messenger resources and destroys it.
func (this *Messenger) Close() (status error) {
	if err := this.ctlr.Close(); err != nil {
		return err
	}

	if this.started {
		if err := this.doStop(); err != nil {
			this.Errorf("could not stop messenger service (ignored): %v", err)
			status = errs.MergeErrors(status, err)
		}
	}

	return status
}

// Start activates the messenger by opening all its listeners.
func (this *Messenger) Start() error {
	token, errToken := this.ctlr.NewToken("Start", nil /* timeout */)
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if this.started {
		return errs.ErrStarted
	}

	if err := this.doStart(); err != nil {
		this.Errorf("could not start messenger: %v", err)
		return err
	}

	this.Infof("messenger %s is started listening at %v", this.uid,
		this.listenerMap)
	this.started = true
	return nil
}

// Stop deactivates the messenger by closing all its listeners. It also closes
// all open transports, so no further messages can be sent or received using
// the messenger.
func (this *Messenger) Stop() error {
	token, errToken := this.ctlr.NewToken("Stop", nil /* timeout */)
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if !this.started {
		return errs.ErrStopped
	}

	if err := this.doStop(); err != nil {
		this.Errorf("could not stop messenger: %v", err)
		return err
	}

	this.Infof("messenger %s is stopped", this.uid)
	this.started = false
	return nil
}

// UID returns the messenger uid.
func (this *Messenger) UID() string {
	// This field is read-only after Initialize, so it is thread-safe.
	return this.uid
}

// AddListenerAddress adds a new listener address to the messenger.
//
// newAddress: new listener address for the messenger.
//
// Returns nil on success.
func (this *Messenger) AddListenerAddress(newAddress string) error {
	token, errToken := this.ctlr.NewToken("AddListenerAddress",
		nil /* timeout */, "this.state", "this.listenerMap")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	// Check for duplicates.
	for _, address := range this.state.ListenerAddressList {
		if address == newAddress {
			return errs.ErrExist
		}
	}

	// Validate the address.
	validList, _, errCheck := this.CheckPeerAddressList(this.uid,
		[]string{newAddress})
	if errCheck != nil {
		this.Errorf("listener address %s is invalid", newAddress)
		return errs.ErrInvalid
	}

	if len(validList) == 0 {
		this.Errorf("listener address %s is not supported", newAddress)
		return errs.ErrInvalid
	}

	// If the messenger is started, we should open a new listener.
	if this.started {
		if err := this.startListener(newAddress); err != nil {
			this.Errorf("could not start listener on address %s: %v", newAddress,
				err)
			return err
		}
	}

	// TODO: Make these changes persistent using wal.

	this.state.ListenerAddressList = append(this.state.ListenerAddressList,
		newAddress)
	return nil
}

// AddPeerAddress adds a new peer to the known peer repository.
//
// peerID: Globally unique id for the remote messenger.
//
// addressList: List of listener addresses of the remote messenger.
//
// Returns nil on success.
func (this *Messenger) AddPeerAddress(peerID string,
	addressList []string) error {

	token, errToken := this.ctlr.NewToken("AddPeerAddress", nil, /* timeout */
		"this.state")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	// Check the address list.
	validList, unsupportedList, errCheck := this.CheckPeerAddressList(peerID,
		addressList)
	if errCheck != nil {
		this.Errorf("one or more addresses in %v for peer %s are invalid",
			addressList, peerID)
		return errCheck
	}

	if len(unsupportedList) > 0 {
		this.Infof("addresses %v in list %v for peer %s are not supported",
			unsupportedList, addressList, peerID)
	}

	if len(validList) == 0 {
		this.Errorf("no supported addresses in %v for peer %s", addressList, peerID)
		return errs.ErrInvalid
	}

	// Check for duplicates.
	for _, peer := range this.state.PeerList {
		if peer.GetMessengerId() == peerID {
			this.Errorf("peer with id %s already exists", peerID)
			return errs.ErrExist
		}
	}

	// TODO: Make these changes persistent using an wal.

	newPeer := &thispb.Peer{}
	newPeer.MessengerId = proto.String(peerID)
	newPeer.AddressList = append(newPeer.AddressList, addressList...)
	this.state.PeerList = append(this.state.PeerList, newPeer)
	return nil
}

// NewPost creates a message header for a post message.
func (this *Messenger) NewPost() *msgpb.Header {
	header := &msgpb.Header{}
	header.MessageId = proto.Int64(atomic.AddInt64(&this.lastMessageID, 1))
	header.MessengerId = proto.String(this.uid)
	return header
}

// NewResponse creates a message header for a response message.
//
// request: Message header for the received request.
func (this *Messenger) NewResponse(request *msgpb.Header) *msgpb.Header {
	header := this.NewPost()
	header.Response = &msgpb.Header_Response{}
	header.Response.RequestId = proto.Int64(request.GetMessageId())
	return header
}

// NewRequest creates a message header for a request message.
//
// classID: Class or namespace name for the target operation.
//
// objectID: Optional object id, if target operation can apply to multiple
//           objects.
//
// methodName: Function or method name for the target operation.
//
// Returns a message header.
func (this *Messenger) NewRequest(classID, objectID,
	methodName string) *msgpb.Header {

	header := this.NewPost()
	header.Request = &msgpb.Header_Request{}
	header.Request.ClassId = proto.String(classID)
	header.Request.MethodName = proto.String(methodName)
	if len(objectID) > 0 {
		header.Request.ObjectId = proto.String(objectID)
	}
	//
	// Create a response channel for the request. Response channel is where
	// responses are stored for the request.
	//
	// Ideally, we should be creating the response channel when the request is
	// sent -- not when it is created. Bute if we create response channel during
	// a send, we need to lock requestMap in every send operation, which means,
	// if a request is sent multiple times to different nodes, we end up
	// acquiring the lock multiple times, which adds unnecessary overhead,
	// because we update requestMap with response channel only once.
	//
	token, errToken := this.ctlr.NewToken("NewRequest", nil, /* timeout */
		"this.requestMap")
	if errToken != nil {
		return nil
	}
	defer this.ctlr.CloseToken(token)

	requestID := header.GetMessageId()
	if _, ok := this.requestMap[requestID]; !ok {
		responseCh := make(chan *Entry, this.opts.ResponseQueueSize)
		this.requestMap[requestID] = responseCh
	}
	return header
}

// CloseMessage releases a message header.
//
// header: Message header.
//
// Returns nil on success.
func (this *Messenger) CloseMessage(header *msgpb.Header) error {
	if header.GetMessengerId() != this.uid {
		this.Errorf("message header %s is not created by this messenger", header)
		return errs.ErrInvalid
	}

	if header.Request == nil {
		return nil
	}
	//
	// Remove the request and its response channel from live requests map.
	//
	token, errToken := this.ctlr.NewToken("CloseMessage", nil, /* timeout */
		"this.requestMap")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	requestID := header.GetMessageId()
	responseCh, found := this.requestMap[requestID]
	if !found {
		return nil
	}

	close(responseCh)
	delete(this.requestMap, requestID)
	return nil
}

// NewPeer creates a remote messenger instance for a possibly unknown peer, if
// necessary.
//
// peerID: Globally unique id for the remote messenger.
//
// addressList: List of listener addresses where remote messenger can be
// reached.
//
// Returns remote messenger instance.
func (this *Messenger) NewPeer(peerID string, addressList []string) (
	*Peer, error) {

	token, errToken := this.ctlr.NewToken("NewPeer", nil, /* timeout */
		"this.peerMap")
	if errToken != nil {
		return nil, errToken
	}
	defer this.ctlr.CloseToken(token)

	peer, found := this.peerMap[peerID]
	if found {
		return peer, nil
	}

	return this.doNewPeer(peerID, addressList)
}

// OpenPeer creates a remote messenger instance for an already known peer.
//
// peerID: Globally unique id for the remote messenger.
//
// Returns remote messenger instance.
func (this *Messenger) OpenPeer(peerID string) (*Peer, error) {
	token, errToken := this.ctlr.NewToken("OpenPeer", nil, /* timeout */
		"this.peerMap", "this.state")
	if errToken != nil {
		return nil, errToken
	}
	defer this.ctlr.CloseToken(token)

	peer, found := this.peerMap[peerID]
	if found {
		return peer, nil
	}

	peerList := this.state.GetPeerList()
	for _, peer := range peerList {
		if peer.GetMessengerId() == peerID {
			return this.doNewPeer(peerID, peer.GetAddressList())
		}
	}
	return nil, errs.ErrNotExist
}

// ClosePeer closes all open transports to a remote messenger instance and
// releases its resources.
//
// peerID: Globally unique id for the remote peer instance.
//
// Returns nil on success.
func (this *Messenger) ClosePeer(peerID string) (status error) {
	token, errToken := this.ctlr.NewToken("ClosePeer", nil, /* timeout */
		"this.peerMap", peerID)
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	peer, found := this.peerMap[peerID]
	if !found {
		return nil
	}

	delete(this.peerMap, peerID)

	token.ReleaseResources("this.peerMap")

	close(peer.outCh)
	for tport := range peer.transportMap {
		if tport.connection != nil {
			if err := tport.connection.Close(); err != nil {
				this.Errorf("could not close transport connection for %s: %v",
					tport.name, err)
				status = errs.MergeErrors(status, err)
			}
		}
		tport.connection = nil
	}
	peer.transportMap = make(map[*Transport]struct{})
	return status
}

// Send sends a message to target messenger instance.
//
// targetID: Globally unique messenger id for the target.
//
// header: Message header.
//
// data: User data sent with the message.
//
// Returns nil on success.
func (this *Messenger) Send(targetID string, header *msgpb.Header,
	data []byte) error {

	// Handle loopback message as a special case.
	if targetID == this.uid {
		if err := this.DispatchIncoming(this.uid, header, data); err != nil {
			this.Errorf("could not dispatch loopback message: %v", err)
			return err
		}
		return nil
	}

	peer, errOpen := this.OpenPeer(targetID)
	if errOpen != nil {
		this.Errorf("could not open remote messenger instance for %s: %v",
			targetID, errOpen)
		return errOpen
	}

	entry := &Entry{header: header, data: data}

	// Send is always non-blocking.
	select {
	case peer.outCh <- entry:
		return nil
	default:
		return errs.ErrOverflow
	}
}

// Receive waits for a response to a live request.
//
// request: Message header of a live request.
//
// timeout: Time to wait for a response.
//
// Returns response message header and response data on success.
func (this *Messenger) Receive(request *msgpb.Header, timeout time.Duration) (
	*msgpb.Header, []byte, error) {

	token, errToken := this.ctlr.NewToken("Receive", time.After(timeout),
		"this.requestMap")
	if errToken != nil {
		return nil, nil, errToken
	}
	defer this.ctlr.CloseToken(token)

	requestID := request.GetMessageId()
	responseCh, found := this.requestMap[requestID]
	if !found {
		this.Errorf("no live request [%s] exist", request)
		return nil, nil, errs.ErrNotExist
	}

	// Close the token early to unblock others.
	this.ctlr.CloseToken(token)

	// Perform a non-blocking receive if timeout is zero.
	if timeout == 0 {
		select {
		case entry := <-responseCh:
			return entry.header, entry.data, nil

		default:
			return nil, nil, errs.ErrRetry
		}
	}

	// Perform a blocking receive with a timeout.
	select {
	case <-time.After(timeout):
		this.Warningf("timedout waiting for response to %s", request)
		return nil, nil, errs.ErrTimeout

	case entry := <-responseCh:
		return entry.header, entry.data, nil
	}
}

// OpenTransport opens a new transport to remote messenger instance.
//
// peer: Remote messenger instance.
//
// address: Listener address for the remote messenger.
//
// Returns new transport on success.
func (this *Messenger) OpenTransport(peer *Peer, address string) (
	*Transport, error) {

	addressURL, errParse := url.Parse(address)
	if errParse != nil {
		this.Errorf("could not parse address %s for peer %s: %v", address,
			peer.peerID, errParse)
		return nil, errParse
	}

	network := ""
	switch addressURL.Scheme {
	case "tcp":
		network = "tcp4"

	default:
		this.Errorf("unsupported address scheme %s in address %s to peer %s",
			addressURL.Scheme, address, peer.peerID)
		return nil, errs.ErrInvalid
	}

	connection, errDial := net.Dial(network, addressURL.Host)
	if errDial != nil {
		this.Errorf("could not connect to address %s for peer %s: %v",
			address, peer.peerID, errDial)
		return nil, errDial
	}

	tport, errNew := this.NewTransport(address, connection,
		this.opts.NegotiationTimeout)
	if errNew != nil {
		this.Errorf("could not negotiate transport at address %s to peer %s",
			address, peer.peerID)
		return nil, errNew
	}

	if tport.remoteID != peer.peerID {
		this.Errorf("unexpected remote id %s when connected to address %s "+
			"looking for peer %s", tport.remoteID, address, peer.peerID)
		return nil, errs.ErrInvalid
	}
	return tport, nil
}

// NewTransport negotiates a new transport over an established network
// connection.
//
// listener: An example listener address for the network connection.
//
// connection: Incoming or outgoing network connection.
//
// timeout: Timeout for the negotiation.
//
// Returns a transport object on success.
func (this *Messenger) NewTransport(listener string, connection net.Conn,
	timeout time.Duration) (newTport *Transport, status error) {

	msnOptions := &msgpb.MessengerOptions{}
	msnOptions.AddressList = this.ListenerAddressList()

	// Set the negotiation deadline.
	start := time.Now()
	deadline := start.Add(timeout)
	timeoutCh := time.After(timeout)
	connection.SetDeadline(deadline)
	defer connection.SetDeadline(time.Time{})

	reader := bufio.NewReader(connection)
	packer := GetPacker(listener)

	sendHeader := this.NewPost()
	sendHeader.Options = &msgpb.Header_Options{}
	sendHeader.Options.MessengerOptions = msnOptions
	rawBytes, errEncode := packer.Encode(sendHeader, nil)
	if errEncode != nil {
		this.Errorf("could not encode header [%s]: %v", sendHeader, errEncode)
		return nil, errEncode
	}

	if _, err := connection.Write(rawBytes); err != nil {
		this.Errorf("could not write to connection %s: %v", connection, err)
		return nil, err
	}

	packetSize, errPeek := packer.PeekSize(reader)
	if errPeek != nil {
		this.Errorf("could not peek for packet size: %v", errPeek)
		return nil, errPeek
	}

	buffer := make([]byte, packetSize)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		this.Errorf("could not read negotiation message: %v", err)
		return nil, err
	}

	recvHeader, _, errDecode := packer.Decode(buffer)
	if errDecode != nil {
		this.Errorf("could not decode negotiation message: %v", errDecode)
		return nil, errDecode
	}

	remoteOptions := recvHeader.GetOptions()
	if remoteOptions == nil {
		this.Errorf("negotiation message from the remote has no options field")
		return nil, errs.ErrInvalid
	}

	messengerOptions := remoteOptions.GetMessengerOptions()
	if messengerOptions == nil {
		this.Errorf("negotiation message from the remote has no messenger options")
		return nil, errs.ErrInvalid
	}

	remoteAddressList := messengerOptions.GetAddressList()
	if remoteAddressList == nil {
		this.Errorf("negotiation message doesn't have remote messenger addresses")
		return nil, errs.ErrInvalid
	}

	remoteID := recvHeader.GetMessengerId()
	validList, unsupportedList, errCheck := this.CheckPeerAddressList(remoteID,
		remoteAddressList)
	if errCheck != nil {
		this.Errorf("one or more remote messenger addresses exchanged in " +
			"negotaition are invalid")
		return nil, errs.ErrInvalid
	}

	if len(validList) == 0 {
		this.Errorf("no addresses %v of remote messenger %s exchanged in "+
			"negotiation are supported", remoteAddressList, remoteID)
		return nil, errs.ErrInvalid
	}

	if len(unsupportedList) > 0 {
		this.Warningf("some addresses %v out of %v of remote messenger %s are "+
			"not supported", unsupportedList, remoteAddressList, remoteID)
	}

	name := fmt.Sprintf("{%s:%s-%s:%s}", this.uid, connection.LocalAddr(),
		remoteID, connection.RemoteAddr())
	tport := &Transport{
		name:             name,
		connection:       connection,
		reader:           reader,
		packer:           packer,
		remoteID:         remoteID,
		messageOptions:   remoteOptions.GetMessageOptions(),
		messengerOptions: messengerOptions,
	}

	// Create a remote messenger instance if it doesn't already exist.
	peer, errNew := this.NewPeer(remoteID, validList)
	if errNew != nil {
		this.Errorf("could not open peer %s: %v", remoteID, errNew)
		return nil, errNew
	}

	// Negotiation was successful, so add the transport to the remote messenger.
	token, errToken := this.ctlr.NewToken("NewTransport", timeoutCh,
		remoteID)
	if errToken != nil {
		this.Errorf("could not get token for opening new transport %s: %v",
			tport.name, errToken)
		return nil, errToken
	}
	defer this.ctlr.CloseToken(token)

	peer.transportMap[tport] = struct{}{}
	this.Infof("added new transport %s connecting to peer %s", tport.name,
		remoteID)

	this.wg.Add(1)
	go this.goReceive(peer, tport)
	return tport, nil
}

// CloseTransport removes a transport to a remote messenger instance.
//
// peer: The remote messenger instance.
//
// tport: Transport to remove.
//
// Returns nil on success.
func (this *Messenger) CloseTransport(peer *Peer, tport *Transport) (
	status error) {

	token, errToken := this.ctlr.NewToken("CloseTransport", nil, /* timeout */
		peer.peerID)
	if errToken != nil {
		return errToken
	}
	delete(peer.transportMap, tport)
	this.ctlr.CloseToken(token)

	if tport.connection == nil {
		return nil
	}

	if err := tport.connection.Close(); err != nil {
		this.Errorf("could not close transport connection: %v", err)
		status = errs.MergeErrors(status, err)
	}
	tport.connection = nil
	return status
}

// RegisterClass exports named functions to remote messenger instances.
//
// classID: Class or namespace name for the exports.
//
// handler: Request handler for the exports.
//
// methodList: List of function names exported to remote messengers.
//
// Returns nil on success.
func (this *Messenger) RegisterClass(classID string, handler msg.Handler,
	methodList ...string) error {

	token, errToken := this.ctlr.NewToken("RegisterClass", nil, /* timeout */
		"this.exportMap")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	methodMap, found := this.exportMap[classID]
	if !found {
		methodMap = make(map[string]msg.Handler)
		this.exportMap[classID] = methodMap
	}

	var dupList []string
	if len(methodMap) > 0 {
		for _, method := range methodList {
			if _, ok := methodMap[method]; ok {
				dupList = append(dupList, method)
			}
		}
	}

	if len(dupList) > 0 {
		this.Errorf("methods %v are already registered for class id %s", dupList,
			classID)
		return errs.ErrExist
	}

	for _, method := range methodList {
		methodMap[method] = handler
	}
	return nil
}

// UnregisterClass removes functions exported to remote messengers.
//
// classID: Class or namespace name for the exports.
//
// methodList: List of function names to remove from the exports.
//
// Returns nil on success.
func (this *Messenger) UnregisterClass(classID string,
	methodList ...string) error {

	token, errToken := this.ctlr.NewToken("UnregisterClass", nil, /* timeout */
		"this.exportMap")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	methodMap, found := this.exportMap[classID]
	if !found {
		this.Errorf("no class with id %s is registered", classID)
		return errs.ErrNotExist
	}

	var unknownList []string
	for _, method := range methodList {
		if _, ok := methodMap[method]; !ok {
			unknownList = append(unknownList, method)
		}
	}

	if len(unknownList) > 0 {
		this.Errorf("methods %v were not found under class id %s", unknownList,
			classID)
		return errs.ErrNotExist
	}

	for _, method := range methodList {
		delete(methodMap, method)
	}
	return nil
}

// DispatchIncoming dispatches an incoming message based on the message type.
//
// senderID: Globally unique messenger id that has delivered this message.
//
// header: Message header for the incoming message.
//
// data: User data in the incoming message.
//
// Returns nil if message is delivered successfully.
func (this *Messenger) DispatchIncoming(senderID string, header *msgpb.Header,
	data []byte) error {

	if header.Response != nil {
		return this.DispatchResponse(header, data)
	}

	if header.Request == nil {
		return this.DispatchPost(header, data)
	}

	// This is a request message, so add it to the request queue.

	entry := &Entry{header: header, data: data}

	if this.opts.DispatchRequestTimeout > 0 {
		select {
		case this.requestCh <- entry:
			return nil
		case <-time.After(this.opts.DispatchRequestTimeout):
			this.Errorf("could not dispatch request %s because request queue "+
				"is full", header)
			return errs.ErrOverflow
		}
	}

	select {
	case this.requestCh <- entry:
		return nil
	default:
		this.Errorf("could not dispatch request %s because request queue is full",
			header)
		return errs.ErrOverflow
	}
}

// DispatchPost dispatches an incoming post message to appropriate handler.
//
// header: Message header for the post message.
//
// data: User data in the post message.
//
// Returns nil dispatching the post successfully.
func (this *Messenger) DispatchPost(header *msgpb.Header, data []byte) error {
	// TODO: Post messages are not implemented yet.
	return nil
}

// DispatchResponse enqueues an incoming response into the response channel for
// the corresponding request.
//
// header: Message header for the response.
//
// data: User data in the response message.
//
// Returns nil on success.
func (this *Messenger) DispatchResponse(header *msgpb.Header,
	data []byte) error {

	token, errToken := this.ctlr.NewToken("DispatchResponse", nil, /* timeout */
		"this.requestMap")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	response := header.GetResponse()
	requestID := response.GetRequestId()
	responseCh, found := this.requestMap[requestID]
	if !found {
		this.Errorf("could not find request %d to dispatch response %s",
			requestID, header)
		return errs.ErrNotExist
	}

	// Close the token early to release the resources.
	this.ctlr.CloseToken(token)

	entry := &Entry{header: header, data: data}

	select {
	case responseCh <- entry:
		return nil
	default:
		this.Errorf("could not queue response %s to request %d", header,
			requestID)
		return errs.ErrOverflow
	}
}

// DispatchRequest invokes the target (exported) function identified in the
// request message.
//
// header: Message header for the request.
//
// data: User data in the request.
//
// Returns dispatch operation status and the handler status.
func (this *Messenger) DispatchRequest(header *msgpb.Header,
	data []byte) (msnStatus, appStatus error) {

	defer func() {
		if msnStatus != nil || appStatus != nil {
			failure := this.NewResponse(header)
			if msnStatus != nil {
				failure.Response.MessengerStatus = errs.MakeProtoFromError(msnStatus)
			} else {
				failure.Response.HandlerStatus = errs.MakeProtoFromError(appStatus)
			}

			sourceID := header.GetMessengerId()
			if err := this.Send(sourceID, failure, nil); err != nil {
				this.Errorf("could not reply failure %s for %s from %s (ignored)",
					failure, header, sourceID)
			}
		}
	}()

	token, errToken := this.ctlr.NewToken("DispatchRequest", nil, /* timeout */
		"this.exportMap")
	if errToken != nil {
		return errToken, nil
	}
	defer this.ctlr.CloseToken(token)

	request := header.GetRequest()
	classID := request.GetClassId()
	methodName := request.GetMethodName()
	if len(classID) == 0 || len(methodName) == 0 {
		this.Errorf("header %s is invalid because class id and/or method name "+
			"are empty", header)
		msnStatus = errs.NewErrInvalid("class id and/or method name cannot be " +
			"empty")
		return msnStatus, nil
	}

	methodMap, found := this.exportMap[classID]
	if !found {
		this.Errorf("no class with id %s was registered", classID)
		msnStatus = errs.NewErrNotExist("no class with id %s was registered",
			classID)
		return msnStatus, nil
	}

	handler, found := methodMap[methodName]
	if !found {
		this.Errorf("class %s has no method named %s", classID, methodName)
		msnStatus = errs.NewErrNotExist("class %s has no method named %s", classID,
			methodName)
		return msnStatus, nil
	}

	// Close the token early to release the resources.
	this.ctlr.CloseToken(token)

	appStatus = handler.Dispatch(header, data)
	if appStatus != nil {
		this.Warningf("request %s failed with status %v", header, appStatus)
	}
	return nil, appStatus
}

// FlushMessages writes input messages to the transports in round-robin order.
//
// peer: Target remote messenger instance.
//
// entryList: List of messages to write.
//
// Returns number of messages written into the transports.
func (this *Messenger) FlushMessages(peer *Peer, entryList []*Entry) (
	cnt int, status error) {

	// FlushMessages must block Stop operation, so we take an extra private
	// resource (named flush-peer) so that full lock acquired in Stop will be
	// blocked.
	resource := fmt.Sprintf("flush-%s", peer.peerID)

	timeoutCh := time.After(this.opts.MaxWriteTimeout)
	token, errToken := this.ctlr.NewToken("FlushMessages", timeoutCh,
		peer.peerID, resource)
	if errToken != nil {
		return 0, errToken
	}
	defer this.ctlr.CloseToken(token)

	// Make a copy of open transports or peer addresses.
	var addressList []string
	var transportList []*Transport
	if len(peer.transportMap) == 0 {
		addressList = make([]string, len(peer.addressList))
		copy(addressList, peer.addressList)
	} else {
		transportList = make([]*Transport, 0, len(peer.transportMap))
		for tport := range peer.transportMap {
			if atomic.LoadInt32(&tport.bad) == 0 {
				transportList = append(transportList, tport)
			} else {
				delete(peer.transportMap, tport)
				if tport.connection != nil {
					tport.connection.Close()
					tport.connection = nil
				}
			}
		}
	}

	// Close the token to release the resources.
	token.ReleaseResources(peer.peerID)

	// Open new transports if no open transports exist.
	if len(transportList) == 0 {
		for _, address := range addressList {
			tport, errNew := this.OpenTransport(peer, address)
			if errNew != nil {
				this.Warningf("could not negotiate with peer %s (ignored): %v",
					peer.peerID, errNew)
				continue
			}
			transportList = append(transportList, tport)
			break
		}
	}

	// We cannot send any messages if no transports are open.
	if len(transportList) == 0 {
		this.Errorf("could not open transport to peer %s on any of addresses %v",
			peer.peerID, addressList)
		return 0, errs.ErrRetry
	}

	// Send as many messages as possible.
	count := 0
	numTports := len(transportList)
	for index, entry := range entryList {
		tport := transportList[index%numTports]

		entry.header.SenderTimestampNsecs = proto.Int64(time.Now().UnixNano())
		packet, errEncode := tport.packer.Encode(entry.header, entry.data)
		if errEncode != nil {
			this.Warningf("could not encode packet %s to send on %s (ignored): %v",
				entry.header, tport.name, errEncode)
			return count, errEncode
		}

		deadline := time.Now().Add(this.opts.MaxWriteTimeout)
		tport.connection.SetWriteDeadline(deadline)
		if _, err := tport.connection.Write(packet); err != nil {
			if err != io.EOF && !this.ctlr.IsClosed() {
				this.Errorf("could not write packet to %s: %v", tport.name, err)
			}
			atomic.StoreInt32(&tport.bad, 1)
			return count, err
		}
		tport.connection.SetWriteDeadline(time.Time{})
		this.Infof("=> %s to %s", entry.header, peer.peerID)
		count++
	}
	return count, nil
}

// HasListener returns true if a listener address is valid.
func (this *Messenger) HasListener(address string) bool {
	lock := this.ctlr.ReadLock("this.listenerMap")
	defer lock.Unlock()

	if this.listenerMap == nil {
		return false
	}

	_, found := this.listenerMap[address]
	return found
}

// ListenerAddressList returns all current listeners' addresses.
func (this *Messenger) ListenerAddressList() []string {
	lock := this.ctlr.ReadLock("this.listenerMap")
	defer lock.Unlock()
	//
	// Client tools that don't have a well defined port number, so their
	// listeners use dynamically assigned port number. So, query the listener
	// address dynamically.
	//
	var addressList []string
	for _, listener := range this.listenerMap {
		addr := listener.Addr()
		address := fmt.Sprintf("%s://%s", addr.Network(), addr.String())
		addressList = append(addressList, address)
	}

	return addressList
}

// GetPacker returns the packet maker for all transports made by the class of
// given listener.
func GetPacker(listener string) msg.Packer {
	return msg.SimplePacker
}

// CheckPeerAddressList validates addresses of a remote messenger.
//
// peerID: Globally unique id for the remote messenger.
//
// addressList: List of listener addresses of the remote messenger.
//
// Returns supported and unsupported addresses and nil on success. Returns
// non-nil error if any address is unparsable.
func (this *Messenger) CheckPeerAddressList(peerID string,
	addressList []string) ([]string, []string, error) {

	// Validate all addresses and ignore unsupported addresses.
	var validAddressList, otherAddressList []string
	for _, address := range addressList {
		addressURL, errParse := url.Parse(address)
		if errParse != nil {
			this.Errorf("could not parse address %s of peer %s", address, peerID)
			return nil, nil, errParse
		}

		switch addressURL.Scheme {
		case "tcp4", "tcp":
		default:
			this.Warningf("address scheme %s (in %s) is not supported for peer %s",
				addressURL, address, peerID)
			otherAddressList = append(otherAddressList, address)
			continue
		}

		if _, _, err := net.SplitHostPort(addressURL.Host); err != nil {
			this.Errorf("invalid host:port parameters in address %s: %v", address,
				err)
			return nil, nil, err
		}

		if strings.IndexRune(addressURL.Host, ':') == -1 {
			this.Errorf("mandatory port number is missing in address %s", address)
			return nil, nil, errs.ErrInvalid
		}
		validAddressList = append(validAddressList, address)
	}
	return validAddressList, otherAddressList, nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Messenger) doStart() (status error) {
	// Open a listener on all listen addresses.
	for _, address := range this.state.ListenerAddressList {
		if err := this.startListener(address); err != nil {
			this.Errorf("could not start listener at %s: %v", address, err)
			return err
		}
		defer func() {
			if status != nil {
				if err := this.stopListener(address); err != nil {
					this.Errorf("could not stop listener at %s: %v", address, err)
					status = errs.MergeErrors(status, err)
				}
			}
		}()
	}
	return status
}

func (this *Messenger) doStop() (status error) {
	for address, listener := range this.listenerMap {
		if err := listener.Close(); err != nil {
			this.Errorf("could not close listener %s: %v", address, err)
			status = errs.MergeErrors(status, err)
		}
	}
	this.listenerMap = make(map[string]net.Listener)

	for _, entryCh := range this.requestMap {
		close(entryCh)
	}
	this.requestMap = make(map[int64]chan *Entry)

	for _, peer := range this.peerMap {
		for tport := range peer.transportMap {
			if err := tport.connection.Close(); err != nil {
				this.Errorf("could not close transport %s to peer %s: %v", tport.name,
					peer.peerID, err)
				status = errs.MergeErrors(status, err)
			}
			tport.connection = nil
		}
		this.Infof("closed %d sockets to peer %s", len(peer.transportMap),
			peer.peerID)
		peer.transportMap = make(map[*Transport]struct{})
	}
	return status
}

func (this *Messenger) startListener(address string) error {
	addressURL, errParse := url.Parse(address)
	if errParse != nil {
		this.Errorf("could not parse listener address %s: %v", address, errParse)
		return errParse
	}

	network := ""
	switch addressURL.Scheme {
	case "tcp":
		network = "tcp4"

	default:
		this.Errorf("unsupported listener address scheme %s", addressURL.Scheme)
		return errs.ErrInvalid
	}

	listener, errListen := net.Listen(network, addressURL.Host)
	if errListen != nil {
		this.Errorf("could not open listener at %s: %v", address, errListen)
		return errListen
	}

	this.listenerMap[address] = listener

	this.wg.Add(1)
	go this.goAccept(address, listener)
	return nil
}

func (this *Messenger) stopListener(address string) (status error) {
	listener, found := this.listenerMap[address]
	if !found {
		this.Errorf("could not find listener for address %s", address)
		return errs.ErrNotExist
	}
	delete(this.listenerMap, address)
	if err := listener.Close(); err != nil {
		this.Errorf("could not close the listener %s: %v", address, err)
		status = errs.MergeErrors(status, err)
	}
	return status
}

func (this *Messenger) goAccept(listenerID string, listener net.Listener) {
	defer this.wg.Done()

	for {
		connection, errAccept := listener.Accept()
		if errAccept != nil {
			// If listener is being removed, it shouldn't exist in the listenerMap;
			// otherwise, not being able to accept connections is an unrecoverable
			// error.
			if this.HasListener(listenerID) {
				this.Fatalf("could not accept on a listener %s: %v", listenerID,
					errAccept)
			}
			return
		}
		this.Infof("received incoming connection from %s to listener %s",
			connection.RemoteAddr(), listenerID)

		this.wg.Add(1)
		go func(connection net.Conn) {
			defer this.wg.Done()

			remoteAddr := connection.RemoteAddr()
			_, errTport := this.NewTransport(listenerID, connection,
				this.opts.NegotiationTimeout)
			if errTport != nil {
				this.Errorf("could not create transport on incoming connection "+
					"from %s (ignored): %v", remoteAddr, errTport)
			}
		}(connection)
	}
}

func (this *Messenger) doNewPeer(peerID string, addressList []string) (
	*Peer, error) {

	// Validate all addresses and ignore unsupported addresses.
	validList, unsupportedList, errCheck := this.CheckPeerAddressList(peerID,
		addressList)
	if errCheck != nil {
		this.Errorf("one or more addresses in %v for peer %s are invalid",
			addressList, peerID)
		return nil, errCheck
	}

	if len(unsupportedList) > 0 {
		this.Infof("addresses %v in %v for peer %s are not supported",
			unsupportedList, addressList, peerID)
	}

	if len(validList) == 0 {
		this.Errorf("no supported addresses in %v for peer %s", addressList, peerID)
		return nil, errs.ErrInvalid
	}

	peer := &Peer{
		peerID:       peerID,
		addressList:  validList,
		transportMap: make(map[*Transport]struct{}),
		outCh:        make(chan *Entry, this.opts.SendQueueSize),
	}
	this.peerMap[peerID] = peer

	this.wg.Add(1)
	go this.goSend(peer)
	return peer, nil
}

// goReceive reads incoming messages from a transport and handles them
// appropriately.
func (this *Messenger) goReceive(peer *Peer, tport *Transport) {
	defer this.wg.Done()

	for {
		packetSize, errPeek := tport.packer.PeekSize(tport.reader)
		if errPeek != nil {
			if errPeek != io.EOF && !this.ctlr.IsClosed() {
				this.Errorf("could not peek next packet size from %s: %v", tport.name,
					errPeek)
			}
			atomic.StoreInt32(&tport.bad, 1)
			return
		}
		packet := make([]byte, packetSize)
		if _, err := io.ReadFull(tport.reader, packet); err != nil {
			if errPeek != io.EOF && !this.ctlr.IsClosed() {
				this.Errorf("could not read next message from %s: %v", tport.name, err)
			}
			return
		}
		header, data, errDecode := tport.packer.Decode(packet)
		if errDecode != nil {
			this.Errorf("could not decode incoming message from %s: %v", tport.name,
				errDecode)
			return
		}
		header.ReceiverTimestampNsecs = proto.Int64(time.Now().UnixNano())
		this.Infof("<= %s from %s", header, peer.peerID)
		if err := this.DispatchIncoming(peer.peerID, header, data); err != nil {
			this.Warningf("could not dispatch incoming message %s from %s "+
				"(ignored): %v", header, tport.name, err)
		}
	}
}

// goSend sends outgoing messages on a transport.
func (this *Messenger) goSend(peer *Peer) {
	defer this.wg.Done()

	var entryList []*Entry
	timeoutCh := time.After(this.opts.SendRetryTimeout)
	closeCh := this.ctlr.GetCloseChannel()

	for {
		select {
		case <-closeCh:
			return

		case <-timeoutCh:
			if len(entryList) > 0 {
				count, errFlush := this.FlushMessages(peer, entryList)
				if errFlush != nil {
					this.Errorf("could not send messages to peer %s (will retry): %v",
						peer.peerID, errFlush)
				}
				entryList = entryList[count:]
			}
			timeoutCh = time.After(this.opts.SendRetryTimeout)

		case entry := <-peer.outCh:
			entryList = append(entryList, entry)
			count, errFlush := this.FlushMessages(peer, entryList)
			if errFlush != nil {
				this.Errorf("could not send messages to peer %s (will retry): %v",
					peer.peerID, errFlush)
			}
			entryList = entryList[count:]
		}
	}
}

// goDispatch receives incoming requests from the dispatch queue and calls the
// registered handler in a loop.
func (this *Messenger) goDispatch() {
	defer this.wg.Done()

	closeCh := this.ctlr.GetCloseChannel()
	for {
		select {
		case <-closeCh:
			return
		case entry := <-this.requestCh:
			this.DispatchRequest(entry.header, entry.data)
		}
	}
}
