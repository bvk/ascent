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
// This file implements simple unit tests for Messenger data type.
//

package simple

import (
	"testing"
	"time"

	"go-ascent/base/log"

	msgpb "proto-ascent/msg"
)

func TestLoopbackMessages(test *testing.T) {
	filePath := "/tmp/test_msg_simple_loopback_messages.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-msg-simple")
	logger.Infof("starting new test")

	opts := &Options{
		MaxWriteTimeout:     20 * time.Millisecond,
		ResponseQueueSize:   1,
		SendQueueSize:       1,
		NegotiationTimeout:  20 * time.Millisecond,
		SendRetryTimeout:    10 * time.Millisecond,
		MaxDispatchRequests: 1,
	}
	if err := opts.Validate(); err != nil {
		test.Errorf("could not validate messenger options: %v", err)
		return
	}

	msn1 := &Messenger{Logger: logger}
	if err := msn1.Initialize(opts, "msn1"); err != nil {
		test.Errorf("could not initialize messenger msn1: %v", err)
		return
	}
	defer func() {
		if err := msn1.Close(); err != nil {
			test.Errorf("could not close messenger msn1: %v", err)
		}
	}()

	if err := msn1.Start(); err != nil {
		test.Errorf("could not start messenger msn1: %v", err)
		return
	}
	defer func() {
		if err := msn1.Stop(); err != nil {
			test.Errorf("could not stop messenger msn1: %v", err)
		}
	}()

	server := &RPCServer{Logger: logger, Messenger: msn1}

	if err := msn1.RegisterClass("msn1", server, "a", "b", "c"); err != nil {
		test.Errorf("could not export rpc services: %v", err)
		return
	}
	defer func() {
		if err := msn1.UnregisterClass("msn1", "a", "b", "c"); err != nil {
			test.Errorf("could not unregister rpc services: %v", err)
		}
	}()

	start := time.Now()
	for ii := 0; ii < 1000; ii++ {
		reqHeader := msn1.NewRequest("msn1", "", "a", time.Second)
		if err := msn1.Send("msn1", reqHeader, []byte("request")); err != nil {
			test.Errorf("could not send request to self: %v", err)
			return
		}
		if _, _, err := msn1.Receive(reqHeader); err != nil {
			test.Errorf("could not receive response from self: %v", err)
			return
		}
	}
	duration := time.Since(start)
	test.Logf("average loopback round-trip time is %v", duration/1000)
}

func TestNetworkMessaging(test *testing.T) {
	filePath := "/tmp/test_msg_simple_network_messaging.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-msg-simple")
	logger.Infof("starting new test")

	opts := &Options{
		MaxWriteTimeout:        20 * time.Millisecond,
		ResponseQueueSize:      1,
		SendQueueSize:          1,
		NegotiationTimeout:     20 * time.Millisecond,
		SendRetryTimeout:       10 * time.Millisecond,
		MaxDispatchRequests:    1,
		DispatchRequestTimeout: time.Millisecond,
	}
	if err := opts.Validate(); err != nil {
		test.Errorf("could not validate messenger options: %v", err)
		return
	}

	msn1 := &Messenger{Logger: logger}
	if err := msn1.Initialize(opts, "msn1"); err != nil {
		test.Errorf("could not initialize messenger msn1: %v", err)
		return
	}
	defer func() {
		if err := msn1.Close(); err != nil {
			test.Errorf("could not close messenger msn1: %v", err)
		}
	}()

	msn2 := &Messenger{Logger: logger}
	if err := msn2.Initialize(opts, "msn2"); err != nil {
		test.Errorf("could not initialize messenger msn2: %v", err)
		return
	}
	defer func() {
		if err := msn2.Close(); err != nil {
			test.Errorf("could not close messenger msn2: %v", err)
		}
	}()

	if err := msn1.Start(); err != nil {
		test.Errorf("could not start messenger msn1: %v", err)
		return
	}
	defer func() {
		if err := msn1.Stop(); err != nil {
			test.Errorf("could not stop messenger msn1: %v", err)
		}
	}()

	if err := msn2.Start(); err != nil {
		test.Errorf("could not start messenger msn2: %v", err)
		return
	}
	defer func() {
		if err := msn2.Stop(); err != nil {
			test.Errorf("could not stop messenger msn2: %v", err)
		}
	}()

	msn1Address := "tcp://127.0.0.1:10000"
	if err := msn1.AddListenerAddress(msn1Address); err != nil {
		test.Errorf("could not add listener address %s to msn1: %v", msn1Address,
			err)
		return
	}
	msn1AddressList := msn1.ListenerAddressList()

	msn2Address := "tcp://127.0.0.1:20000"
	if err := msn2.AddListenerAddress(msn2Address); err != nil {
		test.Errorf("could not add listener address %s to msn2: %v", msn2Address,
			err)
		return
	}
	msn2AddressList := msn2.ListenerAddressList()

	if err := msn1.AddPeerAddress("msn2", msn2AddressList); err != nil {
		test.Errorf("could not add msn2 addresses to msn1: %v", err)
		return
	}

	if err := msn2.AddPeerAddress("msn1", msn1AddressList); err != nil {
		test.Errorf("could not add msn1 addresses to msn2: %v", err)
		return
	}

	// Register a service on msn1.
	msn1server := &RPCServer{Logger: logger, Messenger: msn1}

	if err := msn1.RegisterClass("class", msn1server, "ping"); err != nil {
		test.Errorf("could not export rpc services: %v", err)
		return
	}
	defer func() {
		if err := msn1.UnregisterClass("class", "ping"); err != nil {
			test.Errorf("could not unregister rpc services: %v", err)
		}
	}()

	const count = 1000
	start := time.Now()
	for ii := 0; ii < count; ii++ {
		reqHeader := msn2.NewRequest("class", "", "ping", time.Second)
		reqData := []byte("request-data")
		if err := msn2.Send("msn1", reqHeader, reqData); err != nil {
			test.Errorf("could not send request to msn1: %v", err)
			return
		}
		resHeader, _, errRecv := msn2.Receive(reqHeader)
		if errRecv != nil {
			test.Errorf("could not receive response from msn1: %v", errRecv)
			return
		}
		logger.Infof("[%s] -> [%s]", reqHeader, resHeader)
		if err := msn2.CloseMessage(reqHeader); err != nil {
			test.Errorf("could not close request %s: %v", reqHeader, err)
			return
		}
	}
	duration := time.Since(start)
	test.Logf("average loopback network round-trip time is %v", duration/count)
}

///////////////////////////////////////////////////////////////////////////////

type RPCServer struct {
	log.Logger
	*Messenger

	rpcCount int
}

func (this *RPCServer) Dispatch(header *msgpb.Header, data []byte) error {
	response := this.NewResponse(header)
	sourceID := header.GetMessengerId()
	if err := this.Send(sourceID, response, []byte("response-data")); err != nil {
		this.Errorf("could not send response for %s to messenger %s: %v", header,
			sourceID, err)
		return err
	}
	return nil
}
