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
// This file implements unit tests for classic paxos.
//

package classic

import (
	"bytes"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	"go-ascent/base/log"
	"go-ascent/msg/simple"
	"go-ascent/wal/fswal"
)

func TestClassicPaxosConsensus(test *testing.T) {
	runtime.GOMAXPROCS(4)

	filePath := "/tmp/test_paxos_classic_consensus.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-paxos-classic")
	logger.Infof("starting new test")

	tmpDir, errTemp := ioutil.TempDir("", "TestWALChanges")
	if errTemp != nil {
		test.Fatalf("could not create temporary directory: %v", errTemp)
	}
	defer func() {
		if !test.Failed() {
			os.RemoveAll(tmpDir)
		}
	}()

	walOpts := &fswal.Options{
		MaxReadSize:     4096,
		MaxWriteSize:    4096,
		MaxReadDirNames: 1024,
		MaxFileSize:     1024 * 1024,
		FileMode:        os.FileMode(0600),
	}

	msnOpts := &simple.Options{
		MaxWriteTimeout:        20 * time.Millisecond,
		ResponseQueueSize:      1024,
		SendQueueSize:          1024,
		NegotiationTimeout:     20 * time.Millisecond,
		SendRetryTimeout:       10 * time.Millisecond,
		MaxDispatchRequests:    10,
		DispatchRequestTimeout: time.Millisecond,
	}
	if err := msnOpts.Validate(); err != nil {
		test.Errorf("could not validate messenger options: %v", err)
		return
	}

	paxosOpts := &Options{
		ProposeRetryInterval:    time.Millisecond,
		NumExtraPhase1Acceptors: 1,
		LearnTimeout:            10 * time.Millisecond,
		LearnRetryInterval:      time.Millisecond,
	}

	type Agent struct {
		name        string
		addressList []string
		chosen      []byte

		wal   *fswal.WriteAheadLog
		msn   *simple.Messenger
		paxos *Paxos
	}

	newAgent := func(name string) *Agent {
		agent := &Agent{}

		wal1 := &fswal.WriteAheadLog{Logger: logger}
		if err := wal1.Initialize(walOpts, tmpDir, name); err != nil {
			test.Errorf("could not create wal for %s: %v", name, err)
			return nil
		}

		msn1 := &simple.Messenger{Logger: logger}
		if err := msn1.Initialize(msnOpts, name); err != nil {
			test.Errorf("could not initialize messenger for %s: %v", name, err)
			return nil
		}

		paxos1 := &Paxos{Logger: logger}
		errInit := paxos1.Initialize(paxosOpts, "paxos/classic", "test", msn1,
			wal1)
		if errInit != nil {
			test.Errorf("could not initialize paxos1 instance for %s: %v", name,
				errInit)
			return nil
		}

		rpcList := PaxosRPCList()
		errRegister := msn1.RegisterClass("paxos/classic", paxos1, rpcList...)
		if errRegister != nil {
			test.Errorf("could not export paxos instance rpcs: %v", errRegister)
			return nil
		}

		agent.name = name
		agent.msn = msn1
		agent.wal = wal1
		agent.paxos = paxos1
		return agent
	}
	agent1 := newAgent("one")
	agent2 := newAgent("two")
	agent3 := newAgent("three")

	startAgent := func(agent *Agent) {
		if err := agent.msn.Start(); err != nil {
			test.Errorf("could not start messenger on %s: %v", agent.name, err)
			return
		}

		msn1Address := "tcp://127.0.0.1:0"
		if err := agent.msn.AddListenerAddress(msn1Address); err != nil {
			test.Errorf("could not add listener address %s to %s: %v", msn1Address,
				agent.name, err)
			return
		}
		agent.addressList = agent.msn.ListenerAddressList()
	}
	startAgent(agent1)
	startAgent(agent2)
	startAgent(agent3)

	connectAgents := func(this *Agent, rest ...*Agent) {
		for _, other := range rest {
			errAdd := this.msn.AddPeerAddress(other.name, other.addressList)
			if errAdd != nil {
				test.Errorf("could not add peer %s to %s: %v", this.name, other.name,
					errAdd)
				return
			}
		}
	}
	connectAgents(agent1, agent2, agent3)
	connectAgents(agent2, agent1, agent3)
	connectAgents(agent3, agent1, agent2)

	configureAgents := func(this *Agent,
		proposers, acceptors, learners []string) {
		errConfig := this.paxos.Configure(proposers, acceptors, learners)
		if errConfig != nil {
			test.Errorf("could not configure paxos on %s: %v", this.name,
				errConfig)
			return
		}
	}
	agents := []string{agent1.name, agent2.name, agent3.name}
	configureAgents(agent1, agents, agents, agents)
	configureAgents(agent2, agents, agents, agents)
	configureAgents(agent3, agents, agents, agents)

	doneCh := make(chan bool)
	propose := func(client *Agent, value string) {
		defer func() {
			doneCh <- true
		}()

		start := time.Now()
		chosen, errProp := client.paxos.Propose([]byte(value), time.Second)
		if errProp != nil {
			test.Errorf("could not propose value %s: %v", value, errProp)
			return
		}

		test.Logf("classic paxos consensus took %v time to choose %s for %s",
			time.Since(start), chosen, client.name)
		client.chosen = chosen
	}
	go propose(agent1, "agent1")
	go propose(agent2, "agent2")
	go propose(agent3, "agent3")
	<-doneCh
	<-doneCh
	<-doneCh

	if bytes.Compare(agent1.chosen, agent2.chosen) != 0 ||
		bytes.Compare(agent2.chosen, agent3.chosen) != 0 ||
		bytes.Compare(agent3.chosen, agent1.chosen) != 0 {
		test.Errorf("different values are chosen, which is wrong")
		return
	}

	closeAgent := func(agent *Agent) {
		rpcList := PaxosRPCList()
		errUnregister := agent.msn.UnregisterClass("paxos/classic", rpcList...)
		if errUnregister != nil {
			test.Errorf("could not unregister paxos instance exports: %v",
				errUnregister)
			return
		}

		if err := agent.paxos.Close(); err != nil {
			test.Errorf("could not close paxos on %s: %v", agent.name, err)
			return
		}
		if err := agent.msn.Stop(); err != nil {
			test.Errorf("could not stop messenger on %s: %v", agent.name, err)
			return
		}
		if err := agent.msn.Close(); err != nil {
			test.Errorf("could not close messenger on %s: %v", agent.name, err)
			return
		}
		if err := agent.wal.Close(); err != nil {
			test.Errorf("could not close wal on %s: %v", agent.name, err)
			return
		}
	}
	closeAgent(agent1)
	closeAgent(agent2)
	closeAgent(agent3)
}
