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
// This file implements unit tests for algo/election package.
//

package election

import (
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	"go-ascent/base/log"
	"go-ascent/msg/simple"
	"go-ascent/paxos/classic"
	"go-ascent/wal/fswal"
)

func TestAlgoElection(test *testing.T) {
	runtime.GOMAXPROCS(4)

	filePath := "/tmp/test_algo_election.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-algo-election")
	logger.Infof("starting new test")

	tmpDir, errTemp := ioutil.TempDir("", "TestAlgoElectionWAL")
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
	if err := walOpts.Validate(); err != nil {
		test.Errorf("could not validate fswal options: %v", err)
		return
	}

	msnOpts := &simple.Options{
		MaxWriteTimeout:        20 * time.Millisecond,
		ResponseQueueSize:      1024,
		SendQueueSize:          1024,
		NegotiationTimeout:     20 * time.Millisecond,
		SendRetryTimeout:       10 * time.Millisecond,
		MaxDispatchRequests:    1024,
		DispatchRequestTimeout: time.Millisecond,
	}
	if err := msnOpts.Validate(); err != nil {
		test.Errorf("could not validate messenger options: %v", err)
		return
	}

	electionOpts := &Options{
		MaxElectionHistory: 10,
		PaxosOptions: classic.Options{
			ProposeRetryInterval:    time.Millisecond,
			NumExtraPhase1Acceptors: 1,
			LearnTimeout:            time.Second,
		},
	}
	if err := electionOpts.Validate(); err != nil {
		test.Errorf("could not validate election options: %v", err)
		return
	}

	type Agent struct {
		log.Logger

		name        string
		addressList []string
		chosen      []byte

		wal      *fswal.WriteAheadLog
		msn      *simple.Messenger
		election *Election
	}

	newAgent := func(name string) *Agent {
		agent := &Agent{}

		wal := &fswal.WriteAheadLog{Logger: logger}
		if err := wal.Initialize(walOpts, tmpDir, name); err != nil {
			test.Errorf("could not create wal for %s: %v", name, err)
			return nil
		}

		msn := &simple.Messenger{Logger: logger}
		if err := msn.Initialize(msnOpts, name); err != nil {
			test.Errorf("could not initialize messenger for %s: %v", name, err)
			return nil
		}

		election := &Election{Logger: logger}
		errInit := election.Initialize(electionOpts, "algo/election", "test", msn,
			wal)
		if errInit != nil {
			test.Errorf("could not initialize election instance for %s: %v", name,
				errInit)
			return nil
		}

		rpcList := ElectionRPCList()
		errRegister := msn.RegisterClass("algo/election", election, rpcList...)
		if errRegister != nil {
			test.Errorf("could not export paxos instance rpcs: %v", errRegister)
			return nil
		}

		agent.Logger = msn
		agent.name = name
		agent.msn = msn
		agent.wal = wal
		agent.election = election
		return agent
	}
	agent1 := newAgent("one")
	agent2 := newAgent("two")
	agent3 := newAgent("three")
	other := newAgent("other")

	startAgent := func(agent *Agent) {
		if err := agent.msn.Start(); err != nil {
			test.Errorf("could not start messenger on %s: %v", agent.name, err)
			return
		}

		msnAddress := "tcp://127.0.0.1:0"
		if err := agent.msn.AddListenerAddress(msnAddress); err != nil {
			test.Errorf("could not add listener address %s to %s: %v", msnAddress,
				agent.name, err)
			return
		}
		agent.addressList = agent.msn.ListenerAddressList()
	}
	startAgent(agent1)
	startAgent(agent2)
	startAgent(agent3)
	startAgent(other)

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
	connectAgents(agent1, agent2, agent3, other)
	connectAgents(agent2, agent1, agent3, other)
	connectAgents(agent3, agent1, agent2, other)
	connectAgents(other, agent1, agent2, agent3)

	configureAgents := func(this *Agent, committee []string) {
		errConfig := this.election.Configure(committee)
		if errConfig != nil {
			test.Errorf("could not configure paxos on %s: %v", this.name,
				errConfig)
			return
		}
	}
	committee := []string{agent1.name, agent2.name, agent3.name}
	configureAgents(agent1, committee)
	configureAgents(agent2, committee)
	configureAgents(agent3, committee)
	configureAgents(other, committee)

	doneCh := make(chan bool)
	monitor := func(client *Agent) {
		defer func() {
			doneCh <- true
		}()

		leader, round, errRefresh := client.election.RefreshLeader(time.Second)
		if errRefresh != nil {
			test.Errorf("could not refresh election status: %v", errRefresh)
			return
		}
		test.Logf("last known election leader is %s for round %d", leader,
			round)

		if leader == "" {
			newLeader, newRound, errElect := client.election.ElectLeader(time.Second)
			if errElect != nil {
				test.Errorf("could not elect new leader: %v", errElect)
				return
			}
			test.Logf("new leader %s is elected for round %d", newLeader,
				newRound)
			leader, round = newLeader, newRound
		}
	}

	go monitor(agent1)
	go monitor(agent2)
	go monitor(agent3)
	go monitor(other)
	<-doneCh
	<-doneCh
	<-doneCh
	<-doneCh

	closeAgent := func(agent *Agent) {
		rpcList := ElectionRPCList()
		errUnregister := agent.msn.UnregisterClass("algo/election", rpcList...)
		if errUnregister != nil {
			test.Errorf("could not unregister election exports: %v", errUnregister)
			return
		}

		if err := agent.election.Close(); err != nil {
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
	_ = closeAgent
	// closeAgent(agent1)
	// closeAgent(agent2)
	// closeAgent(agent3)
	// closeAgent(other)
}
