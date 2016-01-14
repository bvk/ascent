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

package multi

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"go-ascent/algo/election"
	"go-ascent/base/log"
	"go-ascent/msg/simple"
	"go-ascent/paxos/classic"
	"go-ascent/wal/fswal"
)

func TestMultiPaxosPropose(test *testing.T) {
	runtime.GOMAXPROCS(4)

	filePath := "/tmp/test_paxos_multi_propose.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-paxos-multi")
	logger.Infof("starting new test")

	tmpDir, errTemp := ioutil.TempDir("", "TestMultiPaxosPropose")
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
		MaxWriteSize:    32 * 1024,
		MaxReadDirNames: 1024,
		MaxFileSize:     100 * 1024 * 1024,
		FileMode:        os.FileMode(0600),
	}
	if err := walOpts.Validate(); err != nil {
		test.Errorf("could not validate fswal options: %v", err)
		return
	}

	msnOpts := &simple.Options{
		MaxWriteTimeout:        20 * time.Millisecond,
		ResponseQueueSize:      100,
		SendQueueSize:          100,
		NegotiationTimeout:     20 * time.Millisecond,
		SendRetryTimeout:       10 * time.Millisecond,
		MaxDispatchRequests:    100,
		DispatchRequestTimeout: time.Millisecond,
	}
	if err := msnOpts.Validate(); err != nil {
		test.Errorf("could not validate messenger options: %v", err)
		return
	}

	paxosOpts := &Options{
		LeaderElectionTimeout:  time.Second,
		LeaderElectionInterval: time.Second,
		LeaderSetupTimeout:     time.Second,
		LeaderCheckTimeout:     30 * time.Millisecond,
		LeaderCheckInterval:    time.Second,
		ElectionOptions: election.Options{
			MaxElectionHistory: 10,
			PaxosOptions: classic.Options{
				ProposeRetryInterval:    time.Millisecond,
				NumExtraPhase1Acceptors: 1,
				LearnTimeout:            time.Second,
				LearnRetryInterval:      time.Millisecond,
			},
		},
	}
	if err := paxosOpts.Validate(); err != nil {
		test.Errorf("could not validate paxos options: %v", err)
		return
	}

	type Agent struct {
		name        string
		addressList []string

		wal   *fswal.WriteAheadLog
		msn   *simple.Messenger
		paxos *Paxos
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

		if err := msn.Start(); err != nil {
			test.Errorf("could not start messenger on %s: %v", name, err)
			return nil
		}

		msnAddress := "tcp://127.0.0.1:0"
		if err := msn.AddListenerAddress(msnAddress); err != nil {
			test.Errorf("could not add listener address %s to %s: %v", msnAddress,
				name, err)
			return nil
		}
		addressList := msn.ListenerAddressList()

		paxos := &Paxos{Logger: logger}
		errInit := paxos.Initialize(paxosOpts, "paxos/multi", "test", msn, wal)
		if errInit != nil {
			test.Errorf("could not initialize paxos1 instance for %s: %v", name,
				errInit)
			return nil
		}

		rpcList := MultiPaxosRPCList()
		errRegister := msn.RegisterClass("paxos/multi", paxos, rpcList...)
		if errRegister != nil {
			test.Errorf("could not export paxos instance rpcs: %v", errRegister)
			return nil
		}

		agent.name = name
		agent.msn = msn
		agent.wal = wal
		agent.paxos = paxos
		agent.addressList = addressList
		return agent
	}
	agent1 := newAgent("one")
	agent2 := newAgent("two")
	agent3 := newAgent("three")

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
		concurrency int, coordinators, acceptors []string) {
		errConfig := this.paxos.Configure(concurrency, coordinators, acceptors)
		if errConfig != nil {
			test.Errorf("could not configure paxos on %s: %v", this.name,
				errConfig)
			return
		}
	}
	concurrency := 1000
	nodes := []string{agent1.name, agent2.name, agent3.name}
	configureAgents(agent1, concurrency, nodes, nodes)
	configureAgents(agent2, concurrency, nodes, nodes)
	configureAgents(agent3, concurrency, nodes, nodes)

	_ = agent1.paxos.Refresh()
	_ = agent2.paxos.Refresh()
	_ = agent3.paxos.Refresh()

	time.Sleep(time.Second)

	proposeValue := func(agent *Agent, value string) {
		start := time.Now()
		index, errPropose := agent.paxos.Propose([]byte(value), time.Second)
		if errPropose != nil {
			test.Errorf("could not propose value %s through agent %s: %v", value,
				agent.name, errPropose)
			return
		}
		test.Logf("value %s is chosen at index %d in %v time", value, index,
			time.Since(start))
	}

	count := 100
	agents := []*Agent{agent1, agent2, agent3}
	for ii := 0; ii < count; ii++ {
		value := fmt.Sprintf("%d", ii)
		agent := agents[rand.Intn(len(agents))]
		proposeValue(agent, value)
	}
}
