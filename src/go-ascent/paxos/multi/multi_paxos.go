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
// This file defines Paxos type which implements Multi-Paxos functionality.
//
// THREAD SAFETY
//
// All public functions are thread-safe.
//
// NOTES
//
// This multi-paxos implementation doesn't support reconfiguration.
//
// In multi-paxos, coordinators takes both Proposer and Learner roles of
// classic paxos. A Proposer role is assumed so that leader coordinator can
// issue phase1 and phase2 operations. A Learner role is assumed so that
// coordinator can limit multi-paxos concurrency.
//

package multi

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"go-ascent/algo/election"
	"go-ascent/base/errs"
	"go-ascent/base/log"
	"go-ascent/msg"
	"go-ascent/thread/ctlr"
	"go-ascent/wal"

	msgpb "proto-ascent/msg"
	thispb "proto-ascent/paxos/multi"
)

// ZeroValue is the special value proposed by the leaders to resolve the state
// to a consistent point after the election.
var ZeroValue = []byte("")

// Options define user configurable settings.
type Options struct {
	// Maximum time to wait for a leader election to complete.
	LeaderElectionTimeout time.Duration

	// Maximum time to wait before starting a new election if previous election
	// couldn't be completed.
	LeaderElectionInterval time.Duration

	// Maximum time to wait for leader initialization to complete after an
	// election.
	LeaderSetupTimeout time.Duration

	// Maximum time to wait in checking for current leader health.
	LeaderCheckTimeout time.Duration

	// Maximum time interval between successive leader health checks.
	LeaderCheckInterval time.Duration

	// User options for leader election.
	ElectionOptions election.Options
}

// Validate checks if user configuration items are all valid.
func (this *Options) Validate() (status error) {
	if this.LeaderElectionTimeout < 10*time.Millisecond {
		err := errs.NewErrInvalid("leader election timeout is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.LeaderElectionInterval < 10*time.Millisecond {
		err := errs.NewErrInvalid("leader election interval is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.LeaderSetupTimeout < 10*time.Millisecond {
		err := errs.NewErrInvalid("leader initialization timeout is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.LeaderCheckTimeout < 10*time.Millisecond {
		err := errs.NewErrInvalid("leader health check timeout is too small")
		status = errs.MergeErrors(status, err)
	}
	if this.LeaderCheckInterval < 10*time.Millisecond {
		err := errs.NewErrInvalid("leader health check interval is too small")
		status = errs.MergeErrors(status, err)
	}
	return status
}

// Paxos type implements Multi-Paxos algorithm.
type Paxos struct {
	log.Logger

	// The Messenger.
	msn msg.Messenger

	// Write ahead log to save mutli-paxos state.
	wal wal.WriteAheadLog

	// Resource controller for admission control and synchronization.
	ctlr ctlr.BasicController

	// Alarm handler to schedule periodic and background jobs.
	alarm ctlr.Alarm

	// Rpc namespace for the multi-paxos rpcs. All objects that participate in
	// this multi-paxos instance share same uid.
	namespace string

	// Globally unique id for the multi-paxos instance. All objects that
	// participate in this multi-paxos instance share same uid.
	uid string

	// User configurable options.
	opts Options

	//
	// Multi-Paxos instance configuration.
	//

	// Atomic variables indicating the role of this multi-paxos object. Every
	// multi-paxos object is a multi-paxos client by default, so no special
	// variable is defined for the client role.
	isAcceptor    int32
	isCoordinator int32

	// If this object is a coordinator, it's index the sorted coordinator list.
	coordinatorIndex int

	// List of acceptor nodes.
	acceptorList []string

	// List of coordinator nodes. Coordinator nodes are take both, proposer and
	// learner roles.
	coordinatorList []string

	// Atomic variable for the majority quorum size.
	majoritySize int32

	// Maximum allowed, concurrent consensus operations. Multi-Paxos allows
	// consensus on different sequence ids in parallel.
	concurrency int

	//
	// Coordinator state.
	//

	// Leader election instance for coordinators.
	election election.Election

	// Latest phase1 ballot in-use or used by the coordinator.
	phase1Ballot int64

	// Mapping from sequence id to the value chosen on that sequence id.
	chosenMap map[int64][]byte

	// Next unused sequence id.
	nextUnusedSequenceID int64

	// Smallest sequence id whose consensus result is not yet known.
	leastUncommittedSequenceID int64

	// Atomic variable indicating the current leader state. If non-zero, this
	// coordinator is the leader. If positive, leader initialization is completed
	// successfully.
	leaderInitialized int32

	// Wall timestamp when leader initialization is started. This value can be
	// used by others to figure out if leader initialization is taking too long
	// on this node.
	leaderTimestampNsecs int64

	//
	// Acceptor state.
	//

	// The infinite ballot number promised by the acceptor in phase1.
	promisedBallot int64

	// Largest sequence id voted by this acceptor.
	largestVotedSequenceID int64

	// Mapping from sequence id to the largest ballot voted for that sequence id.
	votedBallotMap map[int64]int64

	// Mapping from sequence id to the latest value voted for that sequence.
	votedValueMap map[int64][]byte
}

// MultiPaxosRPCList returns list of rpcs handled by multi-paxos objects.
func MultiPaxosRPCList() []string {
	electionRPCList := election.ElectionRPCList()
	rpcList := []string{"MultiPaxos.Status", "MultiPaxos.Propose",
		"MultiPaxos.Phase1", "MultiPaxos.Phase2"}
	return append(rpcList, electionRPCList...)
}

// Initialize initializes a multi paxos instance.
func (this *Paxos) Initialize(opts *Options, namespace, uid string,
	msn msg.Messenger, wal wal.WriteAheadLog) (status error) {

	if err := opts.Validate(); err != nil {
		this.Errorf("invalid user options: %v", err)
		return err
	}

	re := regexp.MustCompile(uid)
	if err := wal.ConfigureRecoverer(re, this); err != nil {
		this.Errorf("could not configure wal recoverer: %v", err)
		return err
	}
	defer func() {
		if status != nil {
			if err := wal.ConfigureRecoverer(re, nil); err != nil {
				this.Errorf("could not unconfigure wal recoverer: %v", err)
				status = errs.MergeErrors(status, err)
			}
		}
	}()

	this.election.Logger = this.Logger
	errInit := this.election.Initialize(&opts.ElectionOptions, namespace,
		uid+"/election", msn, wal)
	if errInit != nil {
		this.Errorf("could not initialize leader election object: %v", errInit)
		return errInit
	}
	defer func() {
		if status != nil {
			if err := this.election.Close(); err != nil {
				this.Errorf("could not close leader election object: %v", err)
				status = errs.MergeErrors(status, err)
			}
		}
	}()

	this.wal = wal
	this.msn = msn
	this.uid = uid
	this.opts = *opts
	this.namespace = namespace
	this.majoritySize = -1
	this.promisedBallot = -1
	this.coordinatorIndex = -1
	this.concurrency = 0
	this.nextUnusedSequenceID = 0
	this.leastUncommittedSequenceID = 0
	this.largestVotedSequenceID = -1
	this.chosenMap = make(map[int64][]byte)
	this.votedValueMap = make(map[int64][]byte)
	this.votedBallotMap = make(map[int64]int64)

	this.Logger = this.NewLogger("multi-paxos:%s-%s", this.msn.UID(), uid)
	this.ctlr.Initialize(this)
	this.alarm.Initialize()
	return nil
}

// Close releases all resources and destroys the object.
func (this *Paxos) Close() (status error) {
	if err := this.ctlr.Close(); err != nil {
		return err
	}

	if err := this.election.Close(); err != nil {
		this.Errorf("could not close election object: %v", err)
		status = errs.MergeErrors(status, err)
	}

	if err := this.alarm.Close(); err != nil {
		this.Errorf("could not close alarm handler: %v", err)
		status = errs.MergeErrors(status, err)
	}

	re := regexp.MustCompile(this.uid)
	if err := this.wal.ConfigureRecoverer(re, nil); err != nil {
		this.Errorf("could not unconfigure wal recoverer: %v", err)
		status = errs.MergeErrors(status, err)
	}
	return nil
}

// IsCoordinator returns true if this object is serving a coordinator role.
func (this *Paxos) IsCoordinator() bool {
	return atomic.LoadInt32(&this.isCoordinator) != 0
}

// IsAcceptor returns true if this object is serving an acceptor role.
func (this *Paxos) IsAcceptor() bool {
	return atomic.LoadInt32(&this.isAcceptor) != 0
}

// MajoritySize returns the majority quorum size for this multi-paxos instance.
func (this *Paxos) MajoritySize() int {
	return int(atomic.LoadInt32(&this.majoritySize))
}

// IsConfigured returns true if this object is configured.
func (this *Paxos) IsConfigured() bool {
	return this.MajoritySize() > 0
}

// IsLeader returns true if this object is the multi-paxos leader.
func (this *Paxos) IsLeader() bool {
	return atomic.LoadInt32(&this.leaderInitialized) != 0
}

// IsLeaderInitialized returns true if this object is the leader and is
// initialized successfully.
func (this *Paxos) IsLeaderInitialized() bool {
	return atomic.LoadInt32(&this.leaderInitialized) > 0
}

// Configure configures this multi-paxos object with its peer information.
func (this *Paxos) Configure(concurrency int, coordinators,
	acceptors []string) (status error) {

	lock, errLock := this.ctlr.LockAll()
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	if this.IsConfigured() {
		this.Errorf("this multi-paxos object is already configured")
		return errs.ErrExist
	}

	// TODO: Check for duplicates in the inputs.

	acceptorList := make([]string, len(acceptors))
	copy(acceptorList, acceptors)
	sort.Sort(sort.StringSlice(acceptorList))

	coordinatorList := make([]string, len(coordinators))
	copy(coordinatorList, coordinators)
	sort.Sort(sort.StringSlice(coordinatorList))

	self := this.msn.UID()
	isAcceptor := false
	for _, acceptor := range acceptorList {
		if acceptor == self {
			isAcceptor = true
			break
		}
	}

	isCoordinator := false
	coordinatorIndex := -1
	for ii, coordinator := range coordinatorList {
		if coordinator == self {
			isCoordinator = true
			coordinatorIndex = ii
			break
		}
	}

	if isCoordinator {
		if err := this.election.Configure(coordinatorList); err != nil {
			this.Errorf("could not configure leader election object: %v", err)
			return err
		}
		if err := this.election.SetCommitteeWatch(this); err != nil {
			this.Errorf("could not set leader election watch: %v", err)
			return err
		}
	}

	// Save configuration in the wal.
	config := thispb.Configuration{}
	config.AcceptorList = acceptorList
	config.CoordinatorList = coordinatorList
	config.Concurrency = proto.Int32(int32(concurrency))
	config.MajoritySize = proto.Int32(int32(len(acceptors)/2 + 1))
	if isAcceptor {
		config.IsAcceptor = proto.Bool(true)
	}
	if isCoordinator {
		config.IsCoordinator = proto.Bool(true)
		config.CoordinatorIndex = proto.Int32(int32(coordinatorIndex))
	}
	if err := this.doUpdateConfig(&config); err != nil {
		this.Errorf("could not commit configuration: %v", err)
		return err
	}
	return nil
}

// RecoverCheckpoint recovers state from a checkpoint record.
func (this *Paxos) RecoverCheckpoint(uid string, data []byte) error {
	if uid != this.uid {
		if this.uid+"/election" == uid {
			return this.election.RecoverCheckpoint(uid, data)
		}
		this.Errorf("checkpoint record doesn't belong to this instance")
		return errs.ErrInvalid
	}

	walRecord := thispb.WALRecord{}
	if err := proto.Unmarshal(data, &walRecord); err != nil {
		this.Errorf("could not parse checkpoint wal record: %v", err)
		return err
	}

	if walRecord.Checkpoint == nil {
		this.Errorf("checkpoint record has no data")
		return errs.ErrCorrupt
	}
	checkpoint := walRecord.GetCheckpoint()

	config := checkpoint.GetConfiguration()
	this.doRestoreConfig(config)

	if this.IsAcceptor() {
		if checkpoint.AcceptorState == nil {
			this.Errorf("checkpoint record has no acceptor state")
			return errs.ErrCorrupt
		}
		this.doRestoreAcceptor(checkpoint.GetAcceptorState())
	}

	if this.IsCoordinator() {
		if checkpoint.CoordinatorState == nil {
			this.Errorf("checkpoint record has no coordinator state")
			return errs.ErrCorrupt
		}
		this.doRestoreCoordinator(checkpoint.GetCoordinatorState())
	}
	return nil
}

// RecoverChange recovers an update from a change record.
func (this *Paxos) RecoverChange(lsn wal.LSN, uid string, data []byte) error {
	if lsn == nil {
		// We reached end of wal recovery, figure out the final state and resume
		// any inflight operations.
		if this.IsCoordinator() {
			if err := this.RecoverChange(lsn, uid, data); err != nil {
				this.Errorf("could not recover final election change: %v", err)
				return err
			}
		}

		errSched := this.alarm.ScheduleAt(this.uid+"/Refresh", time.Now(),
			this.Refresh)
		if errSched != nil {
			this.Errorf("could not schedule refresh operation: %v", errSched)
			return errSched
		}
		return nil
	}

	if uid != this.uid {
		if this.uid+"/election" == uid {
			return this.election.RecoverChange(lsn, uid, data)
		}
		this.Errorf("change record doesn't belong to this instance")
		return errs.ErrInvalid
	}

	walRecord := thispb.WALRecord{}
	if err := proto.Unmarshal(data, &walRecord); err != nil {
		this.Errorf("could not parse change record: %v", err)
		return err
	}

	switch {
	case walRecord.ConfigChange != nil:
		return this.doUpdateConfig(walRecord.GetConfigChange())

	case walRecord.AcceptorChange != nil:
		return this.doUpdateAcceptor(walRecord.GetAcceptorChange())

	case walRecord.CoordinatorChange != nil:
		return this.doUpdateCoordinator(walRecord.GetCoordinatorChange())

	default:
		this.Errorf("invalid/corrupt wal change record: %s", walRecord)
		return errs.ErrCorrupt
	}
	return nil
}

// TakeCheckpoint saves current state into the wal as a checkpoint record.
func (this *Paxos) TakeCheckpoint() error {
	lock := this.ctlr.ReadLockAll()
	defer lock.Unlock()

	if !this.IsConfigured() {
		this.Errorf("multi paxos instance is not yet configured")
		return errs.ErrInvalid
	}

	checkpoint := thispb.Checkpoint{}

	config := thispb.Configuration{}
	this.doSaveConfig(&config)
	checkpoint.Configuration = &config

	if this.IsAcceptor() {
		state := thispb.AcceptorState{}
		this.doSaveAcceptor(&state)
		checkpoint.AcceptorState = &state
	}

	if this.IsCoordinator() {
		state := thispb.CoordinatorState{}
		this.doSaveCoordinator(&state)
		checkpoint.CoordinatorState = &state
	}

	walRecord := thispb.WALRecord{}
	walRecord.Checkpoint = &checkpoint
	errAppend := wal.AppendCheckpointProto(this.wal, this.uid, &walRecord)
	if errAppend != nil {
		this.Errorf("could not append checkpoint record: %v", errAppend)
		return errAppend
	}
	lock.Unlock()

	if this.IsCoordinator() {
		return this.election.TakeCheckpoint()
	}
	return nil
}

// Refresh inspects paxos object state and takes recovery actions as necessary.
func (this *Paxos) Refresh() (status error) {
	if this.IsCoordinator() {
		return this.LeaderCheck()
	}
	return nil
}

// GetNextHigherBallot picks new ballot number for the coordinator that is
// higher than the given 'ballot' value.
func (this *Paxos) GetNextHigherBallot(ballot int64) (int64, error) {
	lock, errLock := this.ctlr.Lock("coordinator")
	if errLock != nil {
		return -1, errLock
	}
	defer lock.Unlock()

	return this.doGetNextHigherBallot(ballot)
}

// ElectionUpdate handles the callback from election module.
func (this *Paxos) ElectionUpdate(round int64, leader string) {
	// Giving up leadership should be lock-free.
	self := this.msn.UID()
	if this.IsLeader() && leader != self {
		atomic.StoreInt32(&this.leaderInitialized, 0)
	}
}

// LeaderCheck periodically monitors the leader for the health status.
func (this *Paxos) LeaderCheck() (status error) {
	defer func() {
		if status != nil && errs.IsNoLeader(status) {
			errSched := this.alarm.ScheduleAt(this.uid+"/StartNewElection",
				time.Now(), this.StartNewElection)
			if errSched != nil {
				this.Errorf("could not schedule to begin new election: %v", errSched)
			}
			return
		}

		// Schedule leader check again after the timeout.
		next := time.Now().Add(this.opts.LeaderCheckInterval)
		errSched := this.alarm.ScheduleAt(this.uid+"/LeaderCheck", next,
			this.LeaderCheck)
		if errSched != nil {
			this.Errorf("could not schedule to leader check: %v", errSched)
		}
	}()

	leader := this.election.CurrentLeader()
	if len(leader) == 0 {
		this.Warningf("forcing new leader election because past leader is unknown")
		return errs.ErrNoLeader
	}

	self := this.msn.UID()
	if leader == self {
		return nil
	}

	// Send a status rpc to the leader to check he is alive. Otherwise, begin a
	// new election round for new leader.

	statusHeader := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Status", this.opts.LeaderCheckTimeout)
	defer this.msn.CloseMessage(statusHeader)

	statusRequest := thispb.StatusRequest{}
	message := thispb.PaxosMessage{StatusRequest: &statusRequest}
	errSend := msg.SendProto(this.msn, leader, statusHeader, &message)
	if errSend != nil {
		this.Errorf("could not send status request to check for leader health: %v",
			errSend)
		return errSend
	}

	message.Reset()
	_, errRecv := msg.ReceiveProto(this.msn, statusHeader, &message)
	if errRecv != nil {
		this.Errorf("could not receive status response from leader %s: %v",
			leader, errRecv)
		return errRecv
	}
	if message.StatusResponse == nil {
		this.Errorf("status response from leader is empty")
		return errs.ErrCorrupt
	}
	response := message.GetStatusResponse()
	if response.LeaderInitializeStatus == nil {
		this.Errorf("no leader initialization status in the status response")
		return errs.ErrNoLeader
	}
	leaderInitialized := response.GetLeaderInitializeStatus()
	if leaderInitialized > 0 {
		this.Infof("leader %s is initialized and healthy", leader)
		return nil
	}

	if leaderInitialized < 0 {
		if response.LeaderInitializeTimestampNsecs == nil {
			this.Errorf("no leader initialization timestamp is the status response")
			return errs.ErrCorrupt
		}
		nowLeader := TimeFromUnixNano(response.GetTimestampNsecs())
		initTimestampNsecs := response.GetLeaderInitializeTimestampNsecs()
		initLeader := TimeFromUnixNano(initTimestampNsecs)
		initDuration := nowLeader.Sub(initLeader)
		if initDuration > this.opts.LeaderSetupTimeout {
			this.Errorf("leader %s is found initializing for %v, which is too "+
				"long; starting new election", leader, initDuration)
			return errs.ErrNoLeader
		}
		this.Infof("leader %s is initializing (%s passed)", leader, initDuration)
	}
	return nil
}

// StartNewElection begins new leader election round.
func (this *Paxos) StartNewElection() (status error) {
	newLeader, newRound, errElect := this.election.ElectLeader(
		this.opts.LeaderElectionTimeout)
	if errElect != nil {
		this.Errorf("could not elect new leader: %v", errElect)
		return errElect
	}
	this.Infof("new leader %s is elected for round %d", newLeader, newRound)

	self := this.msn.UID()
	if self == newLeader {
		return this.TakeLeadership()
	}

	next := time.Now().Add(this.opts.LeaderCheckInterval)
	errSched := this.alarm.ScheduleAt(this.uid+"/LeaderCheck", next,
		this.LeaderCheck)
	if errSched != nil {
		this.Errorf("could not schedule to leader check: %v", errSched)
		return errSched
	}
	return nil
}

// TakeLeadership performs leader initializations.
func (this *Paxos) TakeLeadership() (status error) {
	if !this.IsCoordinator() {
		this.Errorf("this object is not a coordinator")
		return errs.ErrInvalid
	}

	this.Infof("starting leader initialization operations")

	// Make a copy of all necessary configuration parameters.
	rlock := this.ctlr.ReadLockAll()
	acceptorList := append([]string{}, this.acceptorList...)
	majoritySize := this.MajoritySize()
	concurrency := this.concurrency
	leastUncommittedSequenceID := this.leastUncommittedSequenceID
	rlock.Unlock()

	// Do not allow two instances of this function by taking exclusive lock on
	// the function name.
	serialize, errSerialize := this.ctlr.Lock("TakeLeadership")
	if errSerialize != nil {
		return errSerialize
	}
	defer serialize.Unlock()

	start := time.Now()
	deadline := start.Add(this.opts.LeaderSetupTimeout)

	atomic.StoreInt32(&this.leaderInitialized, -1)
	atomic.StoreInt64(&this.leaderTimestampNsecs, start.UnixNano())
	defer func() {
		if status == nil {
			// Using compare-and-swap checks that leadership was not taken away
			// during leader initialization.
			if !atomic.CompareAndSwapInt32(&this.leaderInitialized, -1, 1) {
				atomic.StoreInt64(&this.leaderTimestampNsecs, 0)
			}
		} else {
			atomic.StoreInt64(&this.leaderTimestampNsecs, 0)
			atomic.StoreInt32(&this.leaderInitialized, 0)
			this.Errorf("giving up leadership cause leader couldn't initialize: %v",
				status)
		}
	}()

	// Get Status responses from majority of acceptors to figure out the highest
	// promised ballot.
	statusHeader := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Status", deadline.Sub(time.Now()))
	defer this.msn.CloseMessage(statusHeader)

	statusRequest := thispb.StatusRequest{}
	statusMessage := thispb.PaxosMessage{StatusRequest: &statusRequest}
	count, errSend := msg.SendAllProto(this.msn, acceptorList, statusHeader,
		&statusMessage)
	if errSend != nil && count < majoritySize {
		this.Errorf("could not send status request to majority: %v", errSend)
		return errSend
	}

	maxBallotNumber := int64(-1)
	statusSet := make(map[string]struct{})
	for ii := 0; ii < count && len(statusSet) < majoritySize; ii++ {
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, statusHeader, &message)
		if errRecv != nil {
			this.Errorf("could not receive enough status responses: %v", errRecv)
			return errRecv
		}
		acceptor := resHeader.GetMessengerId()
		if _, ok := statusSet[acceptor]; ok {
			continue
		}
		response := message.GetStatusResponse()
		if response != nil && response.PromisedBallot != nil {
			statusSet[acceptor] = struct{}{}
			maxPromised := response.GetPromisedBallot()
			if maxPromised > maxBallotNumber {
				maxBallotNumber = maxPromised
			}
		}
	}
	this.Infof("leader has received majority status responses from %v",
		statusSet)

	// Pick a ballot number higher than the max promised ballot number.
	ballot, errGet := this.GetNextHigherBallot(maxBallotNumber)
	if errGet != nil {
		this.Errorf("could not find a higher ballot number: %v", errGet)
		return errGet
	}
	this.Infof("leader decided to use ballot %d for phase1 request", ballot)

	// Get Phase1 responses from majority of acceptors for the higher ballot
	// number.
	phase1Header := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Phase1", deadline.Sub(time.Now()))
	defer this.msn.CloseMessage(phase1Header)

	phase1Request := thispb.Phase1Request{}
	phase1Request.BallotNumber = proto.Int64(ballot)
	phase1Request.LeastUncommittedSequenceId = proto.Int64(
		leastUncommittedSequenceID)
	phase1Message := thispb.PaxosMessage{Phase1Request: &phase1Request}
	count, errSend = msg.SendAllProto(this.msn, acceptorList, phase1Header,
		&phase1Message)
	if errSend != nil && count < majoritySize {
		this.Errorf("could not send phase1 request to majority: %v", errSend)
		return errSend
	}

	phase1Map := make(map[string]*thispb.Phase1Response)
	for ii := 0; ii < count && len(phase1Map) < majoritySize; ii++ {
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, phase1Header, &message)
		if errRecv != nil {
			this.Errorf("could not receive phase1 responses from majority: %v",
				errRecv)
			return errRecv
		}
		acceptor := resHeader.GetMessengerId()
		if _, ok := phase1Map[acceptor]; ok {
			continue
		}
		response := message.GetPhase1Response()
		if response == nil {
			continue
		}
		if response.GetPromisedBallot() != ballot {
			continue
		}
		phase1Map[acceptor] = response
	}

	leader := this.msn.UID()
	this.Infof("leader %s got majority number of phase1 responses %v",
		leader, phase1Map)

	// Process the phase1 responses to figure out sequence ids that needs to be
	// resolved as part of leader change.

	largestSequenceID := int64(-1)
	leastSequenceID := int64(math.MaxInt64)
	maxVotedBallotMap := make(map[int64]int64)
	maxVotedValueMap := make(map[int64][]byte)
	numBallotResponseMap := make(map[int64]int)
	for _, phase1response := range phase1Map {
		for _, state := range phase1response.SequenceStateList {
			sequenceID := state.GetSequenceId()
			numBallotResponseMap[sequenceID]++

			if sequenceID < leastSequenceID {
				leastSequenceID = sequenceID
			}
			if sequenceID > largestSequenceID {
				largestSequenceID = sequenceID
			}

			ballot := state.GetVotedBallot()
			value := state.GetVotedValue()
			maxBallot, found := maxVotedBallotMap[sequenceID]
			if !found || maxBallot < ballot {
				maxVotedBallotMap[sequenceID] = ballot
				maxVotedValueMap[sequenceID] = value
			}
		}
	}

	change := thispb.CoordinatorChange{}

	phase2Request := thispb.Phase2Request{}
	phase2Request.BallotNumber = proto.Int64(ballot)
	for ii := leastSequenceID; ii < largestSequenceID; ii++ {
		sequenceID := ii

		chosen, isChosen := this.chosenMap[sequenceID]
		if !isChosen && numBallotResponseMap[sequenceID] < majoritySize {
			this.Fatalf("could not receive majority phase1 responses for "+
				"sequence id %d", sequenceID)
		}

		var proposedValue []byte
		if isChosen {
			proposedValue = chosen
		} else if value, ok := maxVotedValueMap[sequenceID]; ok {
			proposedValue = value
		} else {
			proposedValue = ZeroValue
		}

		phase2Request.SequenceIdList = append(phase2Request.SequenceIdList,
			sequenceID)
		phase2Request.ProposedValueList = append(phase2Request.ProposedValueList,
			proposedValue)

		change.SequenceIdList = append(change.SequenceIdList, sequenceID)
		change.ChosenValueList = append(change.ChosenValueList, proposedValue)
	}

	// Some non-majority acceptors may have uncommitted votes after the largest
	// sequence id known to this leader. We must invalidate them with Zero value
	// to guarantee repeated-read property. Otherwise, this leader view is not
	// same as another leader's view which includes the non-majority acceptors.

	lastInvalidateSequenceID := largestSequenceID + int64(concurrency)
	for ii := largestSequenceID; ii < lastInvalidateSequenceID; ii++ {
		phase2Request.SequenceIdList = append(phase2Request.SequenceIdList, ii)
		phase2Request.ProposedValueList = append(phase2Request.ProposedValueList,
			ZeroValue)

		change.SequenceIdList = append(change.SequenceIdList, ii)
		change.ChosenValueList = append(change.ChosenValueList, ZeroValue)
	}

	phase2Header := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Phase2", deadline.Sub(time.Now()))
	defer this.msn.CloseMessage(phase2Header)

	phase2Message := thispb.PaxosMessage{Phase2Request: &phase2Request}
	count, errSend = msg.SendAllProto(this.msn, acceptorList, phase2Header,
		&phase2Message)
	if errSend != nil && count < majoritySize {
		this.Errorf("could not send phase2 request to majority: %v", errSend)
		return errSend
	}

	phase2Set := make(map[string]struct{})
	for ii := 0; ii < count && len(phase2Set) < majoritySize; ii++ {
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, phase2Header, &message)
		if errRecv != nil {
			this.Errorf("could not receive majority phase2 responses: %v", errRecv)
			return errRecv
		}
		acceptor := resHeader.GetMessengerId()
		if _, ok := phase2Set[acceptor]; ok {
			continue
		}
		response := message.GetPhase2Response()
		if response == nil {
			continue
		}
		if response.GetPromisedBallot() != ballot {
			continue
		}
		phase2Set[acceptor] = struct{}{}
	}

	if err := this.UpdateCoordinator(&change); err != nil {
		this.Errorf("could not update coordinator state: %v", err)
		return err
	}

	this.Infof("new leader is initialized successfully")
	return nil
}

// TimeFromUnixNano is a helper function to convert wall time in unix
// nanoseconds to time.Time format.
func TimeFromUnixNano(unixNano int64) time.Time {
	secs, nsecs := unixNano/1000000000, unixNano%1000000000
	return time.Unix(secs, nsecs)
}

///////////////////////////////////////////////////////////////////////////////

// StatusRPC implements the MultiPaxos.Status rpc.
func (this *Paxos) StatusRPC(reqHeader *msgpb.Header,
	request *thispb.StatusRequest) (status error) {

	response := thispb.StatusResponse{}
	response.TimestampNsecs = proto.Int64(time.Now().UnixNano())
	if this.IsAcceptor() {
		lock := this.ctlr.ReadLock("acceptor")
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		response.LargestVotedSequenceId = proto.Int64(this.largestVotedSequenceID)
		lock.Unlock()
	}
	if this.IsCoordinator() {
		lock := this.ctlr.ReadLock("coordinator")
		response.LeastUncommittedSequenceId = proto.Int64(
			this.leastUncommittedSequenceID)
		if this.IsLeader() {
			response.LeaderPhase1Ballot = proto.Int64(this.phase1Ballot)
			response.LeaderNextUnusedSequenceId = proto.Int64(
				this.nextUnusedSequenceID)
			response.LeaderInitializeTimestampNsecs = proto.Int64(
				this.leaderTimestampNsecs)
			response.LeaderInitializeStatus = proto.Int32(atomic.LoadInt32(
				&this.leaderInitialized))
		}
		lock.Unlock()
	}

	remote := reqHeader.GetMessengerId()
	message := thispb.PaxosMessage{StatusResponse: &response}
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send status response to %s: %v", remote, err)
		return err
	}
	return nil
}

// ProposeRPC implements the MultiPaxos.Propose rpc.
func (this *Paxos) ProposeRPC(reqHeader *msgpb.Header,
	request *thispb.ProposeRequest) (status error) {

	if !this.IsCoordinator() {
		this.Errorf("this multi-paxos object is not a coordinator")
		return errs.ErrInvalid
	}

	if !this.IsLeader() {
		this.Errorf("this multi-paxos object is not a leader")
		return errs.ErrNoLeader
	}

	if !this.IsLeaderInitialized() {
		this.Warningf("multi-paxos leader is still initializing")
		return errs.ErrRetry
	}

	// Make a copy of all configuration parameters necessary for this op.
	rlock := this.ctlr.ReadLock("config")
	acceptorList := append([]string{}, this.acceptorList...)
	concurrency := this.concurrency
	majoritySize := this.MajoritySize()
	rlock.Unlock()

	// Lock the leader only to assign sequence ids to the values. Also, ensure
	// that number of uncommitted sequence ids do not cross over the concurrency
	// limit.
	lock, errLock := this.ctlr.Lock("coordinator")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	if request.DesiredSequenceId != nil {
		sequenceID := request.GetDesiredSequenceId()
		if this.nextUnusedSequenceID != sequenceID {
			this.Errorf("proposal with desired sequence id %d is refused because "+
				" current sequence id is at %d", sequenceID, this.nextUnusedSequenceID)
			return errs.ErrStale
		}
	}

	numValues := len(request.ProposedValueList)
	numUncommitted := int(this.nextUnusedSequenceID -
		this.leastUncommittedSequenceID)
	if numUncommitted+numValues > concurrency {
		this.Errorf("too many proposals; this multi-paxos instance is only "+
			"configured for %d concurrent proposals", concurrency)
		return errs.ErrOverflow
	}

	firstSequenceID := this.nextUnusedSequenceID
	phase1Ballot := this.phase1Ballot
	this.nextUnusedSequenceID += int64(numValues)

	// Release the lock early, so that other proposals can happen in
	// parallel. They may run past this proposal, but since sequence ids are
	// assigned under the lock, they will not conflict with this proposal.
	lock.Unlock()

	// Since leader is initialized, it must already have Phase1 responses from
	// majority of the acceptors, so it just needs assign proper sequence id and
	// send Phase2 request to the acceptors.

	phase2Request := thispb.Phase2Request{}
	phase2Request.BallotNumber = proto.Int64(phase1Ballot)
	for ii, value := range request.ProposedValueList {
		phase2Request.SequenceIdList = append(phase2Request.SequenceIdList,
			firstSequenceID+int64(ii))
		phase2Request.ProposedValueList = append(phase2Request.ProposedValueList,
			value)
	}

	phase2Header := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Phase2", msg.RequestTimeout(reqHeader))
	defer this.msn.CloseMessage(phase2Header)

	phase2Message := thispb.PaxosMessage{Phase2Request: &phase2Request}
	count, errSend := msg.SendAllProto(this.msn, acceptorList, phase2Header,
		&phase2Message)
	if errSend != nil && count < majoritySize {
		this.Errorf("could not send phase2 request to majority: %v", errSend)
		return errSend
	}

	acceptorSet := make(map[string]struct{})
	for ii := 0; ii < count && len(acceptorSet) < majoritySize; ii++ {
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, phase2Header, &message)
		if errRecv != nil {
			this.Errorf("could not receive majority phase2 responses: %v", errRecv)
			return errRecv
		}
		acceptor := resHeader.GetMessengerId()
		if _, ok := acceptorSet[acceptor]; ok {
			continue
		}
		if message.Phase2Response == nil {
			continue
		}
		response := message.GetPhase2Response()
		// Phase2 rpc either votes for all proposed sequence ids or none, so we
		// don't need to check the response for per sequence id votes.
		if response.GetPromisedBallot() != phase1Ballot {
			continue
		}
		acceptorSet[acceptor] = struct{}{}
	}

	// We got majority votes for the proposal, so in the interest of lower
	// latency, send the response immediately.
	response := thispb.ProposeResponse{}
	response.SequenceIdList = phase2Request.SequenceIdList
	response.ChosenValueList = phase2Request.ProposedValueList
	message := thispb.PaxosMessage{ProposeResponse: &response}
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send propose response: %v", err)
		return err
	}

	// Update the coordinator with the results of consensus.
	change := thispb.CoordinatorChange{}
	change.SequenceIdList = phase2Request.SequenceIdList
	change.ChosenValueList = phase2Request.ProposedValueList
	if err := this.UpdateCoordinator(&change); err != nil {
		this.Errorf("could not update coordinator with propose result: %v", err)
		return err
	}
	return nil
}

// Phase1RPC implements the MultiPaxos.Phase1 rpc.
func (this *Paxos) Phase1RPC(reqHeader *msgpb.Header,
	request *thispb.Phase1Request) (status error) {

	if !this.IsAcceptor() {
		return errs.ErrInvalid
	}

	// Phase1 request is performed by the coordinator after winning the leader
	// election. He performs an infinite Phase1 with the first missing sequence
	// id and a ballot number. In response, acceptors reply their votes for all
	// known sequence ids starting with the leader's missing sequence id or
	// starting with the first un-voted sequence id, whichever is the smaller.

	lock, errLock := this.ctlr.TimedLock(msg.RequestTimeout(reqHeader),
		"acceptor")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	response := thispb.Phase1Response{}
	leader := reqHeader.GetMessengerId()
	ballot := request.GetBallotNumber()

	// Verify that rpc ballot number is higher than the already promised ballot.
	if ballot < this.promisedBallot {
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		message := thispb.PaxosMessage{Phase1Response: &response}
		errSend := msg.SendResponseProto(this.msn, reqHeader, &message)
		if errSend != nil {
			this.Errorf("could not send phase1 rejection response to %s: %v", leader,
				errSend)
			return errSend
		}
		return nil
	}

	// Pick the first sequence id whose information is requested or is not yet
	// voted.
	last := this.largestVotedSequenceID
	first := request.GetLeastUncommittedSequenceId()
	for sequenceID := first; sequenceID <= last; sequenceID++ {
		statepb := thispb.SequenceState{}
		statepb.SequenceId = proto.Int64(sequenceID)
		if votedBallot, ok := this.votedBallotMap[sequenceID]; ok {
			statepb.VotedBallot = proto.Int64(votedBallot)
			statepb.VotedValue = this.votedValueMap[sequenceID]
		}
		response.SequenceStateList = append(response.SequenceStateList, &statepb)
	}

	change := thispb.AcceptorChange{}
	change.PromisedBallot = proto.Int64(ballot)
	if err := this.doUpdateAcceptor(&change); err != nil {
		this.Errorf("could not commit acceptor promises to %s: %v", leader, err)
		return err
	}

	response.PromisedBallot = proto.Int64(ballot)
	message := thispb.PaxosMessage{Phase1Response: &response}
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send phase1 response to %s: %v", leader, err)
		return err
	}
	return nil
}

// Phase2RPC implements the MultiPaxos.Phase2 rpc.
func (this *Paxos) Phase2RPC(reqHeader *msgpb.Header,
	request *thispb.Phase2Request) error {

	if !this.IsAcceptor() {
		return errs.ErrInvalid
	}

	// Phase2 request is sent by a leader who already completed phase1
	// successfully. But, since more than one node could be thinking himself as
	// the leader, we still verify the ballot numbers before voting on any value.

	rlock := this.ctlr.ReadLock("acceptor")
	promisedBallot := this.promisedBallot
	rlock.Unlock()

	// Lock sequence ids from the request, so that multiple phase2 rpcs can be
	// processed in parallel.

	first := ""
	rest := []string{}
	for ii, sequenceID := range request.SequenceIdList {
		if ii == 0 {
			first = fmt.Sprintf("seq-%d", sequenceID)
		} else {
			rest = append(rest, fmt.Sprintf("seq-%d", sequenceID))
		}
	}

	lock, errLock := this.ctlr.TimedLock(msg.RequestTimeout(reqHeader),
		first, rest...)
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	response := thispb.Phase2Response{}
	leader := reqHeader.GetMessengerId()

	ballot := request.GetBallotNumber()
	if ballot < promisedBallot {
		response.PromisedBallot = proto.Int64(promisedBallot)
		message := thispb.PaxosMessage{Phase2Response: &response}
		errSend := msg.SendResponseProto(this.msn, reqHeader, &message)
		if errSend != nil {
			this.Errorf("could not send phase2 rejection response to %s: %v", leader,
				errSend)
			return errSend
		}
		return nil
	}

	change := thispb.AcceptorChange{}
	for index, sequenceID := range request.SequenceIdList {
		vote := thispb.AcceptorChange_Vote{}
		vote.SequenceId = proto.Int64(sequenceID)
		vote.VotedBallot = proto.Int64(ballot)
		vote.VotedValue = request.ProposedValueList[index]
		change.VoteList = append(change.VoteList, &vote)
	}

	if err := this.UpdateAcceptor(&change); err != nil {
		this.Errorf("could not commit acceptor votes: %v", err)
		return err
	}

	response.PromisedBallot = proto.Int64(promisedBallot)
	message := thispb.PaxosMessage{Phase2Response: &response}
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send phase2 response to %s: %v", leader, err)
		return err
	}
	return nil
}

// Dispatch dispatches incoming rpc requests.
func (this *Paxos) Dispatch(header *msgpb.Header, data []byte) error {
	if header.Request == nil {
		this.Errorf("incoming message %s is not a rpc request", header)
		return errs.ErrInvalid
	}
	request := header.GetRequest()
	objectID := request.GetObjectId()

	if objectID != this.uid {
		if strings.HasPrefix(objectID, this.uid+"/election") {
			return this.election.Dispatch(header, data)
		}
		this.Errorf("rpc request %s doesn't belong to this instance", header)
		return errs.ErrInvalid
	}

	message := thispb.PaxosMessage{}
	if err := proto.Unmarshal(data, &message); err != nil {
		this.Errorf("could not parse incoming message %s", header)
		return err
	}

	switch {
	case message.StatusRequest != nil:
		return this.StatusRPC(header, message.GetStatusRequest())

	case message.ProposeRequest != nil:
		return this.ProposeRPC(header, message.GetProposeRequest())

	case message.Phase1Request != nil:
		return this.Phase1RPC(header, message.GetPhase1Request())

	case message.Phase2Request != nil:
		return this.Phase2RPC(header, message.GetPhase2Request())

	default:
		this.Errorf("unknown/invalid rpc request %s", header)
		return errs.ErrInvalid
	}
}

func (this *Paxos) Propose(value []byte, timeout time.Duration) (
	int64, error) {

	// If local instance is not a proposer, find a random proposer.
	leader := this.election.CurrentLeader()

	// Send the propose request.
	request := thispb.ProposeRequest{}
	request.ProposedValueList = append(request.ProposedValueList, value)
	message := thispb.PaxosMessage{}
	message.ProposeRequest = &request
	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"MultiPaxos.Propose", timeout)
	errSend := msg.SendProto(this.msn, leader, reqHeader, &message)
	if errSend != nil {
		this.Errorf("could not send propose request to %s: %v", leader, errSend)
		return -1, errSend
	}

	// Wait for the response.
	_, errRecv := msg.ReceiveProto(this.msn, reqHeader, &message)
	if errRecv != nil {
		this.Errorf("could not receive propose response from %s: %v", leader,
			errRecv)
		return -1, errRecv
	}

	if message.ProposeResponse == nil {
		this.Errorf("propose response from %s is empty", leader)
		return -1, errs.ErrCorrupt
	}
	response := message.GetProposeResponse()
	if response.SequenceIdList == nil {
		this.Errorf("propose response from %s has no sequence id", leader)
		return -1, errs.ErrCorrupt
	}
	return response.SequenceIdList[0], nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Paxos) doGetNextHigherBallot(ballot int64) (int64, error) {
	if !this.IsCoordinator() {
		return -1, errs.ErrInvalid
	}

	nextBallot := this.phase1Ballot
	if nextBallot < 0 {
		nextBallot = int64(this.coordinatorIndex)
	}
	for nextBallot < ballot {
		nextBallot += int64(len(this.coordinatorList))
	}

	change := thispb.CoordinatorChange{}
	change.Phase1Ballot = proto.Int64(nextBallot)
	if err := this.doUpdateCoordinator(&change); err != nil {
		this.Errorf("could not update the coordinator state: %v", err)
		return -1, err
	}
	return this.phase1Ballot, nil
}

func (this *Paxos) doSaveConfig(config *thispb.Configuration) {
	config.AcceptorList = this.acceptorList
	config.CoordinatorList = this.coordinatorList
	config.Concurrency = proto.Int32(int32(this.concurrency))
	config.MajoritySize = proto.Int32(atomic.LoadInt32(&this.majoritySize))
	if this.IsAcceptor() {
		config.IsAcceptor = proto.Bool(true)
	}
	if this.IsCoordinator() {
		config.IsCoordinator = proto.Bool(true)
		config.CoordinatorIndex = proto.Int32(int32(this.coordinatorIndex))
	}
}

func (this *Paxos) doRestoreConfig(config *thispb.Configuration) {
	this.acceptorList = config.AcceptorList
	this.coordinatorList = config.CoordinatorList
	this.concurrency = int(config.GetConcurrency())
	atomic.StoreInt32(&this.majoritySize, config.GetMajoritySize())
	if config.GetIsAcceptor() {
		atomic.StoreInt32(&this.isAcceptor, 1)
	}
	if config.GetIsCoordinator() {
		atomic.StoreInt32(&this.isCoordinator, 1)
		this.coordinatorIndex = int(config.GetCoordinatorIndex())
	}
}

func (this *Paxos) doUpdateConfig(change *thispb.Configuration) error {
	this.doRestoreConfig(change)
	return nil
}

func (this *Paxos) doSaveAcceptor(state *thispb.AcceptorState) {
	state.PromisedBallot = proto.Int64(this.promisedBallot)

	for sequenceID, ballot := range this.votedBallotMap {
		state.VotedBallotMapKeys = append(state.VotedBallotMapKeys, sequenceID)
		state.VotedBallotMapValues = append(state.VotedBallotMapValues, ballot)
	}

	for sequenceID, value := range this.votedValueMap {
		state.VotedValueMapKeys = append(state.VotedValueMapKeys, sequenceID)
		state.VotedValueMapValues = append(state.VotedValueMapValues, value)
	}
}

func (this *Paxos) doRestoreAcceptor(state *thispb.AcceptorState) {
	this.promisedBallot = state.GetPromisedBallot()

	maxSequenceID := int64(-1)
	for index := range state.VotedBallotMapKeys {
		key := state.VotedBallotMapKeys[index]
		value := state.VotedBallotMapValues[index]
		this.votedBallotMap[key] = value

		if key > maxSequenceID {
			maxSequenceID = key
		}
	}
	this.largestVotedSequenceID = maxSequenceID

	for index := range state.VotedValueMapKeys {
		key := state.VotedValueMapKeys[index]
		value := state.VotedValueMapValues[index]
		this.votedValueMap[key] = value
	}
}

func (this *Paxos) doUpdateAcceptor(change *thispb.AcceptorChange) error {
	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.AcceptorChange = change
		_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
		if errSync != nil {
			this.Errorf("could not write acceptor change record: %v", errSync)
			return errSync
		}
	}

	if change.PromisedBallot != nil {
		this.promisedBallot = change.GetPromisedBallot()
	}

	for _, vote := range change.VoteList {
		sequenceID := vote.GetSequenceId()
		ballot := vote.GetVotedBallot()
		value := vote.GetVotedValue()

		this.votedBallotMap[sequenceID] = ballot
		this.votedValueMap[sequenceID] = value
		if sequenceID > this.largestVotedSequenceID {
			this.largestVotedSequenceID = sequenceID
		}
	}
	return nil
}

func (this *Paxos) UpdateAcceptor(change *thispb.AcceptorChange) error {
	lock, errLock := this.ctlr.Lock("acceptor")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	return this.doUpdateAcceptor(change)
}

func (this *Paxos) doSaveCoordinator(state *thispb.CoordinatorState) {
	state.Phase1Ballot = proto.Int64(this.phase1Ballot)
	state.LeastUncommittedSequenceId = proto.Int64(
		this.leastUncommittedSequenceID)

	for sequenceID, value := range this.chosenMap {
		state.ChosenMapKeys = append(state.ChosenMapKeys, sequenceID)
		state.ChosenMapValues = append(state.ChosenMapValues, value)
	}
}

func (this *Paxos) doRestoreCoordinator(state *thispb.CoordinatorState) {
	this.phase1Ballot = state.GetPhase1Ballot()
	this.leastUncommittedSequenceID = state.GetLeastUncommittedSequenceId()

	maxSequenceID := this.leastUncommittedSequenceID - 1
	for index := range state.ChosenMapKeys {
		key := state.ChosenMapKeys[index]
		value := state.ChosenMapValues[index]
		this.chosenMap[key] = value

		if key > maxSequenceID {
			maxSequenceID = key
		}
	}
	this.nextUnusedSequenceID = maxSequenceID + 1

	// If chosen map is empty, then next unused sequence id must be equal to the
	// last uncommitted sequence id.
	if this.nextUnusedSequenceID >= this.leastUncommittedSequenceID {
		this.Fatal("multi-paxos bug or data is corrupt: chosenMap: %v, "+
			"nextUnusedSequenceID: %d, leastUncommittedSequenceID: %d",
			this.chosenMap, this.nextUnusedSequenceID,
			this.leastUncommittedSequenceID)
	}
}

func (this *Paxos) doUpdateCoordinator(
	change *thispb.CoordinatorChange) error {

	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.CoordinatorChange = change
		_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
		if errSync != nil {
			this.Errorf("could not write learner change record: %v", errSync)
			return errSync
		}
	}

	if change.Phase1Ballot != nil {
		this.phase1Ballot = change.GetPhase1Ballot()
	}

	for index, sequenceID := range change.SequenceIdList {
		chosen := change.ChosenValueList[index]
		this.chosenMap[sequenceID] = chosen
	}

	index := this.leastUncommittedSequenceID
	for _, ok := this.chosenMap[index]; ok; _, ok = this.chosenMap[index] {
		index++
	}
	this.leastUncommittedSequenceID = index
	return nil
}

func (this *Paxos) UpdateCoordinator(change *thispb.CoordinatorChange) error {
	lock, errLock := this.ctlr.Lock("coordinator")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	return this.doUpdateCoordinator(change)
}
