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
// This file implements Election type, which implements fault-tolerant leader
// election algorithm using classic paxos instances.
//
// THREAD SAFETY
//
// All public functions are thread-safe.
//
// NOTES
//
// An election consists of a fixed set of nodes forming a committee and
// an unlimited number of clients.
//
// Election happens in monotonic rounds and can be started by any client. Every
// client proposes himself and one of the client is chosen as the leader. A
// leader can be chosen for round X, only after a leader was chosen in round
// X-1. So, clients must first learn current election round before attempting
// to start a new round.
//
// Committee members decide who wins the election by (classic paxos) majority
// vote. So, election fault tolerance is determined by the size of the
// committee.
//
// Elected leader is responsible to inform all clients about his leadership.
// Also, clients may can ask the committee for the election result at any time.
//

package election

import (
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"
	"go-ascent/base/log"
	"go-ascent/msg"
	"go-ascent/paxos/classic"
	"go-ascent/thread/ctlr"
	"go-ascent/wal"

	thispb "proto-ascent/algo/election"
	msgpb "proto-ascent/msg"
)

// Watcher defines the callback interface on committee members to notify
// leadership changes.
type Watcher interface {
	// ElectionUpdate callback is invoked with election status whenever
	// new election is performed.
	ElectionUpdate(round int64, leader string)
}

// Options defines user configurable options for election.
type Options struct {
	// Maximum number of old election rounds to keep as a history.
	MaxElectionHistory int

	// Options for classic paxos consensus.
	PaxosOptions classic.Options
}

// Validate checks user configuration options for invalid settings.
func (this *Options) Validate() (status error) {
	if this.MaxElectionHistory < 0 {
		err := errs.NewErrInvalid("election round history size cannot be -ve")
		status = errs.MergeErrors(status, err)
	}
	if err := this.PaxosOptions.Validate(); err != nil {
		status = errs.MergeErrors(status, err)
	}
	return status
}

// Election type implements classic paxos based leader election.
type Election struct {
	log.Logger

	// Controller for admission control and synchronization.
	ctlr ctlr.BasicController

	// Messenger to send/receives messages to/from peers.
	msn msg.Messenger

	// Write ahead log to save election state.
	wal wal.WriteAheadLog

	//
	// Election configuration
	//

	// User configuration options.
	opts Options

	// Globally unique id for the election instance. All nodes participating in
	// election share the same uid.
	uid string

	// Network RPC namespace for this instance.
	namespace string

	// Atomic variable indicating the role for this object.
	inCommittee int32

	// Majority size for the committee, which is always, len(committee)/2+1.
	majoritySize int32

	// List of nodes that form the committee for the election.
	committee []string

	//
	// Dynamic election state.
	//

	// Current election round and the winner. This may not always be up to date
	// due to refresh intervals and network latency, etc.
	currentWinner string
	currentRound  int64

	//
	// State for committee members.
	//

	// A watcher to notify election updates.
	watch Watcher

	// Classic paxos instances for the election rounds.
	classicPaxosMap map[int64]*classic.Paxos

	// Map of last few recent election results.
	electionHistoryMap map[int64]string
}

// ElectionRPCList returns list of rpcs handled by election objects.
func ElectionRPCList() []string {
	paxosRPCList := classic.PaxosRPCList()
	rpcList := []string{"Election.Status", "Election.Elect"}
	return append(rpcList, paxosRPCList...)
}

// Initialize initializes an election object.
func (this *Election) Initialize(opts *Options, namespace, uid string,
	msn msg.Messenger, wal wal.WriteAheadLog) (status error) {

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

	this.uid = uid
	this.namespace = namespace
	this.opts = *opts
	this.msn = msn
	this.wal = wal
	this.majoritySize = -1
	this.currentRound = -1
	this.classicPaxosMap = make(map[int64]*classic.Paxos)
	this.electionHistoryMap = make(map[int64]string)

	this.Logger = this.NewLogger("election-%s-%s", this.msn.UID(), this.uid)
	this.ctlr.Initialize(this)
	return nil
}

// Close releases all resources and destroys the object.
func (this *Election) Close() (status error) {
	if err := this.ctlr.Close(); err != nil {
		return err
	}

	re := regexp.MustCompile(this.uid)
	if err := this.wal.ConfigureRecoverer(re, nil); err != nil {
		this.Errorf("could not unconfigure wal recoverer: %v", err)
		status = errs.MergeErrors(status, err)
	}

	for round, paxos := range this.classicPaxosMap {
		if err := paxos.Close(); err != nil {
			this.Errorf("could not close paxos instance for round %d: %v", round,
				err)
			status = errs.MergeErrors(status, err)
		}
	}
	return status
}

// Configure initializes the election instance with the committee members.
func (this *Election) Configure(agents []string) error {
	lock, errLock := this.ctlr.LockAll()
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	if this.MajoritySize() > 0 {
		this.Errorf("election instance is already configured")
		return errs.ErrInvalid
	}

	// TODO: Check for duplicates.

	committee := make([]string, len(agents))
	copy(committee, agents)
	sort.Sort(sort.StringSlice(committee))
	majoritySize := len(committee)/2 + 1

	self := this.msn.UID()

	inCommittee := false
	for _, member := range committee {
		if member == self {
			inCommittee = true
			break
		}
	}

	// Save configuration in the wal.
	config := thispb.Configuration{}
	config.CommitteeList = committee
	config.MajoritySize = proto.Int32(int32(majoritySize))
	config.InCommittee = proto.Bool(inCommittee)
	return this.doUpdateConfig(&config)
}

// RecoverCheckpoint restores last known election state from a checkpoint
// record.
func (this *Election) RecoverCheckpoint(uid string, data []byte) error {
	if uid != this.uid {
		// Check if uid belongs to one of our paxos instances.
		if this.IsPaxosUID(uid) {
			paxos, errGet := this.doGetPaxosInstance(uid)
			if errGet != nil {
				this.Errorf("could not find paxos instance for %s: %v", uid, errGet)
				return errGet
			}
			return paxos.RecoverCheckpoint(uid, data)
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

	this.doRestoreConfig(checkpoint.GetConfiguration())

	if this.InCommittee() {
		if checkpoint.CommitteeState == nil {
			this.Errorf("committee member checkpoint has no election state")
			return errs.ErrCorrupt
		}
		this.doRestoreCommittee(checkpoint.GetCommitteeState())
	}

	this.doRestoreElection(checkpoint.GetElectionState())
	return nil
}

// RecoverChange updates election state from a change record.
func (this *Election) RecoverChange(lsn wal.LSN, uid string,
	data []byte) error {

	if uid != this.uid {
		// Check if uid belongs to one of our paxos instances.
		if this.IsPaxosUID(uid) {
			paxos, errGet := this.doGetPaxosInstance(uid)
			if errGet != nil {
				this.Errorf("could not find paxos instance for %s: %v", uid, errGet)
				return errGet
			}
			return paxos.RecoverChange(lsn, uid, data)
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
		this.doRestoreConfig(walRecord.GetConfigChange())
		return nil

	case walRecord.CommitteeChange != nil:
		if !this.InCommittee() {
			this.Errorf("found committee change when instance is not a member")
			return errs.ErrCorrupt
		}
		return this.doUpdateCommittee(walRecord.GetCommitteeChange())

	case walRecord.ElectionChange != nil:
		return this.doUpdateElection(walRecord.GetElectionChange())

	default:
		this.Errorf("invalid/unknown change record %s", walRecord)
		return errs.ErrInvalid
	}
	return nil
}

// TakeCheckpoint logs a checkpoint record to the wal.
func (this *Election) TakeCheckpoint() error {
	lock := this.ctlr.ReadLockAll()
	defer lock.Unlock()

	if this.MajoritySize() <= 0 {
		this.Errorf("election instance is not yet configured")
		return errs.ErrInvalid
	}

	checkpoint := thispb.Checkpoint{}

	config := thispb.Configuration{}
	this.doSaveConfig(&config)
	checkpoint.Configuration = &config

	if this.InCommittee() {
		state := thispb.CommitteeState{}
		this.doSaveCommittee(&state)
		checkpoint.CommitteeState = &state
	}

	state := thispb.ElectionState{}
	this.doSaveElection(&state)
	checkpoint.ElectionState = &state

	walRecord := thispb.WALRecord{}
	walRecord.Checkpoint = &checkpoint
	errAppend := wal.AppendCheckpointProto(this.wal, this.uid, &walRecord)
	if errAppend != nil {
		this.Errorf("could not append checkpoint record: %v", errAppend)
		return errAppend
	}

	// Checkpoint recent paxos instances' state.
	if this.InCommittee() {
		state := checkpoint.CommitteeState
		for _, round := range state.ElectionRoundList {
			if paxos, ok := this.classicPaxosMap[round]; ok {
				if err := paxos.TakeCheckpoint(); err != nil {
					this.Errorf("could not take checkpoint of paxos instance for "+
						"round %d: %v", err)
					return err
				}
			}
		}
	}
	return nil
}

// InCommittee returns true if this object is a member of the election
// committee.
func (this *Election) InCommittee() bool {
	return atomic.LoadInt32(&this.inCommittee) != 0
}

// MajoritySize returns the quorum size for the committee.
func (this *Election) MajoritySize() int {
	return int(atomic.LoadInt32(&this.majoritySize))
}

// CurrentRound returns the latest election round.
func (this *Election) CurrentRound() int64 {
	return atomic.LoadInt64(&this.currentRound)
}

// CurrentLeader returns the leader for the latest round known.
func (this *Election) CurrentLeader() string {
	lock := this.ctlr.ReadLock("election")
	defer lock.Unlock()

	return this.currentWinner
}

// SetCommitteeWatch configures a watch on that gets leader election change
// updates.
func (this *Election) SetCommitteeWatch(watch Watcher) error {
	if !this.InCommittee() {
		return errs.ErrInvalid
	}

	lock, errLock := this.ctlr.Lock("committee")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	this.watch = watch
	return nil
}

// ConsensusUpdate implements paxos.Watcher interface.
func (this *Election) ConsensusUpdate(uid string, index int64, value []byte) {
	round := this.ElectionRoundFromPaxosUID(uid)
	if round <= this.CurrentRound() {
		return
	}
	winner := string(value)

	lock, errLock := this.ctlr.LockAll()
	if errLock != nil {
		return
	}
	defer lock.Unlock()

	change := thispb.ElectionChange{}
	change.ElectionRound = proto.Int64(round)
	change.ElectionWinner = proto.String(winner)
	if err := this.doUpdateElection(&change); err != nil {
		this.Fatalf("could not update new election round status: %v", err)
	}

	if this.InCommittee() {
		change := thispb.CommitteeChange{}
		change.NewElectionRound = proto.Int64(round)
		change.NewElectionWinner = proto.String(winner)
		if err := this.doUpdateCommittee(&change); err != nil {
			this.Fatalf("could not update committee with new election status: %v",
				err)
		}
	}
}

// ElectLeader performs a leader election for the next round.
func (this *Election) ElectLeader(timeout time.Duration) (
	string, int64, error) {

	lock := this.ctlr.ReadLockAll()
	target := this.msn.UID()
	if !this.InCommittee() {
		target = this.committee[rand.Intn(len(this.committee))]
	}
	lock.Unlock()

	request := thispb.ElectRequest{}
	request.ElectionRound = proto.Int64(this.CurrentRound() + 1)
	message := thispb.ElectionMessage{}
	message.ElectRequest = &request

	reqHeader := this.msn.NewRequest(this.namespace, this.uid, "Election.Elect",
		timeout)
	defer this.msn.CloseMessage(reqHeader)

	if err := msg.SendProto(this.msn, target, reqHeader, &message); err != nil {
		this.Errorf("could not send elect request to %s: %v", target, err)
		return "", -1, err
	}

	message.Reset()
	_, errRecv := msg.ReceiveProto(this.msn, reqHeader, &message)
	if errRecv != nil {
		this.Errorf("could not receive elect response from %s: %v", target,
			errRecv)
		return "", -1, errRecv
	}

	if message.ElectResponse == nil {
		this.Errorf("elect response from %s is empty", target)
		return "", -1, errs.ErrCorrupt
	}
	response := message.GetElectResponse()

	change := thispb.ElectionChange{}
	change.ElectionRound = response.ElectionRound
	change.ElectionWinner = response.ElectionWinner
	if err := this.UpdateElection(&change); err != nil {
		this.Errorf("could not update to new election round: %v", err)
		return "", -1, err
	}

	newRound := response.GetElectionRound()
	newLeader := response.GetElectionWinner()
	return newLeader, newRound, nil
}

// RefreshLeader queries committee members for the latest election information.
func (this *Election) RefreshLeader(timeout time.Duration) (
	string, int64, error) {

	lock := this.ctlr.ReadLock("config")
	committee := append([]string{}, this.committee...)
	lock.Unlock()

	request := thispb.StatusRequest{}
	message := thispb.ElectionMessage{}
	message.StatusRequest = &request

	reqHeader := this.msn.NewRequest(this.namespace, this.uid, "Election.Status",
		timeout)
	defer this.msn.CloseMessage(reqHeader)

	count, errSend := msg.SendAllProto(this.msn, committee, reqHeader, &message)
	if errSend != nil && count < this.MajoritySize() {
		this.Errorf("could not send status request to committee: %v", errSend)
		return "", -1, errSend
	}

	// Wait for majority number of status responses and pick the winner for the
	// largest election round. It may also happen that no election round exists
	// because this instance is just created.

	maxElectionRound := int64(-1)
	maxElectionWinner := ""
	remoteSet := make(map[string]struct{})

	for ii := 0; ii < count && len(remoteSet) < this.MajoritySize(); ii++ {
		message := thispb.ElectionMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, reqHeader, &message)
		if errRecv != nil {
			this.Warningf("could not receive any more status responses for %s: %v",
				reqHeader, errRecv)
			break
		}

		memberID := resHeader.GetMessengerId()
		if _, ok := remoteSet[memberID]; ok {
			this.Warningf("duplicate status response from %s (ignored)", memberID)
			continue
		}

		// TODO Check the response header for errors.

		if message.StatusResponse == nil {
			this.Warningf("status response data is empty from %s (ignored)",
				memberID)
			continue
		}

		response := message.GetStatusResponse()
		if response.ElectionRound != nil {
			round := response.GetElectionRound()
			if round > maxElectionRound {
				maxElectionRound = round
				maxElectionWinner = response.GetElectionWinner()
			}
		}
		remoteSet[memberID] = struct{}{}
	}

	if len(remoteSet) < this.MajoritySize() {
		this.Errorf("could not get majority responses %v in finding leader",
			remoteSet)
		return "", -1, errs.ErrRetry
	}

	if maxElectionRound > this.CurrentRound() {
		change := thispb.ElectionChange{}
		change.ElectionRound = proto.Int64(maxElectionRound)
		change.ElectionWinner = proto.String(maxElectionWinner)
		if err := this.UpdateElection(&change); err != nil {
			this.Errorf("could not update election status: %v", err)
			return maxElectionWinner, maxElectionRound, err
		}
	}

	this.Infof("found %s as the last known leader in round %d",
		maxElectionWinner, maxElectionRound)
	return maxElectionWinner, maxElectionRound, nil
}

// StatusRPC implements the Election.Status rpc.
func (this *Election) StatusRPC(reqHeader *msgpb.Header,
	request *thispb.StatusRequest) error {

	if !this.InCommittee() {
		this.Errorf("this election object is not a committee member")
		return errs.ErrNotExist
	}

	lock := this.ctlr.ReadLock("election")
	round := this.CurrentRound()
	winner := this.currentWinner
	lock.Unlock()

	// Committee members always have up to date election information.
	response := thispb.StatusResponse{}
	response.ElectionRound = proto.Int64(round)
	response.ElectionWinner = proto.String(winner)
	message := thispb.ElectionMessage{}
	message.StatusResponse = &response

	clientID := reqHeader.GetMessengerId()
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send status response to %s: %v", clientID, err)
		return err
	}
	return nil
}

// ElectRPC implements the Election.Elect rpc.
func (this *Election) ElectRPC(reqHeader *msgpb.Header,
	request *thispb.ElectRequest) (status error) {

	if !this.InCommittee() {
		return errs.ErrInvalid
	}

	round := request.GetElectionRound()
	current := this.CurrentRound()
	if round != current+1 {
		this.Errorf("could not begin election round %d because current round "+
			"is only at %d", round, current)
		return errs.ErrStale
	}

	uid := this.PaxosUIDFromElectionRound(round)
	paxos, errGet := this.GetPaxosInstance(uid)
	if errGet != nil {
		this.Errorf("could not find paxos instance for uid %s: %v", uid, errGet)
		return errGet
	}

	this.Infof("starting new election for round %d using paxos instance %s",
		round, uid)

	clientID := reqHeader.GetMessengerId()
	proposedValue := []byte(clientID)
	chosenValue, errPropose := paxos.Propose(proposedValue,
		msg.RequestTimeout(reqHeader))
	if errPropose != nil {
		this.Errorf("could not propose %s as the leader for election round %d: %v",
			proposedValue, round, errPropose)
		return errPropose
	}
	winner := string(chosenValue)

	// Send the election result.
	response := thispb.ElectResponse{}
	response.ElectionRound = proto.Int64(round)
	response.ElectionWinner = proto.String(winner)
	message := thispb.ElectionMessage{}
	message.ElectResponse = &response
	if err := msg.SendResponseProto(this.msn, reqHeader, &message); err != nil {
		this.Errorf("could not send election response to %s: %v", clientID, err)
		return err
	}
	return nil
}

// DispatchPaxos handles incoming rpc requests for the paxos instances.
func (this *Election) DispatchPaxos(header *msgpb.Header, data []byte) error {
	request := header.GetRequest()
	objectID := request.GetObjectId()

	instance, errGet := this.GetPaxosInstance(objectID)
	if errGet != nil {
		this.Errorf("could not get paxos instance with object id %s", objectID)
		return errGet
	}

	return instance.Dispatch(header, data)
}

// Dispatch handles all incoming rpc requests for this object.
func (this *Election) Dispatch(header *msgpb.Header, data []byte) error {
	request := header.GetRequest()
	objectID := request.GetObjectId()

	// Dispatch classic paxos rpcs to the paxos instances.
	if objectID != this.uid {
		if this.IsPaxosUID(objectID) {
			return this.DispatchPaxos(header, data)
		}
		this.Errorf("rpc request %s doesn't belong to this instance", header)
		return errs.ErrInvalid
	}

	message := thispb.ElectionMessage{}
	if err := proto.Unmarshal(data, &message); err != nil {
		this.Errorf("could not parse incoming message %s", header)
		return err
	}

	switch {
	case message.ElectRequest != nil:
		return this.ElectRPC(header, message.GetElectRequest())

	case message.StatusRequest != nil:
		return this.StatusRPC(header, message.GetStatusRequest())

	default:
		this.Errorf("unknown/invalid rpc reqest %s", header)
		return errs.ErrInvalid
	}
}

// GetPaxosInstance creates a new paxos object representing an election round.
func (this *Election) GetPaxosInstance(uid string) (*classic.Paxos, error) {
	lock, errLock := this.ctlr.Lock("committee")
	if errLock != nil {
		return nil, errLock
	}
	defer lock.Unlock()

	return this.doGetPaxosInstance(uid)
}

///////////////////////////////////////////////////////////////////////////////

func (this *Election) IsPaxosUID(uid string) bool {
	return this.ElectionRoundFromPaxosUID(uid) >= 0
}

func (this *Election) ElectionRoundFromPaxosUID(uid string) int64 {
	round := int64(-1)
	fmt.Sscanf(uid, this.uid+"/round-%d", &round)
	return round
}

func (this *Election) PaxosUIDFromElectionRound(round int64) string {
	return this.uid + fmt.Sprintf("/round-%d", round)
}

func (this *Election) doGetPaxosInstance(uid string) (
	instance *classic.Paxos, status error) {

	round := this.ElectionRoundFromPaxosUID(uid)
	if round < 0 {
		this.Errorf("could not parse election round from paxos uid %s", uid)
		return nil, errs.ErrCorrupt
	}

	if instance, ok := this.classicPaxosMap[round]; ok {
		return instance, nil
	}

	paxos := &classic.Paxos{Logger: this}
	errInit := paxos.Initialize(&this.opts.PaxosOptions, this.namespace, uid,
		this.msn, this.wal)
	if errInit != nil {
		this.Errorf("could not initialize paxos instance %s: %v", uid, errInit)
		return nil, errInit
	}

	errConfig := paxos.Configure(this.committee, this.committee, this.committee)
	if errConfig != nil {
		this.Errorf("could not configure paxos instance %s: %v", uid, errConfig)
		return nil, errConfig
	}

	defer func() {
		if status != nil {
			if err := paxos.Close(); err != nil {
				this.Errorf("could not close paxos instance %s: %v", uid, err)
				status = errs.MergeErrors(status, err)
			}
		}
	}()

	if err := paxos.SetLearnerWatch(this); err != nil {
		this.Errorf("could not configure watch on the paxos instance %s: %v", uid,
			err)
		return nil, err
	}

	this.classicPaxosMap[round] = paxos
	return paxos, nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Election) doUpdateConfig(change *thispb.Configuration) error {
	walRecord := thispb.WALRecord{}
	walRecord.ConfigChange = change
	_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
	if errSync != nil {
		this.Errorf("could not append config change wal record: %v", errSync)
		return errSync
	}

	this.doRestoreConfig(change)
	return nil
}

func (this *Election) UpdateElection(change *thispb.ElectionChange) error {
	lock, errLock := this.ctlr.Lock("election")
	if errLock != nil {
		return errLock
	}
	defer lock.Unlock()

	return this.doUpdateElection(change)
}

func (this *Election) doUpdateElection(change *thispb.ElectionChange) error {
	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.ElectionChange = change
		_, errQueue := wal.QueueChangeProto(this.wal, this.uid, &walRecord)
		if errQueue != nil {
			this.Errorf("could not write election change wal record: %v", errQueue)
			return errQueue
		}
	}

	atomic.StoreInt64(&this.currentRound, change.GetElectionRound())
	this.currentWinner = change.GetElectionWinner()
	return nil
}

func (this *Election) doUpdateCommittee(change *thispb.CommitteeChange) error {
	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.CommitteeChange = change
		_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
		if errSync != nil {
			this.Errorf("could not write committee change wal record: %v", errSync)
			return errSync
		}
	}

	round := change.GetNewElectionRound()
	winner := change.GetNewElectionWinner()
	this.electionHistoryMap[round] = winner
	return nil
}

func (this *Election) doSaveConfig(config *thispb.Configuration) {
	config.CommitteeList = append([]string{}, this.committee...)
	config.MajoritySize = proto.Int32(int32(this.MajoritySize()))
	if this.InCommittee() {
		config.InCommittee = proto.Bool(true)
	}
}

func (this *Election) doSaveCommittee(state *thispb.CommitteeState) {
	oldestRound := this.CurrentRound() - int64(this.opts.MaxElectionHistory)
	for round, winner := range this.electionHistoryMap {
		if round > oldestRound {
			state.ElectionRoundList = append(state.ElectionRoundList, round)
			state.ElectionWinnerList = append(state.ElectionWinnerList, winner)
		}
	}
}

func (this *Election) doSaveElection(state *thispb.ElectionState) {
	state.CurrentRound = proto.Int64(this.CurrentRound())
	state.CurrentWinner = proto.String(this.currentWinner)
}

func (this *Election) doRestoreConfig(config *thispb.Configuration) {
	this.committee = config.GetCommitteeList()
	atomic.StoreInt32(&this.majoritySize, int32(config.GetMajoritySize()))
	if config.GetInCommittee() {
		atomic.StoreInt32(&this.inCommittee, 1)
	}
}

func (this *Election) doRestoreCommittee(state *thispb.CommitteeState) {
	for index := range state.ElectionRoundList {
		round := state.ElectionRoundList[index]
		winner := state.ElectionWinnerList[index]
		this.electionHistoryMap[round] = winner
		if round > this.CurrentRound() {
			atomic.StoreInt64(&this.currentRound, round)
			this.currentWinner = winner
		}
	}
}

func (this *Election) doRestoreElection(state *thispb.ElectionState) {
	atomic.StoreInt64(&this.currentRound, state.GetCurrentRound())
	this.currentWinner = state.GetCurrentWinner()
}
