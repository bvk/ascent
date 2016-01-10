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
// This file defines Paxos type which implements Classic Paxos functionality.
//
// THREAD SAFETY
//
// All public functions are thread-safe.
//

package classic

import (
	"bytes"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"
	"go-ascent/base/log"
	"go-ascent/msg"
	"go-ascent/thread/ctlr"
	"go-ascent/wal"

	msgpb "proto-ascent/msg"
	thispb "proto-ascent/paxos/classic"
)

// Options defines user configurable items.
type Options struct {
	// Number of attempts to retry on propose failures.
	MaxProposeRetries int

	// Time to wait before retrying a failed proposal with a higher ballot.
	ProposeRetryInterval time.Duration

	// Time to wait for Phase1 responses from acceptors.
	Phase1Timeout time.Duration

	// Number of extra acceptors to request for phase1 promises.
	NumExtraPhase1Acceptors int

	// Time to wait for Phase2 responses from acceptors.
	Phase2Timeout time.Duration
}

// Validate checks if user configuration items are all valid.
func (this *Options) Validate() (status error) {
	if this.MaxProposeRetries < 0 {
		err := errs.NewErrInvalid("propose retry attempts cannot be negative")
		status = errs.MergeErrors(status, err)
	}
	if this.ProposeRetryInterval < time.Millisecond {
		err := errs.NewErrInvalid("propose retry should wait for at least a " +
			"millisecond")
		status = errs.MergeErrors(status, err)
	}
	if this.Phase1Timeout < time.Millisecond {
		err := errs.NewErrInvalid("phase1 timeout must be at least a millisecond")
		status = errs.MergeErrors(status, err)
	}
	if this.NumExtraPhase1Acceptors < 0 {
		err := errs.NewErrInvalid("number of extra phase1 acceptors cannot be -ve")
		status = errs.MergeErrors(status, err)
	}
	if this.Phase2Timeout < time.Millisecond {
		err := errs.NewErrInvalid("phase2 timeout must be at least a millisecond")
		status = errs.MergeErrors(status, err)
	}
	return status
}

// Paxos implements the classic paxos algorithm.
type Paxos struct {
	log.Logger

	// User configurable options.
	opts Options

	// Messenger to send/receive messages.
	msn msg.Messenger

	// Write ahead log to make state changes durable.
	wal wal.WriteAheadLog

	// Controller for synchronization.
	ctlr ctlr.BasicController

	//
	// Paxos Configuration.
	//

	// Globally unique id for the consensus instance. All nodes participating in
	// a consensus share the same uid.
	uid string

	// Network RPC namespace for this instance.
	namespace string

	// Atomic variables indicating the classic paxos roles for this object.
	isLearner  int32
	isAcceptor int32
	isProposer int32

	// All nodes participating in the consensus as per their roles.
	learnerList  []string
	acceptorList []string
	proposerList []string

	// If this instance is a proposer, its index to partition the ballot
	// space. Ballot numbers are computed as follows: the uint64 space is
	// partitioned into len(proposerList) groups, where proposer i owns all
	// uint64s such that x % len(proposerList) == i.
	proposerIndex int

	// Majority size for the consensus. This is same as len(acceptorList)/2+1.
	majoritySize int

	//
	// Acceptor state.
	//
	promisedBallot int64
	votedBallot    int64
	votedValue     []byte

	//
	// Proposer state.
	//
	proposalBallot int64

	//
	// Learner state.
	//
	chosenValue        []byte
	ballotValueMap     map[int64][]byte
	ballotAcceptorsMap map[int64]map[string]struct{}
}

// PaxosRPCList returns list of rpcs handled by classic paxos objects.
func PaxosRPCList() []string {
	return []string{
		"ClassicPaxos.Propose", "ClassicPaxos.Phase1", "ClassicPaxos.Phase2",
		"ClassicPaxos.Learn",
	}
}

// Initialize initializes a classic paxos instance.
func (this *Paxos) Initialize(opts *Options, namespace, uid string,
	msn msg.Messenger, wal wal.WriteAheadLog) (status error) {

	if err := opts.Validate(); err != nil {
		this.Errorf("invalid user options: %v", err)
		return err
	}

	if err := wal.ConfigureRecoverer(this.uid, this); err != nil {
		this.Errorf("could not configure wal recoverer: %v", err)
		return err
	}
	defer func() {
		if status != nil {
			if err := wal.ConfigureRecoverer(this.uid, nil); err != nil {
				this.Errorf("could not unconfigure wal recoverer: %v", err)
				status = errs.MergeErrors(status, err)
			}
		}
	}()

	this.wal = wal
	this.opts = *opts
	this.msn = msn
	this.uid = uid
	this.namespace = namespace
	this.proposerIndex = -1
	this.promisedBallot = -1
	this.votedBallot = -1
	this.majoritySize = -1
	this.proposalBallot = -1
	this.ballotValueMap = make(map[int64][]byte)
	this.ballotAcceptorsMap = make(map[int64]map[string]struct{})

	this.Logger = this.NewLogger("classic-paxos:%s-%s", uid, this.msn.UID())
	this.ctlr.Initialize(this)
	return nil
}

// Close releases all resources and destroys the object.
func (this *Paxos) Close() (status error) {
	if err := this.ctlr.Close(); err != nil {
		return err
	}

	if err := this.wal.ConfigureRecoverer(this.uid, nil); err != nil {
		this.Errorf("could not unconfigure wal recoverer: %v", err)
		status = errs.MergeErrors(status, err)
	}

	return status
}

// Configure initializes the paxos agent configuration. Since classic paxos
// doesn't support reconfiguration, this operation can be performed only
// once.
func (this *Paxos) Configure(proposerList, acceptorList,
	learnerList []string) error {

	token, errToken := this.ctlr.NewToken("ConfigureAgents", nil /* timeout */)
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if len(this.proposerList) > 0 {
		this.Errorf("paxos instance is already configured")
		return errs.ErrExist
	}

	// TODO: Check for duplicates.

	learners := make([]string, len(learnerList))
	copy(learners, learnerList)
	sort.Sort(sort.StringSlice(learners))

	acceptors := make([]string, len(acceptorList))
	copy(acceptors, acceptorList)
	sort.Sort(sort.StringSlice(acceptors))
	majoritySize := len(acceptors)/2 + 1

	proposers := make([]string, len(proposerList))
	copy(proposers, proposerList)
	sort.Sort(sort.StringSlice(proposers))

	self := this.msn.UID()

	isLearner := false
	for _, learner := range learners {
		if learner == self {
			isLearner = true
			break
		}
	}

	isAcceptor := false
	for _, acceptor := range acceptors {
		if acceptor == self {
			isAcceptor = true
		}
	}

	isProposer := false
	proposerIndex := -1
	for ii, proposer := range proposers {
		if proposer == self {
			isProposer = true
			proposerIndex = ii
			break
		}
	}

	// Save configuration in the wal.
	config := thispb.Configuration{}
	config.InstanceId = proto.String(this.uid)
	config.LearnerList = learners
	config.ProposerList = proposers
	config.AcceptorList = acceptors
	config.MajoritySize = proto.Int32(int32(majoritySize))
	if isLearner {
		config.IsLearner = proto.Bool(true)
	}
	if isAcceptor {
		config.IsAcceptor = proto.Bool(true)
	}
	if isProposer {
		config.IsProposer = proto.Bool(true)
		config.ProposerIndex = proto.Int32(int32(proposerIndex))
	}

	walRecord := thispb.WALRecord{}
	walRecord.ConfigChange = &config
	data, errMarshal := proto.Marshal(&walRecord)
	if errMarshal != nil {
		this.Errorf("could not marshal wal record: %v", errMarshal)
		return errMarshal
	}

	if _, err := this.wal.SyncChangeRecord(this.uid, data); err != nil {
		this.Errorf("could not append wal record: %v", err)
		return err
	}

	if err := this.restoreConfig(&config); err != nil {
		this.Fatalf("could not configure the instance from wal record: %v", err)
	}
	return nil
}

// RecoverCheckpoint recovers state from a checkpoint record.
func (this *Paxos) RecoverCheckpoint(uid string, data []byte) error {
	if uid != this.uid {
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
		return errs.ErrInvalid
	}
	checkpoint := walRecord.GetCheckpoint()

	if err := this.restoreConfig(checkpoint.GetConfiguration()); err != nil {
		this.Errorf("could not restore paxos instance configuration: %v", err)
		return err
	}

	if checkpoint.ProposalBallot != nil {
		this.proposalBallot = checkpoint.GetProposalBallot()
	}

	if checkpoint.PromisedBallot != nil {
		this.promisedBallot = checkpoint.GetPromisedBallot()
	}

	if checkpoint.VotedBallot != nil {
		this.votedBallot = checkpoint.GetVotedBallot()
		this.votedValue = checkpoint.GetVotedValue()
	}

	if checkpoint.ChosenValue != nil {
		this.chosenValue = checkpoint.GetChosenValue()
	}
	return nil
}

// RecoverChange recovers an update from a change record.
func (this *Paxos) RecoverChange(lsn wal.LSN, uid string, data []byte) error {
	if uid != this.uid {
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
		return this.restoreConfig(walRecord.GetConfigChange())

	case walRecord.Phase1Change != nil:
		phase1change := walRecord.GetPhase1Change()
		this.promisedBallot = phase1change.GetPromisedBallot()

	case walRecord.Phase2Change != nil:
		phase2change := walRecord.GetPhase2Change()
		this.votedBallot = phase2change.GetVotedBallot()
		this.votedValue = phase2change.GetVotedValue()

	case walRecord.BallotChange != nil:
		ballotChange := walRecord.GetBallotChange()
		this.proposalBallot = ballotChange.GetProposalBallot()

	case walRecord.LearnerChange != nil:
		learnerChange := walRecord.GetLearnerChange()
		if learnerChange.ChosenValue != nil {
			this.chosenValue = learnerChange.GetChosenValue()
		} else {
			this.updateBallotStatuses(learnerChange.BallotStatusList)
		}
	}
	return nil
}

// TakeCheckpoint saves current state into the wal as a checkpoint record.
func (this *Paxos) TakeCheckpoint() error {
	lock := this.ctlr.ReadLockAll()
	defer lock.Unlock()

	config := thispb.Configuration{}
	config.InstanceId = proto.String(this.uid)
	config.LearnerList = this.learnerList
	config.ProposerList = this.proposerList
	config.AcceptorList = this.acceptorList
	config.MajoritySize = proto.Int32(int32(this.majoritySize))

	if this.IsLearner() {
		config.IsLearner = proto.Bool(true)
	}
	if this.IsProposer() {
		config.IsProposer = proto.Bool(true)
		config.ProposerIndex = proto.Int32(int32(this.proposerIndex))
	}
	if this.IsAcceptor() {
		config.IsAcceptor = proto.Bool(true)
	}

	checkpoint := thispb.Checkpoint{}
	checkpoint.Configuration = &config
	if this.proposalBallot >= 0 {
		checkpoint.ProposalBallot = proto.Int64(this.proposalBallot)
	}
	if this.promisedBallot >= 0 {
		checkpoint.PromisedBallot = proto.Int64(this.promisedBallot)
	}
	if this.votedBallot >= 0 {
		checkpoint.VotedBallot = proto.Int64(this.votedBallot)
		checkpoint.VotedValue = this.votedValue
	}
	if this.chosenValue != nil {
		checkpoint.ChosenValue = this.chosenValue
	}

	walRecord := thispb.WALRecord{}
	walRecord.Checkpoint = &checkpoint
	data, errMarshal := proto.Marshal(&walRecord)
	if errMarshal != nil {
		this.Errorf("could not marshal checkpoint wal record: %v", errMarshal)
		return errMarshal
	}

	if err := this.wal.AppendCheckpointRecord(this.uid, data); err != nil {
		this.Errorf("could not append checkpoint record: %v", err)
		return err
	}
	return nil
}

// IsLearner returns true if this paxos instance is a learner.
func (this *Paxos) IsLearner() bool {
	return atomic.LoadInt32(&this.isLearner) != 0
}

// IsAcceptor returns true if this paxos instance is an acceptor.
func (this *Paxos) IsAcceptor() bool {
	return atomic.LoadInt32(&this.isAcceptor) != 0
}

// IsProposer returns true if this paxos instance is a learner.
func (this *Paxos) IsProposer() bool {
	return atomic.LoadInt32(&this.isProposer) != 0
}

// ProposeRPC handles ProposeRequest rpc.
func (this *Paxos) ProposeRPC(header *msgpb.Header,
	request *thispb.ProposeRequest) (status error) {

	token, errToken := this.ctlr.NewToken("ProposeRPC", nil, "proposer")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if !this.IsProposer() {
		this.Errorf("this paxos instance is not a proposer; rejecting %s", header)
		return errs.ErrInvalid
	}

	start := time.Now()
	defer func() {
		this.Infof("propose operation took %v time with status: %v",
			time.Since(start), status)
	}()

	var chosen []byte
	proposal := request.GetProposedValue()
	for ii := 0; chosen == nil && ii < this.opts.MaxProposeRetries; ii++ {
		ballot, errNext := this.getNextProposalBallot()
		if errNext != nil {
			this.Errorf("could not select higher ballot: %v", errNext)
			return errNext
		}
		this.Infof("using ballot number %d for next proposal", ballot)

		value, errPropose := this.doPropose(ballot, proposal)
		if errPropose != nil {
			this.Warningf("could not propose with ballot %d: %v", ballot, errPropose)
			time.Sleep(this.opts.ProposeRetryInterval)
			continue
		}
		chosen = value
	}

	if chosen == nil {
		this.Errorf("could not propose value %s in %d attempts",
			proposal, this.opts.MaxProposeRetries)
		return errs.ErrRetry
	}

	response := thispb.ProposeResponse{}
	response.ChosenValue = chosen
	message := thispb.PaxosMessage{}
	message.ProposeResponse = &response
	data, errMarshal := proto.Marshal(&message)
	if errMarshal != nil {
		this.Errorf("could not marshal propose response: %v", errMarshal)
		return errMarshal
	}

	clientID := header.GetMessengerId()
	resHeader := this.msn.NewResponse(header)
	errSend := this.msn.Send(clientID, resHeader, data)
	if errSend != nil {
		this.Errorf("could not send propose response to %s: %v", clientID, errSend)
		return errSend
	}
	return nil
}

// Phase1RPC handles Phase1Request rpc.
func (this *Paxos) Phase1RPC(header *msgpb.Header,
	request *thispb.Phase1Request) (status error) {

	token, errToken := this.ctlr.NewToken("Phase1RPC", nil, "acceptor")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if !this.IsAcceptor() {
		this.Errorf("this paxos instance is not an acceptor; rejecting %s", header)
		return errs.ErrInvalid
	}

	clientID := header.GetMessengerId()
	defer func() {
		response := thispb.Phase1Response{}
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		if this.votedBallot >= 0 {
			response.VotedBallot = proto.Int64(this.votedBallot)
			response.VotedValue = this.votedValue
		}
		message := thispb.PaxosMessage{}
		message.Phase1Response = &response
		data, errMarshal := proto.Marshal(&message)
		if errMarshal != nil {
			this.Errorf("could not marshal phase1 response %s: %v", message,
				errMarshal)
			status = errs.MergeErrors(status, errMarshal)
			return
		}

		resHeader := this.msn.NewResponse(header)
		if err := this.msn.Send(clientID, resHeader, data); err != nil {
			this.Errorf("could not send phase1 response to %s: %v", clientID, err)
			status = errs.MergeErrors(status, err)
			return
		}
		this.Infof("sent phase1 response %s to client %s", resHeader, clientID)
	}()

	ballot := request.GetBallotNumber()
	if ballot < this.promisedBallot {
		this.Warningf("phase1 request from %s is ignored due to stale ballot %d",
			clientID, ballot)
		return nil
	}

	if ballot == this.promisedBallot {
		this.Warningf("duplicate phase1 request from client %s with an already "+
			"promised ballot number %d", clientID, ballot)
		return nil
	}

	// Save promise to the wal.
	phase1change := thispb.Phase1Change{}
	phase1change.PromisedBallot = proto.Int64(ballot)
	walRecord := thispb.WALRecord{}
	walRecord.Phase1Change = &phase1change
	data, errMarshal := proto.Marshal(&walRecord)
	if errMarshal != nil {
		this.Errorf("could not marshal wal record: %v", errMarshal)
		return errMarshal
	}
	if _, err := this.wal.SyncChangeRecord(this.uid, data); err != nil {
		this.Errorf("could not log wal record: %v", err)
		return err
	}

	this.promisedBallot = ballot
	this.Infof("promised higher ballot %d", ballot)
	return nil
}

// Phase2RPC handles Phase2Request rpc.
func (this *Paxos) Phase2RPC(header *msgpb.Header,
	request *thispb.Phase2Request) (status error) {

	token, errToken := this.ctlr.NewToken("Phase2RPC", nil, "acceptor")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if !this.IsAcceptor() {
		this.Errorf("this paxos instance is not an acceptor; rejecting %s", header)
		return errs.ErrInvalid
	}

	clientID := header.GetMessengerId()
	defer func() {
		response := thispb.Phase2Response{}
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		if this.votedBallot >= 0 {
			response.VotedBallot = proto.Int64(this.votedBallot)
			response.VotedValue = this.votedValue
		}
		message := thispb.PaxosMessage{}
		message.Phase2Response = &response
		data, errMarshal := proto.Marshal(&message)
		if errMarshal != nil {
			this.Errorf("could not marshal phase2 response %s: %v", message,
				errMarshal)
			status = errs.MergeErrors(status, errMarshal)
			return
		}
		resHeader := this.msn.NewResponse(header)
		if err := this.msn.Send(clientID, resHeader, data); err != nil {
			this.Errorf("could not send phase2 response to %s: %v", clientID, err)
			status = errs.MergeErrors(status, err)
			return
		}
	}()

	ballot := request.GetBallotNumber()
	if ballot < this.promisedBallot {
		this.Warningf("phase2 request from %s is ignored due to stale ballot %d",
			clientID, ballot)
		return nil
	}

	if ballot > this.promisedBallot {
		this.Errorf("phase2 request from client %s without acquiring a prior "+
			"promise", clientID)
		return nil
	}

	// Save phase2 vote to the wal.
	phase2change := thispb.Phase2Change{}
	phase2change.VotedBallot = proto.Int64(ballot)
	phase2change.VotedValue = request.GetProposedValue()
	walRecord := thispb.WALRecord{}
	walRecord.Phase2Change = &phase2change
	data, errMarshal := proto.Marshal(&walRecord)
	if errMarshal != nil {
		this.Errorf("could not marshal wal record: %v", errMarshal)
		return errMarshal
	}
	if _, err := this.wal.SyncChangeRecord(this.uid, data); err != nil {
		this.Errorf("could not log wal record for phase2 vote: %v", err)
		return err
	}

	this.votedBallot = ballot
	this.votedValue = request.GetProposedValue()

	// Send a notification to the learners.

	ballotStatus := thispb.BallotStatus{}
	ballotStatus.BallotNumber = proto.Int64(this.votedBallot)
	ballotStatus.VotedValue = this.votedValue
	ballotStatus.AcceptorList = []string{this.msn.UID()}
	learn := thispb.LearnNotification{}
	learn.BallotStatusList = append(learn.BallotStatusList, &ballotStatus)
	message := thispb.PaxosMessage{}
	message.LearnNotification = &learn
	data, errMarshal = proto.Marshal(&message)
	if errMarshal != nil {
		this.Errorf("could not marshal learn notification: %v", errMarshal)
		return errMarshal
	}

	notif := this.msn.NewRequest(this.namespace, this.uid, "ClassicPaxos.Learn")
	for _, learner := range this.learnerList {
		if err := this.msn.Send(learner, notif, data); err != nil {
			this.Warningf("could not send learn notification to %s (ignored): %v",
				learner, err)
		}
	}
	_ = this.msn.CloseMessage(notif)

	return nil
}

func (this *Paxos) LearnRPC(header *msgpb.Header,
	request *thispb.LearnNotification) (status error) {

	token, errToken := this.ctlr.NewToken("LearnRPC", nil, "learner")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	if !this.IsLearner() {
		this.Errorf("this paxos instance is not a learner; rejecting %s", header)
		return errs.ErrInvalid
	}

	// If consensus result is already known to both parties, then we don't need
	// send any further messages. Just to be safe, check if both parties agree on
	// the consensus result.
	clientID := header.GetMessengerId()
	if request.ChosenValue != nil && this.chosenValue != nil {
		if bytes.Compare(request.ChosenValue, this.chosenValue) != 0 {
			this.Fatalf("consensus results for %s is different between %s and %s",
				this.uid, clientID, this.msn.UID())
		}
		return nil
	}

	// If the sender is not a learner, we don't need to respond with our state.
	isClientLearner := false
	for _, learner := range this.learnerList {
		if learner == clientID {
			isClientLearner = true
			break
		}
	}

	// Flags to indicate the resulting learner state due to this message.
	newInformation := false
	moreInformation := false

	// If the client is a learner who doesn't know the result of consensus and
	// we have more information than the client, we let him know what we know.
	defer func() {
		if isClientLearner && request.ChosenValue == nil && moreInformation {
			learn := thispb.LearnNotification{}
			if this.chosenValue != nil {
				learn.ChosenValue = this.chosenValue
			} else {
				for ballot, value := range this.ballotValueMap {
					status := &thispb.BallotStatus{}
					status.BallotNumber = proto.Int64(ballot)
					status.VotedValue = value
					for acceptor := range this.ballotAcceptorsMap[ballot] {
						status.AcceptorList = append(status.AcceptorList, acceptor)
					}
					learn.BallotStatusList = append(learn.BallotStatusList, status)
				}
			}
			message := thispb.PaxosMessage{}
			message.LearnNotification = &learn
			data, errMarshal := proto.Marshal(&message)
			if errMarshal != nil {
				this.Errorf("could not marshal learn response: %v", errMarshal)
				status = errs.MergeErrors(status, errMarshal)
				return
			}

			notification := this.msn.NewRequest(this.namespace, this.uid,
				"ClassicPaxos.Learn")
			if err := this.msn.Send(clientID, notification, data); err != nil {
				this.Errorf("could not send learn response to %s: %v", clientID, err)
				status = errs.MergeErrors(status, err)
				return
			}
			_ = this.msn.CloseMessage(notification)
		}
	}()

	// We don't need to process this message if we know already know the
	// consensus result.
	if this.chosenValue != nil {
		moreInformation = true
		return nil
	}

	// If new information is learned by end of this function, save it to the wal.
	defer func() {
		if newInformation {
			learn := thispb.LearnerChange{}
			if this.chosenValue != nil {
				learn.ChosenValue = this.chosenValue
			} else {
				learn.BallotStatusList = request.BallotStatusList
			}
			walRecord := thispb.WALRecord{}
			walRecord.LearnerChange = &learn
			data, errMarshal := proto.Marshal(&walRecord)
			if errMarshal != nil {
				this.Errorf("could not marshal learner change wal record: %v",
					errMarshal)
				status = errs.MergeErrors(status, errMarshal)
				return
			}
			if _, err := this.wal.SyncChangeRecord(this.uid, data); err != nil {
				this.Errorf("could not log learner change to the wal: %v", err)
				status = errs.MergeErrors(status, err)
				return
			}
		}
	}()

	// A learn message may indicate the final result of the consensus, in which
	// case, update local state.
	if request.ChosenValue != nil {
		this.chosenValue = request.GetChosenValue()
		return nil
	}

	// If consensus result is unknown, merge local state with the new state from
	// the request to figure out the result of consensus.
	newInformation, moreInformation = this.updateBallotStatuses(
		request.BallotStatusList)
	return nil
}

// Dispatch implements the msg.Handler interface for Paxos objects.
func (this *Paxos) Dispatch(header *msgpb.Header, data []byte) error {
	request := header.GetRequest()
	if request == nil {
		this.Error("rpc message %s is not a request", header)
		return errs.ErrInvalid
	}

	if request.GetObjectId() != this.uid {
		this.Errorf("rpc request [%s] doesn't belong to this paxos instance %s",
			header, this.uid)
		return errs.ErrInvalid
	}

	message := &thispb.PaxosMessage{}
	if err := proto.Unmarshal(data, message); err != nil {
		this.Errorf("could not parse message [%s]: %v", header, err)
		return err
	}

	switch {
	case message.ProposeRequest != nil:
		if !this.IsProposer() {
			return errs.ErrInvalid
		}
		return this.ProposeRPC(header, message.ProposeRequest)

	case message.Phase1Request != nil:
		if !this.IsAcceptor() {
			return errs.ErrInvalid
		}
		return this.Phase1RPC(header, message.Phase1Request)

	case message.Phase2Request != nil:
		if !this.IsAcceptor() {
			return errs.ErrInvalid
		}
		return this.Phase2RPC(header, message.Phase2Request)

	case message.LearnNotification != nil:
		if !this.IsLearner() {
			return errs.ErrInvalid
		}
		return this.LearnRPC(header, message.LearnNotification)

	default:
		this.Errorf("rpc request [%s] has no request parameters", header)
		return errs.ErrInvalid
	}
}

func (this *Paxos) Propose(value []byte, timeout time.Duration) (
	[]byte, error) {

	// If local instance is not a proposer, find a proposer randomly.
	proposer := this.msn.UID()
	if !this.IsProposer() {
		proposer = this.proposerList[rand.Intn(len(this.proposerList))]
	}
	this.Infof("using %s as the proposer", proposer)

	request := thispb.ProposeRequest{}
	request.ProposedValue = value
	message := thispb.PaxosMessage{}
	message.ProposeRequest = &request
	reqData, errMarshal := proto.Marshal(&message)
	if errMarshal != nil {
		this.Errorf("could not marshal propose request: %v", errMarshal)
		return nil, errMarshal
	}

	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Propose")
	if err := this.msn.Send(proposer, reqHeader, reqData); err != nil {
		this.Errorf("could not send propose request to %s: %v", proposer, err)
		return nil, err
	}

	_, resData, errRecv := this.msn.Receive(reqHeader, timeout)
	if errRecv != nil {
		this.Errorf("could not receive propose response from %s: %v", proposer,
			errRecv)
		return nil, errRecv
	}

	if err := proto.Unmarshal(resData, &message); err != nil {
		this.Errorf("could not parse propose response from %v", proposer, err)
		return nil, err
	}

	if message.ProposeResponse == nil {
		this.Errorf("propose response from %s is empty", proposer)
		return nil, errs.ErrCorrupt
	}

	response := message.GetProposeResponse()
	return response.GetChosenValue(), nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Paxos) getNextProposalBallot() (int64, error) {
	nextProposalBallot := this.proposalBallot
	if this.proposalBallot < 0 {
		nextProposalBallot = int64(this.proposerIndex)
	} else {
		nextProposalBallot += int64(len(this.proposerList))
	}

	ballotChange := thispb.BallotChange{}
	ballotChange.ProposalBallot = proto.Int64(nextProposalBallot)
	walRecord := thispb.WALRecord{}
	walRecord.BallotChange = &ballotChange
	data, errMarshal := proto.Marshal(&walRecord)
	if errMarshal != nil {
		this.Errorf("could not marshal propose change wal record: %v", errMarshal)
		return -1, errMarshal
	}

	if _, err := this.wal.AppendChangeRecord(this.uid, data); err != nil {
		this.Errorf("could not log ballot change to wal: %v", err)
		return -1, err
	}

	this.proposalBallot = nextProposalBallot
	return nextProposalBallot, nil
}

func (this *Paxos) getPhase1AcceptorList(ballot int64) []string {
	maxAcceptors := len(this.acceptorList)
	numAcceptors := this.majoritySize + this.opts.NumExtraPhase1Acceptors
	if numAcceptors > maxAcceptors {
		numAcceptors = maxAcceptors
	}
	acceptorList := make([]string, numAcceptors)
	copy(acceptorList, this.acceptorList[0:numAcceptors])
	return acceptorList
}

func (this *Paxos) doPropose(ballot int64, value []byte) ([]byte, error) {
	votedValue, acceptorList, errPhase1 := this.doPhase1(ballot)
	if errPhase1 != nil {
		this.Errorf("could not complete paxos phase1: %v", errPhase1)
		return nil, errPhase1
	}

	if votedValue != nil {
		value = votedValue
	}

	errPhase2 := this.doPhase2(ballot, value, acceptorList)
	if errPhase2 != nil {
		this.Errorf("could not complete paxos phase2: %v", errPhase2)
		return nil, errPhase2
	}
	return value, nil
}

func (this *Paxos) doPhase1(ballot int64) ([]byte, []string, error) {
	phase1request := &thispb.Phase1Request{}
	phase1request.BallotNumber = proto.Int64(ballot)
	message := &thispb.PaxosMessage{}
	message.Phase1Request = phase1request
	data, errMarshal := proto.Marshal(message)
	if errMarshal != nil {
		this.Errorf("could not marshal phase1 request: %v", errMarshal)
		return nil, nil, errMarshal
	}

	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Phase1")
	defer this.msn.CloseMessage(reqHeader)

	count := 0
	var errSend error
	phase1AcceptorList := this.getPhase1AcceptorList(ballot)
	for _, acceptor := range phase1AcceptorList {
		if err := this.msn.Send(acceptor, reqHeader, data); err != nil {
			this.Warningf("could not send phase1 message to acceptor %s: %v",
				acceptor, err)
			errSend = errs.MergeErrors(errSend, err)
			continue
		}
		count++
	}

	if count < this.majoritySize {
		this.Errorf("could not send phase1 request to majority nodes: %v", errSend)
		return nil, nil, errSend
	}
	this.Infof("sent phase1 request %s to acceptors %v", reqHeader,
		phase1AcceptorList)

	var acceptorList []string
	maxVotedBallot := int64(-1)
	var maxVotedValue []byte
	responseMap := make(map[string]*thispb.Phase1Response)

	deadline := time.Now().Add(this.opts.Phase1Timeout)
	for len(responseMap) < this.majoritySize {
		timeLeft := deadline.Sub(time.Now())
		header, data, errReceive := this.msn.Receive(reqHeader, timeLeft)
		if errReceive != nil {
			this.Warningf("could not receive any more phase1 responses for %s: %v",
				reqHeader, errReceive)
			break
		}

		senderID := header.GetMessengerId()
		if _, ok := responseMap[senderID]; ok {
			this.Warningf("duplicate phase1 response from %s (ignored)", senderID)
			continue
		}

		// TODO Check the response header for errors.

		message := thispb.PaxosMessage{}
		if err := proto.Unmarshal(data, &message); err != nil {
			this.Warningf("could not parse phase1 response from %s (ignored): %v",
				senderID, err)
			continue
		}

		if message.Phase1Response == nil {
			this.Warningf("phase1 response data is empty from %s (ignored)",
				senderID)
			continue
		}

		response := message.GetPhase1Response()
		if response.PromisedBallot == nil {
			this.Warningf("phase1 response from %s has no promise ballot", senderID)
			continue
		}

		promisedBallot := response.GetPromisedBallot()
		if promisedBallot > ballot {
			this.Warningf("acceptor %s has moved on to ballot %d", senderID,
				promisedBallot)
			continue
		}

		if promisedBallot < ballot {
			this.Errorf("acceptor %s did not promise higher ballot %d", senderID,
				ballot)
			continue
		}

		// We received a promise from this acceptor.
		acceptorList = append(acceptorList, senderID)
		responseMap[senderID] = response

		if response.VotedBallot != nil {
			votedBallot := response.GetVotedBallot()
			if votedBallot > maxVotedBallot {
				maxVotedBallot = votedBallot
				maxVotedValue = response.GetVotedValue()
			}
		}
	}

	if len(responseMap) < this.majoritySize {
		this.Warningf("could not get majority phase1 votes %v for ballot %d",
			responseMap, ballot)
		return nil, nil, errs.ErrRetry
	}

	if maxVotedValue == nil {
		this.Infof("no prior value was chosen as per phase1 responses %v",
			responseMap)
	} else {
		this.Infof("value [%s] could have been chosen as per phase1 responses %v",
			maxVotedValue, responseMap)
	}

	return maxVotedValue, acceptorList, nil
}

func (this *Paxos) doPhase2(ballot int64, value []byte,
	acceptorList []string) error {

	phase2request := &thispb.Phase2Request{}
	phase2request.BallotNumber = proto.Int64(ballot)
	phase2request.ProposedValue = value
	message := &thispb.PaxosMessage{}
	message.Phase2Request = phase2request
	data, errMarshal := proto.Marshal(message)
	if errMarshal != nil {
		this.Errorf("could not marshal phase2 request: %v", errMarshal)
		return nil
	}

	header := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Phase2")
	defer this.msn.CloseMessage(header)

	count := 0
	var errSend error
	for _, acceptor := range acceptorList {
		if err := this.msn.Send(acceptor, header, data); err != nil {
			this.Warningf("could not send phase2 request to acceptor %s: %v",
				acceptor, err)
			errSend = errs.MergeErrors(errSend, err)
			continue
		}
		count++
	}

	if count < this.majoritySize {
		this.Errorf("could not send phase2 request to majority acceptors: %v",
			errSend)
		return errSend
	}
	this.Infof("send phase2 request %s to acceptors: %v", header, acceptorList)

	responseMap := make(map[string]*thispb.Phase2Response)
	deadline := time.Now().Add(this.opts.Phase2Timeout)
	for len(responseMap) < this.majoritySize {
		timeLeft := deadline.Sub(time.Now())
		resHeader, data, errReceive := this.msn.Receive(header, timeLeft)
		if errReceive != nil {
			break
		}

		senderID := resHeader.GetMessengerId()
		if _, ok := responseMap[senderID]; ok {
			this.Warningf("duplicate phase2 response from %s (ignored)", senderID)
			continue
		}

		message := thispb.PaxosMessage{}
		if err := proto.Unmarshal(data, &message); err != nil {
			this.Warningf("could not parse phase2 response from %s (ignored): %v",
				senderID, err)
			continue
		}

		if message.Phase2Response == nil {
			this.Warningf("phase2 response data is empty from %s (ignored)",
				senderID)
			continue
		}
		response := message.GetPhase2Response()
		promisedBallot := response.GetPromisedBallot()

		if promisedBallot < ballot {
			this.Errorf("as per phase2 response, acceptor %s seems to have rolled "+
				"back on his phase1 promise to ballot %d", senderID, ballot)
			continue
		}

		if promisedBallot > ballot {
			this.Warningf("acceptor %s has moved on to higher ballot %d", senderID,
				promisedBallot)
			continue
		}

		responseMap[senderID] = response
	}

	if len(responseMap) < this.majoritySize {
		this.Warningf("could not get majority phase2 votes %v for value [%s] "+
			"ballot %d", responseMap, value, ballot)
		return errs.ErrRetry
	}

	this.Infof("value [%s] is chosen by phase2 responses %v", value, responseMap)
	return nil
}

func (this *Paxos) updateBallotStatuses(
	statusList []*thispb.BallotStatus) (bool, bool) {

	newInformation := false
	moreInformation := false
	for _, status := range statusList {
		ballot := status.GetBallotNumber()
		votedValue := status.GetVotedValue()
		acceptorList := status.GetAcceptorList()
		this.ballotValueMap[ballot] = votedValue

		acceptorSet, ok := this.ballotAcceptorsMap[ballot]
		if !ok {
			acceptorSet = make(map[string]struct{})
			this.ballotAcceptorsMap[ballot] = acceptorSet
		}

		for _, acceptor := range acceptorList {
			acceptorSet[acceptor] = struct{}{}
			newInformation = true
		}

		if len(acceptorSet) > len(acceptorList) {
			moreInformation = true
		}

		if len(acceptorSet) >= this.majoritySize {
			moreInformation = true
			this.chosenValue = votedValue
			break
		}
	}
	return newInformation, moreInformation
}

// restoreConfig loads paxos configuration of the object from config proto.
func (this *Paxos) restoreConfig(config *thispb.Configuration) error {
	instanceID := config.GetInstanceId()
	if instanceID != this.uid {
		this.Errorf("config data belongs to different instance %s", instanceID)
		return errs.ErrInvalid
	}

	this.learnerList = config.GetLearnerList()
	this.proposerList = config.GetProposerList()
	this.acceptorList = config.GetAcceptorList()

	if config.IsLearner != nil && config.GetIsLearner() {
		atomic.StoreInt32(&this.isLearner, 1)
	}
	if config.IsAcceptor != nil && config.GetIsAcceptor() {
		atomic.StoreInt32(&this.isAcceptor, 1)
	}
	if config.IsProposer != nil && config.GetIsProposer() {
		atomic.StoreInt32(&this.isProposer, 1)
		this.proposerIndex = int(config.GetProposerIndex())
		this.Infof("configured as proposer %d", this.proposerIndex)
	}

	if config.MajoritySize != nil {
		this.majoritySize = int(config.GetMajoritySize())
	}
	return nil
}
