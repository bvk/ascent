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

	// Time to wait for a learner to acknowledge a phase2 vote by the acceptor.
	LearnTimeout time.Duration
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
	if this.LearnTimeout < time.Millisecond {
		err := errs.NewErrInvalid("learn timeout must be at least a millisecond")
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
	majoritySize int32

	//
	// Acceptor state.
	//
	promisedBallot int64
	votedBallot    int64
	votedValue     []byte

	learnerAckMap map[int64]map[string]struct{}

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
	this.learnerAckMap = make(map[int64]map[string]struct{})

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
// once. Also, local object can be used as a client if it doesn't take any
// role.
//
// proposerList: List of proposer messenger names.
//
// acceptorList: List of acceptor messenger names.
//
// learnerList: List of learner messenger names.
//
// Returns nil on success.
func (this *Paxos) Configure(proposerList, acceptorList,
	learnerList []string) error {

	token, errToken := this.ctlr.NewToken("ConfigureAgents", nil, /* timeout */
		"config")
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

	record := thispb.WALRecord{}
	record.ConfigChange = &config
	if _, err := wal.SyncChangeProto(this.wal, this.uid, &record); err != nil {
		this.Errorf("could not append wal record: %v", err)
		return err
	}

	this.doRestoreConfig(&config)
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

	config := checkpoint.GetConfiguration()
	this.doRestoreConfig(config)

	if this.IsProposer() {
		if checkpoint.ProposerState == nil {
			this.Errorf("checkpoint record has no proposer state")
			return errs.ErrInvalid
		}
		this.doRestoreProposer(checkpoint.GetProposerState())
	}

	if this.IsAcceptor() {
		if checkpoint.AcceptorState == nil {
			this.Errorf("checkpoint record has no acceptor state")
			return errs.ErrInvalid
		}
		this.doRestoreAcceptor(checkpoint.GetAcceptorState())
	}

	if this.IsLearner() {
		if checkpoint.LearnerState == nil {
			this.Errorf("checkpoint record has no learner state")
		}
		this.doRestoreLearner(checkpoint.GetLearnerState())
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
		return this.doUpdateConfig(walRecord.GetConfigChange())

	case walRecord.ProposerChange != nil:
		return this.doUpdateProposer(walRecord.GetProposerChange())

	case walRecord.AcceptorChange != nil:
		return this.doUpdateAcceptor(walRecord.GetAcceptorChange())

	case walRecord.LearnerChange != nil:
		return this.doUpdateLearner(walRecord.GetLearnerChange())

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

	checkpoint := thispb.Checkpoint{}

	config := thispb.Configuration{}
	this.doSaveConfiguration(&config)
	checkpoint.Configuration = &config

	if this.IsProposer() {
		state := thispb.ProposerState{}
		this.doSaveProposer(&state)
		checkpoint.ProposerState = &state
	}

	if this.IsAcceptor() {
		state := thispb.AcceptorState{}
		this.doSaveAcceptor(&state)
		checkpoint.AcceptorState = &state
	}

	if this.IsLearner() {
		state := thispb.LearnerState{}
		this.doSaveLearner(&state)
		checkpoint.LearnerState = &state
	}

	walRecord := thispb.WALRecord{}
	walRecord.Checkpoint = &checkpoint
	errAppend := wal.AppendCheckpointProto(this.wal, this.uid, &walRecord)
	if errAppend != nil {
		this.Errorf("could not append checkpoint record: %v", errAppend)
		return errAppend
	}
	return nil
}

// Refresh inspects paxos object state and takes recovery actions as necessary.
func (this *Paxos) Refresh() error {
	if this.IsAcceptor() {
		// TODO: Notify learners about your latest vote.
	}
	if this.IsLearner() {
		// TODO: Query majority acceptors to determine chosen value.
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

// MajoritySize returns the majority quorum size for the paxos instance.
func (this *Paxos) MajoritySize() int {
	return int(atomic.LoadInt32(&this.majoritySize))
}

// ProposeRPC handles ClassicPaxos.Propose rpc.
func (this *Paxos) ProposeRPC(header *msgpb.Header,
	request *thispb.ProposeRequest) (status error) {

	if !this.IsProposer() {
		this.Errorf("this paxos instance is not a proposer; rejecting %s", header)
		return errs.ErrInvalid
	}

	// OPTIMIZATION If this object is also a learner and already knows the
	// consensus result, we don't need to perform expensive proposal.
	if this.IsLearner() {
		lock := this.ctlr.ReadLock("learner")
		defer lock.Unlock()

		if this.chosenValue != nil {
			response := thispb.ProposeResponse{}
			response.ChosenValue = this.chosenValue
			message := thispb.PaxosMessage{}
			message.ProposeResponse = &response
			errSend := msg.SendResponseProto(this.msn, header, &message)
			if errSend != nil {
				this.Errorf("could not send known chosen value as the propose "+
					"response: %v", errSend)
				return errSend
			}
			return nil
		}
		lock.Unlock()
	}

	// Propose operation modifies proposal ballot number, so acquire a token for
	// write access.
	token, errToken := this.ctlr.NewToken("ProposeRPC", nil, /* timeout */
		"proposer", "config")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	var chosen []byte
	proposal := request.GetProposedValue()
	for ii := 0; chosen == nil && ii < this.opts.MaxProposeRetries; ii++ {
		if ii > 0 {
			time.Sleep(this.opts.ProposeRetryInterval)
		}

		// Get the next proposal ballot number.
		ballot, errNext := this.getNextProposalBallot()
		if errNext != nil {
			this.Errorf("could not select higher ballot: %v", errNext)
			return errNext
		}
		this.Infof("using ballot number %d for the proposal", ballot)

		// Collect phase1 promises from majority number of acceptors.
		votedValue, acceptorList, errPhase1 := this.doPhase1(ballot)
		if errPhase1 != nil {
			this.Warningf("could not complete paxos phase1: %v", errPhase1)
			continue
		}

		// If a value was already voted, it may have been chosen, so propose it
		// instead.
		value := proposal
		if votedValue != nil {
			value = votedValue
		}

		// Collect phase2 votes from majority number of acceptors.
		errPhase2 := this.doPhase2(ballot, value, acceptorList)
		if errPhase2 != nil {
			this.Warningf("could not complete paxos phase2: %v", errPhase2)
			continue
		}

		// A value is chosen, break out of the loop.
		chosen = value
		break
	}

	if chosen == nil {
		this.Errorf("could not propose value %s in %d attempts",
			proposal, this.opts.MaxProposeRetries)
		return errs.ErrRetry
	}

	// Close the token, because we are done with the proposal ballot.
	this.ctlr.CloseToken(token)

	// If local node is a learner, update him with the consensus result directly.
	defer func() {
		if this.IsLearner() {
			token, errToken := this.ctlr.NewToken("UpdateLearner", nil, "learner")
			if errToken != nil {
				return
			}
			defer this.ctlr.CloseToken(token)

			change := thispb.LearnerChange{}
			change.ChosenValue = chosen
			if err := this.doUpdateLearner(&change); err != nil {
				this.Warningf("could not update local learner with the consensus "+
					"result (ignored): %v", err)
			}
		}
	}()

	// Send propose response with chosen value.
	response := thispb.ProposeResponse{}
	response.ChosenValue = chosen
	message := thispb.PaxosMessage{}
	message.ProposeResponse = &response
	errSend := msg.SendResponseProto(this.msn, header, &message)
	if errSend != nil {
		this.Errorf("could not send propose response: %v", errSend)
		return errSend
	}
	return nil
}

// Phase1RPC handles ClassicPaxos.Phase1 rpc.
func (this *Paxos) Phase1RPC(header *msgpb.Header,
	request *thispb.Phase1Request) (status error) {

	if !this.IsAcceptor() {
		this.Errorf("this paxos instance is not an acceptor; rejecting %s", header)
		return errs.ErrInvalid
	}

	token, errToken := this.ctlr.NewToken("Phase1RPC", nil, /* timeout */
		"acceptor")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	clientID := header.GetMessengerId()
	respond := func() error {
		response := thispb.Phase1Response{}
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		if this.votedBallot >= 0 {
			response.VotedBallot = proto.Int64(this.votedBallot)
			response.VotedValue = this.votedValue
		}
		message := thispb.PaxosMessage{}
		message.Phase1Response = &response
		errSend := msg.SendResponseProto(this.msn, header, &message)
		if errSend != nil {
			this.Errorf("could not send phase1 response to %s: %v", clientID,
				errSend)
			return errSend
		}
		return nil
	}

	ballot := request.GetBallotNumber()
	if ballot < this.promisedBallot {
		this.Warningf("phase1 request from %s is ignored due to stale ballot %d",
			clientID, ballot)
		return respond()
	}

	if ballot == this.promisedBallot {
		this.Warningf("duplicate phase1 request from client %s with an already "+
			"promised ballot number %d", clientID, ballot)
		return respond()
	}

	// Save the promise into the wal.
	change := thispb.AcceptorChange{}
	change.PromisedBallot = proto.Int64(ballot)
	if err := this.doUpdateAcceptor(&change); err != nil {
		this.Errorf("could not update acceptor state: %v", err)
		return err
	}

	this.Infof("this acceptor has now promised higher ballot %d from %s", ballot,
		clientID)
	return respond()
}

// Phase2RPC handles ClassicPaxos.Phase2 rpc.
func (this *Paxos) Phase2RPC(header *msgpb.Header,
	request *thispb.Phase2Request) (status error) {

	if !this.IsAcceptor() {
		this.Errorf("this paxos instance is not an acceptor; rejecting %s", header)
		return errs.ErrInvalid
	}

	token, errToken := this.ctlr.NewToken("Phase2RPC", nil, /* timeout */
		"acceptor")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	clientID := header.GetMessengerId()
	respond := func() error {
		response := thispb.Phase2Response{}
		response.PromisedBallot = proto.Int64(this.promisedBallot)
		if this.votedBallot >= 0 {
			response.VotedBallot = proto.Int64(this.votedBallot)
			response.VotedValue = this.votedValue
		}
		message := thispb.PaxosMessage{}
		message.Phase2Response = &response
		errSend := msg.SendResponseProto(this.msn, header, &message)
		if errSend != nil {
			this.Errorf("could not send phase2 response to %s: %v", clientID,
				errSend)
			return errSend
		}
		return nil
	}

	ballot := request.GetBallotNumber()
	if ballot < this.promisedBallot {
		this.Warningf("phase2 request from %s is ignored due to stale ballot %d",
			clientID, ballot)
		return respond()
	}

	if ballot > this.promisedBallot {
		this.Errorf("phase2 request from client %s without acquiring a prior "+
			"promise", clientID)
		return respond()
	}
	value := request.GetProposedValue()

	// Save the phase2 vote into the wal.
	change := thispb.AcceptorChange{}
	change.VotedBallot = proto.Int64(ballot)
	change.VotedValue = value
	if err := this.doUpdateAcceptor(&change); err != nil {
		this.Errorf("could not update acceptor state: %v", err)
		return err
	}

	this.Infof("this acceptor has voted for %s in ballot %s", ballot, value)
	if err := respond(); err != nil {
		return err
	}
	this.ctlr.CloseToken(token)

	// Send a notification to all learners.
	_ = this.NotifyAllLearners(ballot, value, this.opts.LearnTimeout)
	return nil
}

// LearnRPC handles ClassicPaxos.Learn rpc.
func (this *Paxos) LearnRPC(header *msgpb.Header,
	request *thispb.LearnRequest) (status error) {

	if !this.IsLearner() {
		this.Errorf("this paxos instance is not a learner; rejecting %s", header)
		return errs.ErrInvalid
	}

	token, errToken := this.ctlr.NewToken("LearnRPC", nil, /* timeout */
		"learner")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	acceptor := header.GetMessengerId()

	change := thispb.LearnerChange{}
	change.VotedBallot = request.VotedBallot
	change.VotedValue = request.VotedValue
	change.VotedAcceptor = proto.String(acceptor)
	if err := this.doUpdateLearner(&change); err != nil {
		this.Errorf("could not update learner state: %v", err)
		return err
	}

	response := thispb.LearnResponse{}
	message := thispb.PaxosMessage{}
	message.LearnResponse = &response
	errSend := msg.SendResponseProto(this.msn, header, &message)
	if errSend != nil {
		this.Errorf("could not respond to learn request from %s: %v", acceptor,
			errSend)
		return errSend
	}
	return nil
}

// Dispatch implements the msg.Handler interface for Paxos objects.
//
// header: Message header for an incoming request.
//
// data: User data in the message.
//
// Returns the result of perform the incoming request.
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

	case message.LearnRequest != nil:
		if !this.IsLearner() {
			return errs.ErrInvalid
		}
		return this.LearnRPC(header, message.LearnRequest)

	default:
		this.Errorf("rpc request [%s] has no request parameters", header)
		return errs.ErrInvalid
	}
}

// NotifyLocalLearner updates local learner with the current vote.
//
// ballot: Ballot number for the vote.
//
// value: The voted value.
//
// Returns nil on success.
func (this *Paxos) NotifyLocalLearner(ballot int64, value []byte) error {
	if !this.IsLearner() {
		return nil
	}

	token, errToken := this.ctlr.NewToken("NotifyLocalLearner", nil, "learner")
	if errToken != nil {
		return errToken
	}
	defer this.ctlr.CloseToken(token)

	change := thispb.LearnerChange{}
	change.VotedBallot = proto.Int64(ballot)
	change.VotedValue = value
	change.VotedAcceptor = proto.String(this.msn.UID())
	return this.doUpdateLearner(&change)
}

// NotifyAllLearners sends the current vote to all learners.
//
// ballot: Ballot number for the vote.
//
// value: The voted value.
//
// timeout: Maximum time duration to notify all learners.
//
// Returns nil on success.
func (this *Paxos) NotifyAllLearners(ballot int64, value []byte,
	timeout time.Duration) error {

	if err := this.NotifyLocalLearner(ballot, value); err != nil {
		return err
	}

	// Make a copy of learners without the local guy.
	self := this.msn.UID()
	lock := this.ctlr.ReadLock("config")
	learnerList := make([]string, 0, len(this.learnerList))
	for _, learner := range this.learnerList {
		if learner != self {
			learnerList = append(learnerList, learner)
		}
	}
	lock.Unlock()

	request := thispb.LearnRequest{}
	request.VotedBallot = proto.Int64(ballot)
	request.VotedValue = value
	message := thispb.PaxosMessage{}
	message.LearnRequest = &request

	// Send notification to all learners.
	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Learn")
	count, errSend := msg.SendAllProto(this.msn, learnerList, reqHeader,
		&message)
	if errSend != nil {
		this.Errorf("could not send learn request to all learners: %v", errSend)
		return errSend
	}

	// Wait for responses from all learners.
	learnerSet := this.learnerAckMap[ballot]
	deadline := time.Now().Add(timeout)
	for ii := 0; ii < count && len(learnerSet) < len(learnerList); ii++ {
		timeout = deadline.Sub(time.Now())
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, reqHeader, timeout,
			&message)
		if errRecv != nil {
			this.Warningf("could not receive learner responses: %v", errRecv)
			break
		}

		learner := resHeader.GetMessengerId()
		if _, ok := learnerSet[learner]; ok {
			this.Warningf("received duplicate learner response from %s (ignored)",
				learner)
			continue
		}

		if message.LearnResponse == nil {
			this.Errorf("learn response from %s is empty", learner)
			continue
		}

		// Save the learner acknowledgment to the wal.
		change := thispb.AcceptorChange{}
		change.AckedBallot = proto.Int64(ballot)
		change.AckedLearner = proto.String(learner)
		if err := this.doUpdateAcceptor(&change); err != nil {
			this.Errorf("could not update acceptor state: %v", err)
			return err
		}
		learnerSet = this.learnerAckMap[ballot]
	}
	_ = this.msn.CloseMessage(reqHeader)
	return nil
}

// Propose proposes given value for a consensus.
//
// value: The value to propose for consensus.
//
// timeout: Maximum time duration for the propose operation.
//
// Returns the chosen value on success.
func (this *Paxos) Propose(value []byte, timeout time.Duration) (
	[]byte, error) {

	// If local instance is not a proposer, find a random proposer.
	proposer := this.msn.UID()
	if !this.IsProposer() {
		proposer = this.proposerList[rand.Intn(len(this.proposerList))]
		this.Infof("using %s as the proposer", proposer)
	}

	// Send the propose request.
	request := thispb.ProposeRequest{}
	request.ProposedValue = value
	message := thispb.PaxosMessage{}
	message.ProposeRequest = &request
	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Propose")
	errSend := msg.SendProto(this.msn, proposer, reqHeader, &message)
	if errSend != nil {
		this.Errorf("could not send propose request to %s: %v", proposer, errSend)
		return nil, errSend
	}

	// Wait for the response.
	_, errRecv := msg.ReceiveProto(this.msn, reqHeader, timeout, &message)
	if errRecv != nil {
		this.Errorf("could not receive propose response from %s: %v", proposer,
			errRecv)
		return nil, errRecv
	}

	if message.ProposeResponse == nil {
		this.Errorf("propose response from %s is empty", proposer)
		return nil, errs.ErrCorrupt
	}

	response := message.GetProposeResponse()
	return response.GetChosenValue(), nil
}

///////////////////////////////////////////////////////////////////////////////

// getNextProposalBallot return the next higher ballot for local proposer.
func (this *Paxos) getNextProposalBallot() (int64, error) {
	nextProposalBallot := this.proposalBallot
	if this.proposalBallot < 0 {
		nextProposalBallot = int64(this.proposerIndex)
	} else {
		nextProposalBallot += int64(len(this.proposerList))
	}

	change := thispb.ProposerChange{}
	change.ProposalBallot = proto.Int64(nextProposalBallot)
	if err := this.doUpdateProposer(&change); err != nil {
		this.Errorf("could not update the proposer state: %v", err)
		return -1, err
	}
	return this.proposalBallot, nil
}

// getPhase1AcceptorList returns list of acceptors for a given ballot.
func (this *Paxos) getPhase1AcceptorList(ballot int64) []string {
	maxAcceptors := len(this.acceptorList)
	numAcceptors := this.MajoritySize() + this.opts.NumExtraPhase1Acceptors
	if numAcceptors > maxAcceptors {
		numAcceptors = maxAcceptors
	}
	acceptorList := make([]string, numAcceptors)
	copy(acceptorList, this.acceptorList[0:numAcceptors])
	return acceptorList
}

// doPhase1 performs classic paxos phase1 steps.
func (this *Paxos) doPhase1(ballot int64) ([]byte, []string, error) {
	phase1request := thispb.Phase1Request{}
	phase1request.BallotNumber = proto.Int64(ballot)
	message := thispb.PaxosMessage{}
	message.Phase1Request = &phase1request

	reqHeader := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Phase1")
	defer this.msn.CloseMessage(reqHeader)

	phase1AcceptorList := this.getPhase1AcceptorList(ballot)
	count, errSend := msg.SendAllProto(this.msn, phase1AcceptorList, reqHeader,
		&message)
	if errSend != nil && count < this.MajoritySize() {
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
	for ii := 0; ii < count && len(responseMap) < this.MajoritySize(); ii++ {
		timeLeft := deadline.Sub(time.Now())

		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, reqHeader, timeLeft,
			&message)
		if errRecv != nil {
			this.Warningf("could not receive more phase1 responses for %s: %v",
				reqHeader, errRecv)
			break
		}

		acceptor := resHeader.GetMessengerId()
		if _, ok := responseMap[acceptor]; ok {
			this.Warningf("duplicate phase1 response from %s (ignored)", acceptor)
			continue
		}

		if message.Phase1Response == nil {
			this.Warningf("phase1 response data is empty from %s (ignored)",
				acceptor)
			continue
		}

		response := message.GetPhase1Response()
		if response.PromisedBallot == nil {
			this.Warningf("phase1 response from %s has no promise ballot", acceptor)
			continue
		}

		promisedBallot := response.GetPromisedBallot()
		if promisedBallot > ballot {
			this.Warningf("acceptor %s has moved on to ballot %d", acceptor,
				promisedBallot)
			break
		}

		if promisedBallot < ballot {
			this.Errorf("acceptor %s did not promise this ballot %d", acceptor,
				ballot)
			continue
		}

		// We received a promise from this acceptor.
		acceptorList = append(acceptorList, acceptor)
		responseMap[acceptor] = response

		// If there was a voted value already, we need to pick the max voted value.
		if response.VotedBallot != nil {
			votedBallot := response.GetVotedBallot()
			if votedBallot > maxVotedBallot {
				maxVotedBallot = votedBallot
				maxVotedValue = response.GetVotedValue()
			}
		}
	}

	if len(responseMap) < this.MajoritySize() {
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

// doPhase2 performs classic paxos phase2 steps.
func (this *Paxos) doPhase2(ballot int64, value []byte,
	acceptorList []string) error {

	phase2request := thispb.Phase2Request{}
	phase2request.BallotNumber = proto.Int64(ballot)
	phase2request.ProposedValue = value
	message := thispb.PaxosMessage{}
	message.Phase2Request = &phase2request

	header := this.msn.NewRequest(this.namespace, this.uid,
		"ClassicPaxos.Phase2")
	defer this.msn.CloseMessage(header)

	count, errSend := msg.SendAllProto(this.msn, acceptorList, header, &message)
	if errSend != nil && count < this.MajoritySize() {
		this.Errorf("could not send phase2 request to majority acceptors: %v",
			errSend)
		return errSend
	}
	this.Infof("send phase2 request %s to acceptors: %v", header, acceptorList)

	responseMap := make(map[string]*thispb.Phase2Response)
	deadline := time.Now().Add(this.opts.Phase2Timeout)
	for ii := 0; ii < count && len(responseMap) < this.MajoritySize(); ii++ {
		timeLeft := deadline.Sub(time.Now())
		message := thispb.PaxosMessage{}
		resHeader, errRecv := msg.ReceiveProto(this.msn, header, timeLeft,
			&message)
		if errRecv != nil {
			break
		}

		acceptor := resHeader.GetMessengerId()
		if _, ok := responseMap[acceptor]; ok {
			this.Warningf("duplicate phase2 response from %s (ignored)", acceptor)
			continue
		}

		if message.Phase2Response == nil {
			this.Warningf("phase2 response data is empty from %s (ignored)",
				acceptor)
			continue
		}
		response := message.GetPhase2Response()
		promisedBallot := response.GetPromisedBallot()

		if promisedBallot < ballot {
			this.Errorf("as per phase2 response, acceptor %s seems to have rolled "+
				"back on his phase1 promise to ballot %d", acceptor, ballot)
			continue
		}

		if promisedBallot > ballot {
			this.Warningf("acceptor %s has moved on to higher ballot %d", acceptor,
				promisedBallot)
			break
		}

		responseMap[acceptor] = response
	}

	if len(responseMap) < this.MajoritySize() {
		this.Warningf("could not get majority phase2 votes %v for value [%s] "+
			"ballot %d", responseMap, value, ballot)
		return errs.ErrRetry
	}

	this.Infof("value [%s] is chosen by phase2 responses %v", value, responseMap)
	return nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Paxos) doUpdateConfig(change *thispb.Configuration) error {
	this.doRestoreConfig(change)
	return nil
}

func (this *Paxos) doUpdateProposer(change *thispb.ProposerChange) error {
	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.ProposerChange = change
		_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
		if errSync != nil {
			this.Errorf("could not write proposer change record: %v", errSync)
			return errSync
		}
	}

	if change.ProposalBallot != nil {
		ballot := change.GetProposalBallot()
		if this.proposalBallot < ballot {
			this.proposalBallot = ballot
		}
	}
	return nil
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

	if change.VotedBallot != nil {
		this.votedBallot = change.GetVotedBallot()
		this.votedValue = change.GetVotedValue()
	}

	if change.AckedBallot != nil {
		ballot := change.GetAckedBallot()
		learner := change.GetAckedLearner()
		learnerSet, found := this.learnerAckMap[ballot]
		if !found {
			learnerSet = make(map[string]struct{})
			this.learnerAckMap[ballot] = learnerSet
		}
		learnerSet[learner] = struct{}{}
	}
	return nil
}

func (this *Paxos) doUpdateLearner(change *thispb.LearnerChange) error {
	if this.chosenValue != nil {
		return nil
	}

	if !this.wal.IsRecovering() {
		walRecord := thispb.WALRecord{}
		walRecord.LearnerChange = change
		_, errSync := wal.SyncChangeProto(this.wal, this.uid, &walRecord)
		if errSync != nil {
			this.Errorf("could not write learner change record: %v", errSync)
			return errSync
		}
	}

	if change.ChosenValue != nil {
		this.chosenValue = change.GetChosenValue()
		this.Infof("consensus result learned from proposer is %s",
			this.chosenValue)
		return nil
	}

	votedBallot := change.GetVotedBallot()
	votedValue := change.GetVotedValue()
	votedAcceptor := change.GetVotedAcceptor()
	this.ballotValueMap[votedBallot] = votedValue
	acceptorSet, found := this.ballotAcceptorsMap[votedBallot]
	if !found {
		acceptorSet = make(map[string]struct{})
		this.ballotAcceptorsMap[votedBallot] = acceptorSet
	}
	acceptorSet[votedAcceptor] = struct{}{}

	if len(acceptorSet) >= this.MajoritySize() {
		this.chosenValue = votedValue
		this.Infof("consensus result learned through votes is %s", votedValue)
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////

func (this *Paxos) doRestoreConfig(config *thispb.Configuration) {
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
	}
	if config.MajoritySize != nil {
		atomic.StoreInt32(&this.majoritySize, config.GetMajoritySize())
	}
}

func (this *Paxos) doRestoreProposer(state *thispb.ProposerState) {
	this.proposalBallot = state.GetProposalBallot()
}

func (this *Paxos) doRestoreAcceptor(state *thispb.AcceptorState) {
	this.promisedBallot = state.GetPromisedBallot()
	this.votedBallot = state.GetVotedBallot()
	this.votedValue = state.GetVotedValue()

	for index := range state.AckedBallotList {
		ballot := state.AckedBallotList[index]
		learner := state.AckedLearnerList[index]

		learnerSet, found := this.learnerAckMap[ballot]
		if !found {
			learnerSet = make(map[string]struct{})
			this.learnerAckMap[ballot] = learnerSet
		}
		learnerSet[learner] = struct{}{}
	}
}

func (this *Paxos) doRestoreLearner(state *thispb.LearnerState) {
	if state.ChosenValue != nil {
		this.chosenValue = state.GetChosenValue()
		return
	}

	for index := range state.VotedBallotList {
		ballot := state.VotedBallotList[index]
		value := state.VotedValueList[index]
		acceptor := state.VotedAcceptorList[index]

		// Ballot numbers can repeat in the list, but corresponding value is stored
		// only once. So, we may find nils in the state.VotedValueList.
		if value != nil {
			this.ballotValueMap[ballot] = value
		}

		acceptorSet, found := this.ballotAcceptorsMap[ballot]
		if !found {
			acceptorSet = make(map[string]struct{})
			this.ballotAcceptorsMap[ballot] = acceptorSet
		}
		acceptorSet[acceptor] = struct{}{}
	}
}

///////////////////////////////////////////////////////////////////////////////

func (this *Paxos) doSaveConfiguration(config *thispb.Configuration) {
	config.MajoritySize = proto.Int32(this.majoritySize)
	config.ProposerList = append([]string{}, this.proposerList...)
	config.AcceptorList = append([]string{}, this.acceptorList...)
	config.LearnerList = append([]string{}, this.learnerList...)
	if this.IsProposer() {
		config.IsProposer = proto.Bool(true)
		config.ProposerIndex = proto.Int32(int32(this.proposerIndex))
	}
	if this.IsAcceptor() {
		config.IsAcceptor = proto.Bool(true)
	}
	if this.IsLearner() {
		config.IsLearner = proto.Bool(true)
	}
}

func (this *Paxos) doSaveProposer(state *thispb.ProposerState) {
	state.ProposalBallot = proto.Int64(this.proposalBallot)
}

func (this *Paxos) doSaveAcceptor(state *thispb.AcceptorState) {
	state.PromisedBallot = proto.Int64(this.promisedBallot)
	state.VotedBallot = proto.Int64(this.votedBallot)
	state.VotedValue = append([]byte{}, this.votedValue...)
	for ballot, learnerSet := range this.learnerAckMap {
		for learner := range learnerSet {
			state.AckedBallotList = append(state.AckedBallotList, ballot)
			state.AckedLearnerList = append(state.AckedLearnerList, learner)
		}
	}
}

func (this *Paxos) doSaveLearner(state *thispb.LearnerState) {
	if this.chosenValue != nil {
		state.ChosenValue = append([]byte{}, this.chosenValue...)
		return
	}

	for ballot, acceptorSet := range this.ballotAcceptorsMap {
		votedValue := append([]byte{}, this.ballotValueMap[ballot]...)
		for acceptor := range acceptorSet {
			state.VotedBallotList = append(state.VotedBallotList, ballot)
			state.VotedAcceptorList = append(state.VotedAcceptorList, acceptor)
			state.VotedValueList = append(state.VotedValueList, votedValue)

			// Store only one copy of the ballot value for each unique ballot.
			votedValue = nil
		}
	}
}
