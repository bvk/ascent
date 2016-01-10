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

package paxos

// Watcher defines the callback interface to learn the consensus results.
type Watcher interface {
	// ConsensusUpdate is invoked on paxos learners when there is a new chosen
	// value. Watches needs to be registered on paxos instances to receive the
	// consensus notifications. Note that users should not do any assumptions
	// about the order of the updates.
	//
	// uid: UID of the paxos instance sending the notification.
	//
	// sequenceID: Sequence id for the consensus result, if paxos instance
	//             supports multiple consensus results.
	//
	// chosenValue: Chosen value as the result of consensus.
	ConsensusUpdate(uid string, sequenceID int64, chosenValue []byte)
}
