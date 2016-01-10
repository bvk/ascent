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
// This file defines client interface for this package.
//
// THREAD SAFETY
//
// Thread safety depends on the specific wal implementations; however, all WAL
// types defined in this package are thread-safe.
//

package wal

// Logical Sequence Number for wal records to uniquely identify a record
// position in the wal.
type LSN interface {
}

// WriteAheadLog interface defines semantics for all write ahead logs.
// Interface designed to support replicated logs, transactional logs and
// replicated transactional logs.
type WriteAheadLog interface {
	// ConfigureRecoverer registers a separate recoverer for all records with the
	// matching user id.  There can only be one recoverer for an uid.
	//
	// uid: User id for the record.
	//
	// recoverer: Recoverer for the records with matching uid.
	//
	// Returns nil on success.
	ConfigureRecoverer(uid string, recoverer Recoverer) error

	// Recover restores state stored in the wal by calling recoverer methods
	// repeatedly for each wal record. Recoverer is invoked with the state stored
	// in the recent, committed checkpoint records first followed by the change
	// records logged after checkpoint is started.
	//
	// recoverer: User defined object that know how to restore state from the wal
	// records. This recoverer receives the records that do not have any private
	// recoverer configured using ConfigureRecoverer operation.
	//
	// Returns an error if wal data couldn't be read or the error returned by
	// recoverer during recovery if any.
	Recover(recoverer Recoverer) error

	// QueueChangeRecord queues a record into the wal atomically. Record will be
	// written to the wal later, so this operation doesn't block.
	//
	// In case of backend failures, corresponding record may not be written, in
	// which case, wal ensures that no other following records (sync or async)
	// are logged as well.
	//
	// In case of restarts, if this record is not recoverable then, no following
	// record is recovered.
	//
	// uid: User id for the record.
	//
	// record: User data written to the wal.
	//
	// On success, returns a wal-local, monotonically increasing unique id for
	// the record's position.
	QueueChangeRecord(uid string, data []byte) LSN

	// AppendChangeRecord atomically appends a record to the wal. This operation
	// blocks till this record and all records before it are written to the file
	// system. Writing to the kernel may not ensure durability because data could
	// still be cached in the kernel buffers.
	//
	// In case of backend failures record may not be written to the file
	// system. In such cases, wal zeros out the record and returns a failure to
	// the user. Users may choose to retry as necessary.
	//
	// In case of restarts, if this record is not recoverable then, no following
	// records are recovered.
	//
	// uid: User id for the record.
	//
	// record: User data written to the wal.
	//
	// On success, returns a wal-local, monotonically increasing unique id for
	// the record's position.
	AppendChangeRecord(uid string, data []byte) (LSN, error)

	// SyncChangeRecord is similar to AppendChangeRecord, but performs a
	// fdatasync equivalent operation so that record (and all records before it)
	// becomes durable. Users should make sure disk caches are disabled in the
	// system.
	//
	// uid: User id for the record.
	//
	// record: User data written to the wal.
	//
	// On success, returns a wal-local, monotonically increasing unique id for
	// the record's position.
	SyncChangeRecord(uid string, data []byte) (LSN, error)

	// BeginCheckpoint starts a checkpoint operation. Only one checkpoint can be
	// active at a time.
	//
	// Returns nil on success.
	BeginCheckpoint() error

	// EndCheckpoint closes the checkpoint started by a previous BeginCheckpoint
	// operation.
	//
	// commit: When true checkpoint is committed to the disk; otherwise, it is
	// canceled.
	//
	// Returns an error on backend failures. Users can choose to retry as many
	// times as necessary to commit or abort the checkpoint.
	EndCheckpoint(commit bool) error

	// AppendCheckpointRecord writes a checkpoint record to the wal. If
	// checkpoint state is huge, it can be split into multiple records.
	//
	// uid: User id for the record.
	//
	// record: User data written into the checkpoint record.
	//
	// Returns an error on backend failures. A failure in logging a checkpoint
	// record doesn't abort the checkpoint because retrying the same operation
	// may succeed (For example, it may succeed when free disk space becomes
	// available.)
	AppendCheckpointRecord(uid string, data []byte) error
}

// Recoverer interface defines necessary functions for objects that store their
// state and progress into wals and need to recover/reconstruct the state after
// a crash.
type Recoverer interface {
	// RecoverCheckpoint function is invoked with last recent successful
	// checkpointed state. If checkpoint was recorded as multiple records, then
	// this function is invoked multiple times with all checkpoint record in the
	// same order as they were written. If there were any change records written
	// in between the checkpoint records, they will be invoked *after* all
	// checkpoint records are recovered. So, if checkpointed state was not a
	// consistent snapshot, then change records need to figure out if their side
	// effect was already restored as part of recovering the checkpoint.
	//
	// Also, this function may not be invoked if no checkpoints were taken
	// successfully.
	//
	// uid: User id for the record.
	//
	// data: User data in the checkpoint record.
	//
	// Recovery is aborted if this callback returns a non-nil error.
	RecoverCheckpoint(uid string, data []byte) error

	// RecoverChange function is invoked for every change record logged into the
	// wal. If there was a successful checkpoint, only change records that were
	// logged after the corresponding BeginCheckpoint call are recovered. All
	// change records are recovered when no checkpoint was taken.
	//
	// lsn: A wal-local, unique, monotonically increasing logical sequence number
	// for each change record. Since checkpoints could be aborted in the middle,
	// LSN ids may not be in contiguous sequence.
	//
	// uid: User id for the record.
	//
	// data: User data in the change record.
	//
	// Recovery is aborted if this callback returns a non-nil error.
	RecoverChange(lsn LSN, uid string, data []byte) error
}
