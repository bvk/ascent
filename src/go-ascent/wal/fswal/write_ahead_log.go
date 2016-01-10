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
// This file implements WriteAheadLog data type which implements
// WriteAheadLog interface using local files.
//
// THREAD SAFETY
//
// WriteAheadLog is thread safe.
//
// IMPLEMENTATION
//
// WriteAheadLog stores it's data in local files of a fixed
// size. Even though file size is limited, conceptually, WAL size is
// infinite. So, every record is assigned a unique, monotonically increasing
// wal-position -- which acts as the Logical Sequence Number (LSN) for the
// record.
//
// Change records are written into files named in "NAME.change.X" format --
// where X is the file id, which is the wal-position of the first record of the
// file. A new file is created when maximum limit for the file is reached.
//
// Change records are queued in memory till a flush or sync operation is
// invoked.
//
// MaxWriteSize also represents the maximum size allowed for a change or
// checkpoint record. This limit may be removed in future.
//
// Checkpoint records are found in files named in "NAME.checkpoint.X" format --
// where X is the file id, which is the wal-position of the BeginCheckpoint()
// record. Checkpoint files can be as large as necessary and do not have
// maximum limit.
//
// An on-going checkpoint writes its data in a temporary file named,
// "NAME.checkpoint.temp", which is later renamed to "NAME.checkpoint.X" form
// (X is the LSN for begin checkpoint record) when the checkpoint is committed
// using EndCheckpoint() operation.
//
// Since checkpoint data can be huge, adding checkpoint records is always
// synchronous.  Checkpoint records are written immediately to the checkpoint
// file.
//
// TODO
//
// Add support for partial writes, otherwise, record size cannot be bigger than
// MaxWriteSize.
//
// Add support for different types of record checksums.
//
// Add a special EndCheckpoint record so that we can detect truncated
// checkpoint files.
//

package fswal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"

	"go-ascent/base/errs"
	"go-ascent/base/log"
	"go-ascent/wal"

	walpb "proto-ascent/wal"
)

// Options defines user configurable options for local wal.
type Options struct {
	// Maximum number of directory entries to read when scanning a directory for
	// wal data files.
	MaxReadDirNames int

	// Maximum number of bytes to read in one read operation.
	MaxReadSize int

	// Maximum number of bytes to write in one write operation.
	MaxWriteSize int

	// Maximum file size for the change record files.
	MaxFileSize int64

	// File permissions for the data files.
	FileMode os.FileMode
}

// Validate verifies user options for correctness.
func (this *Options) Validate() (status error) {
	if this.MaxReadSize < 8 {
		err := errs.NewErrInvalid("minimum read size must be at least 8 bytes")
		status = errs.MergeErrors(status, err)
	}
	if this.MaxWriteSize < 8 {
		err := errs.NewErrInvalid("minimum write size must be at least 8 bytes")
		status = errs.MergeErrors(status, err)
	}
	if this.MaxFileSize < 8 {
		err := errs.NewErrInvalid("minimum file size must be at least 8 bytes")
		status = errs.MergeErrors(status, err)
	}
	if this.MaxReadDirNames < 1 {
		err := errs.NewErrInvalid("readier must read at least one name per call")
		status = errs.MergeErrors(status, err)
	}
	return status
}

type WriteAheadLog struct {
	log.Logger

	// User configurable options for the wal.
	opts Options

	// Path to the directory where wal files are stored.
	dir string

	// Name of the wal, which is used as the prefix for all wal files.
	name string

	// Mutex for thread-safety.
	mutex sync.Mutex

	// Wal and file offsets to assign to next change and checkpoint records.
	nextChangeOffset     int64
	nextCheckpointOffset int64

	// Flags indicating flush or sync operation is in progress.
	writingChanges     bool
	writingCheckpoints bool

	// Condition variables to wake up any waiting thread after a flush or sync
	// operation.
	writingChangesCond     sync.Cond
	writingCheckpointsCond sync.Cond

	// List of change records and checkpoint records in the queue.
	changeList     [][]byte
	checkpointList [][]byte

	// Flushed offsets for change and checkpoint records. These are always at
	// record boundaries.
	flushChangeOffset     int64
	flushCheckpointOffset int64

	// Wal-offset for the change record that initiated the currently active
	// checkpoint, if any.
	beginCheckpointOffset int64

	// Reference to the currently active checkpoint file.
	checkpointFile *os.File

	// List of change files (by their file ids) on the file system.
	changeFileList []int64

	// List of change files opened for writing.
	changeFileMap map[int64]*os.File

	// Mapping from regexp to the corresponding recoverer.
	recovererMap map[string]wal.Recoverer

	// List of regexp recoverers in the configuration order.
	regexpList []*regexp.Regexp

	// Atomic variable to indicate if wal is currently recovering.
	recovering int32
}

func (this *WriteAheadLog) Initialize(opts *Options,
	dir, name string) (status error) {

	var logger log.Logger
	if this.Logger != nil {
		logger = this.Logger.NewLogger("local-wal:%s", name)
	} else {
		simpleLog := log.SimpleFileLog{}
		if err := simpleLog.Initialize("/dev/stderr"); err != nil {
			return err
		}
		logger = simpleLog.NewLogger("local-wal:%s", name)
	}

	if err := opts.Validate(); err != nil {
		logger.Errorf("bad wal options: %v", err)
		return err
	}

	lwal := WriteAheadLog{
		Logger: logger,
		opts:   *opts,

		dir:  dir,
		name: name,

		beginCheckpointOffset: -1,

		changeFileMap: make(map[int64]*os.File),
		recovererMap:  make(map[string]wal.Recoverer),
	}
	defer func() {
		if status != nil {
			if err := lwal.Close(); err != nil {
				this.Errorf("could not close temporary wal: %v", err)
			}
		}
	}()

	*this = lwal
	this.writingChangesCond.L = &this.mutex
	this.writingCheckpointsCond.L = &this.mutex
	return nil
}

// Close releases all wal resources and destroys the object.
func (this *WriteAheadLog) Close() (status error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.changeFileMap == nil {
		this.Errorf("wal is already closed")
		return errs.ErrClosed
	}

	for fileID, file := range this.changeFileMap {
		if err := file.Close(); err != nil {
			this.Errorf("could not close change records file %d: %v", fileID, err)
			status = errs.MergeErrors(status, err)
		}
	}
	this.changeFileMap = nil

	if this.checkpointFile != nil {
		if err := this.checkpointFile.Close(); err != nil {
			this.Errorf("could not close checkpoints file %d: %v",
				this.beginCheckpointOffset, err)
			status = errs.MergeErrors(status, err)
		}
		this.checkpointFile = nil
	}
	return status
}

// IsRecovering returns true if wal is under recovery.
func (this *WriteAheadLog) IsRecovering() bool {
	return atomic.LoadInt32(&this.recovering) != 0
}

// ConfigureRecoverer adds a recoverer for wal records with the matching user
// id.
//
// uid: User id for the record.
//
// recoverer: Recoverer for the records with matching user id.
//
// Returns nil on success.
func (this *WriteAheadLog) ConfigureRecoverer(re *regexp.Regexp,
	recoverer wal.Recoverer) (status error) {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	reStr := re.String()
	if _, ok := this.recovererMap[reStr]; ok {
		if recoverer == nil {
			delete(this.recovererMap, reStr)
			return nil
		}
		this.Errorf("recoverer for %s is already configured", reStr)
		return errs.ErrExist
	}

	this.regexpList = append(this.regexpList, re)
	this.recovererMap[reStr] = recoverer
	return nil
}

// Recover reads the current wal state from the file system and invokes the
// user defined callback if necessary. This function can be invoked only once,
// before doing any other updates
//
// recoverer: User defined recovery agent.
//
// Returns nil on success.
func (this *WriteAheadLog) Recover(recoverer wal.Recoverer) error {
	atomic.StoreInt32(&this.recovering, 1)
	defer atomic.StoreInt32(&this.recovering, 0)

	dir, errOpen := os.Open(this.dir)
	if errOpen != nil {
		this.Errorf("could not open directory %s: %v", this.dir, errOpen)
		return errOpen
	}
	defer dir.Close()

	var fileNames []string
	for {
		names, errRead := dir.Readdirnames(this.opts.MaxReadDirNames)
		if errRead == io.EOF {
			break
		}
		if errRead != nil {
			this.Errorf("could not read directory %s: %v", this.dir, errRead)
			return errRead
		}
		fileNames = append(fileNames, names...)
	}

	var changeFileList []int64
	checkpointFileID := int64(-1)
	for _, name := range fileNames {
		fileID := int64(-1)
		_, _ = fmt.Sscanf(name, this.name+".change.%d", &fileID)
		if fileID >= 0 {
			changeFileList = append(changeFileList, fileID)
			continue
		}

		_, _ = fmt.Sscanf(name, this.name+".checkpoint.%d", &fileID)
		if fileID >= 0 && fileID > checkpointFileID {
			checkpointFileID = fileID
			continue
		}
	}

	//
	// Recover checkpoint records followed by change records.
	//

	if checkpointFileID >= 0 {
		cbw := func(offset int64, header *walpb.RecordHeader, data []byte) error {
			var uid string
			if header.UserId != nil {
				uid = header.GetUserId()
				if recoverer := this.findRecoverer(uid); recoverer != nil {
					return recoverer.RecoverCheckpoint(uid, data)
				}
			}
			return recoverer.RecoverCheckpoint(uid, data)
		}

		filePath := fmt.Sprintf("%s/%s.checkpoint.%d", this.dir, this.name,
			checkpointFileID)
		if _, err := this.replay(filePath, 0, cbw); err != nil {
			this.Errorf("could not recover checkpoint records: %v", err)
			return err
		}
	}
	sort.Sort(Int64Slice(changeFileList))

	cbw := func(offset int64, header *walpb.RecordHeader, data []byte) error {
		var uid string
		if header.UserId != nil {
			uid = header.GetUserId()
			if recoverer := this.findRecoverer(uid); recoverer != nil {
				return recoverer.RecoverCheckpoint(uid, data)
			}
		}
		return recoverer.RecoverChange(offset, uid, data)
	}

	lastFileID, lastEndOffset := int64(-1), int64(-1)
	for _, fileID := range changeFileList {
		if lastFileID >= 0 && lastEndOffset >= 0 {
			if lastFileID+lastEndOffset != fileID {
				this.Errorf("change records are missing from offset %d to %d between "+
					"successive files %s.change.%d and %s.change.%d",
					lastFileID+lastEndOffset, fileID, this.name, lastFileID, this.name,
					fileID)
				return errs.ErrCorrupt
			}
		}

		fileOffset := int64(0)
		if checkpointFileID >= fileID {
			fileOffset = checkpointFileID - fileID
		}

		filePath := fmt.Sprintf("%s/%s.change.%d", this.dir, this.name, fileID)
		endOffset, errReplay := this.replay(filePath, fileOffset, cbw)
		if errReplay != nil {
			this.Errorf("could not recover change records: %v", errReplay)
			return errReplay
		}
		lastFileID, lastEndOffset = fileID, endOffset
	}

	this.changeFileList = changeFileList
	if lastFileID >= 0 && lastEndOffset >= 0 {
		this.nextChangeOffset = lastFileID + lastEndOffset
	}
	this.flushChangeOffset = this.nextChangeOffset
	this.Infof("wal is restored to offset %d", this.nextChangeOffset)
	return nil
}

// QueueChangeRecord appends a new change record into the wal. Record is not
// immediately written to the file system, but the LSN is guaranteed to be
// unique for that record. Records are written to the file system after a
// timeout or when any synchronous record is written after this record.
//
// uid: User id for the record.
//
// data: User data.
//
// Returns unique LSN representing the record.
func (this *WriteAheadLog) QueueChangeRecord(uid string, data []byte) wal.LSN {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	offset, _ := this.logChangeData(uid, data)
	return offset
}

// AppendChangeRecord appends a new change record into the wal. Record (and all
// records before it) are written to the kernel. Note that, writing to kernel
// doesn't ensure durability.
//
// uid: User id for the record.
//
// data: User data.
//
// On success, returns unique LSN representing the record. On a failure, record
// may not be written, so users should retry as necessary.
func (this *WriteAheadLog) AppendChangeRecord(uid string, data []byte) (
	lsn wal.LSN, status error) {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	offset, size := this.logChangeData(uid, data)
	defer func() {
		if status != nil {
			this.rmChangeEntry(offset, size)
		}
	}()

	if err := this.flushChanges(offset + int64(size)); err != nil {
		this.Errorf("could not flush change records: %v", err)
		return nil, err
	}
	return offset, nil
}

// SyncChangeRecord appends a new change record into the wal. Record (and all
// records before it) are written to the kernel and corresponding files are
// fdatasync-ed so that data becomes durable. Note that, disk caches may
// interfere with durability, so users should disable disk caches if any.
//
// uid: User id for the record.
//
// data: User data.
//
// On success, returns unique LSN representing the record. On a failure, record
// may not be written, so users should retry as necessary.
func (this *WriteAheadLog) SyncChangeRecord(uid string, data []byte) (
	lsn wal.LSN, status error) {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	offset, size := this.logChangeData(uid, data)
	defer func() {
		if status != nil {
			this.rmChangeEntry(offset, size)
		}
	}()

	endOffset := offset + int64(size)
	if err := this.syncChanges(endOffset); err != nil {
		this.Errorf("could not sync change records: %v", err)
		return nil, err
	}
	return offset, nil
}

// BeginCheckpoint starts a new checkpoint. Only one checkpoint can be active
// at a time.
//
// Returns nil on success. No new checkpoint is initiated on failures.
func (this *WriteAheadLog) BeginCheckpoint() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.beginCheckpointOffset >= 0 {
		this.Errorf("checkpoint with lsn %d is already active",
			this.beginCheckpointOffset)
		return errs.ErrInvalid
	}

	header := &walpb.RecordHeader{}
	header.Type = walpb.RecordHeader_BEGIN_CHECKPOINT.Enum()
	header.Checksum = this.checksum(nil)

	offset, _ := this.addChangeEntry(header, nil)
	this.beginCheckpointOffset = offset
	return nil
}

// EndCheckpoint commits or aborts the currently active checkpoint.
//
// commit: When true, indicates that checkpoint should be committed. When
//         false, indicates that checkpoint should be aborted.
//
// Returns nil on success. Errors are ignored when aborting a checkpoint, so
// abort always succeeds. Otherwise, users can retry committing the checkpoint
// as necessary.
func (this *WriteAheadLog) EndCheckpoint(commit bool) (status error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.beginCheckpointOffset < 0 {
		this.Errorf("no checkpoint is currently active")
		return errs.ErrInvalid
	}
	defer func() {
		if status == nil {
			this.beginCheckpointOffset = -1
			this.checkpointList = nil
		}
	}()

	if !commit {
		// Abort always succeeds.
		if err := this.abortCheckpoint(); err != nil {
			this.Warningf("abort checkpoint completed with status: %v", err)
		} else {
			this.Infof("checkpoint with lsn %d is aborted successfully",
				this.beginCheckpointOffset)
		}
		return nil
	}

	if err := this.syncChanges(this.nextChangeOffset); err != nil {
		this.Errorf("could not sync change records: %v", err)
		return err
	}

	if err := this.commitCheckpoint(); err != nil {
		this.Errorf("could not sync checkpoint records: %v", err)
		return err
	}

	this.Infof("checkpoint with lsn %d is committed", this.beginCheckpointOffset)
	return nil
}

// AppendCheckpointRecord appends a new checkpoint record. Since checkpoint
// state can be huge, keeping checkpoint records in memory is not wise. So,
// checkpoint records are always written the to the kernel, so that memory
// utilization will be low.
//
// uid: User id for the record.
//
// data: User data.
//
// Returns nil on success. On failure, no record is written to the checkpoint.
func (this *WriteAheadLog) AppendCheckpointRecord(uid string, data []byte) (
	status error) {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	header := &walpb.RecordHeader{}
	header.Type = walpb.RecordHeader_CHECKPOINT.Enum()
	header.Checksum = this.checksum(data)
	if len(uid) > 0 {
		header.UserId = proto.String(uid)
	}

	offset, size := this.addCheckpointEntry(header, data)
	defer func() {
		if status != nil {
			this.rmCheckpointEntry(offset, size)
		}
	}()

	// We want to batch as many records as possible into one write, so we release
	// the lock before pwritev so that other threads can queue their records.
	// This creates a problem on pwritev failure. If we want to return an error
	// to the caller, then offsets for the following records must be adjusted.

	endOffset := offset + int64(size)
	for this.flushCheckpointOffset < endOffset {
		// When we write a batch of records, a write error may not belong to
		// current thread's record, so it should not report an error to the caller.
		errFlush := this.flushCheckpoints(endOffset)
		if this.flushCheckpointOffset > endOffset {
			break
		}

		if errFlush != nil {
			this.Errorf("could not flush checkpoint record: %v", errFlush)
			return errFlush
		}
	}
	return nil
}

//
// Private functions
//

// findRecoverer returns the recoverer for a record based on its uid.
func (this *WriteAheadLog) findRecoverer(uid string) wal.Recoverer {
	for _, re := range this.regexpList {
		recoverer, found := this.recovererMap[re.String()]
		if !found {
			continue
		}
		if re.MatchString(uid) {
			return recoverer
		}
	}
	return nil
}

// syncChanges flushes all change records before a desired offset and issues
// fdatasync for all open files.
//
// endOffset: The desired wal offset up to which data must be synced.
//
// Returns nil on success. Users can retry on failures if necessary.
func (this *WriteAheadLog) syncChanges(endOffset int64) (
	status error) {

	// Flush all records before the desired offset.
	if err := this.flushChanges(endOffset); err != nil {
		this.Errorf("could not write change records: %v", err)
		return err
	}

	// Sync all open files before the desired offset.
	for _, fileID := range this.changeFileList {
		if fileID >= endOffset {
			break
		}

		if file := this.changeFileMap[fileID]; file != nil {
			if err := file.Sync(); err != nil {
				this.Errorf("could not sync change records file %d", fileID)
				return err
			}
		}
	}

	// Note: Since checkpoint, if any active, may not be committed, it is not
	// necessary to sync the checkpoints file.

	return nil
}

// flushChanges writes change records up to the desired offset to the kernel.
//
// endOffset: The desired wal offset up to which data must be written.
//
// Returns nil on success. Users may retry on failures as necessary.
func (this *WriteAheadLog) flushChanges(endOffset int64) (
	status error) {

	// Only one thread can write records at a time, so wait for your turn.
	for {
		if this.flushChangeOffset >= endOffset {
			return nil
		}
		if !this.writingChanges {
			break
		}
		this.writingChangesCond.Wait()
	}

	this.writingChanges = true
	defer func() {
		this.writingChanges = false
		this.writingChangesCond.Signal()
	}()

	for this.flushChangeOffset < endOffset {
		if err := this.writeChanges(); err != nil {
			this.Errorf("could not write change records: %v", err)
			return err
		}
	}
	return nil
}

// writeChanges batches as many change records as possible and writes them into
// change files in one pwritev operation. New change file will be created, if
// maximum file size is reached.
//
// Returns nil on success. Failures are ignored when non-zero number of bytes
// are written. So, this function fails only when no bytes could be written.
func (this *WriteAheadLog) writeChanges() (status error) {
	// Change records are always written to the last open file.
	lastFileID := int64(0)
	numChangeFiles := len(this.changeFileList)
	if numChangeFiles == 0 {
		this.changeFileList = append(this.changeFileList, 0)
	} else {
		lastFileID = this.changeFileList[numChangeFiles-1]
	}

	// Create change records file if it doesn't exist.
	lastFile, isOpen := this.changeFileMap[lastFileID]
	if !isOpen {
		filePath := fmt.Sprintf("%s/%s.change.%d", this.dir, this.name, lastFileID)
		file, errOpen := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR,
			this.opts.FileMode)
		if errOpen != nil {
			this.Errorf("could not create file %s for change records: %v", filePath,
				errOpen)
			return errOpen
		}
		lastFile = file
		this.changeFileMap[lastFileID] = file
		this.Infof("opened change file %d for writing", lastFileID)
	}
	currentFileSize := this.flushChangeOffset - lastFileID

	// Batch as many records as possible without breaking MaxFileSize and
	// MaxWriteSize limits.
	batchSize := 0
	fileSize := currentFileSize
	var bufferList [][]byte
	for _, entry := range this.changeList {
		entrySize := len(entry)
		if batchSize+entrySize > this.opts.MaxWriteSize {
			break
		}
		if fileSize+int64(entrySize) > this.opts.MaxFileSize {
			break
		}
		batchSize += entrySize
		fileSize += int64(entrySize)
		bufferList = append(bufferList, entry)
	}

	// Retry with next file, if no records could be written due to the max file
	// size limit.
	if fileSize == currentFileSize {
		this.Infof("reached maximum file size for change records file %d",
			this.flushChangeOffset)
		this.changeFileList = append(this.changeFileList, this.flushChangeOffset)
		return this.writeChanges()
	}

	// Unlock the mutex when performing pwritev so that other threads can add
	// more records while IO is in progress.
	this.mutex.Unlock()
	numWrote, errWrite := this.pwritev(lastFile,
		this.flushChangeOffset-lastFileID, bufferList)
	this.mutex.Lock()

	// Remove change records written in total.
	numEntries := 0
	for _, entry := range this.changeList {
		entrySize := len(entry)
		if numWrote >= int64(entrySize) {
			numEntries++
			numWrote -= int64(entrySize)
			this.flushChangeOffset += int64(entrySize)
		}
		break
	}

	// Remove completed change records from the change list.
	this.changeList = this.changeList[numEntries:]

	// Return an error only when no records could be written.
	if errWrite != nil {
		if numEntries == 0 {
			this.Errorf("could not write to change records file: %v", errWrite)
			return errWrite
		}
		this.Warningf("change record batch write operation truncated with status "+
			"(ignored): %v", errWrite)
	}
	return nil
}

// flushCheckpoints writes checkpoint records up to the desire offset to the
// kernel.
//
// endOffset: The desired checkpoint records offset up to which data must be
// written.
//
// Returns nil on success. Users may retry on failures as necessary.
func (this *WriteAheadLog) flushCheckpoints(endOffset int64) (
	status error) {

	// Only one thread can write records at a time, so wait for your turn.
	for {
		if this.checkpointFile != nil && this.flushCheckpointOffset >= endOffset {
			return nil
		}
		if !this.writingCheckpoints {
			break
		}
		this.writingCheckpointsCond.Wait()
	}

	this.writingCheckpoints = true
	defer func() {
		this.writingCheckpoints = false
		this.writingCheckpointsCond.Signal()
	}()

	for this.checkpointFile == nil || this.flushCheckpointOffset < endOffset {
		if err := this.writeCheckpoints(); err != nil {
			this.Errorf("could not write checkpoint records: %v", err)
			return err
		}
	}
	return nil
}

// writeCheckpoints batches as many checkpoint records as possible and writes
// them into checkpoint file in one pwritev operation.
//
// Returns nil on success. Failures are ignored when non-zero number of bytes
// are written. So, this function fails only when no bytes could be written.
func (this *WriteAheadLog) writeCheckpoints() error {
	// Create a checkpoint file if it doesn't exist.
	if this.checkpointFile == nil {
		filePath := fmt.Sprintf("%s/%s.checkpoint.temp", this.dir, this.name)
		file, errOpen := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC,
			this.opts.FileMode)
		if errOpen != nil {
			this.Errorf("could not open checkpoint file %s: %v", filePath, errOpen)
			return errOpen
		}
		this.checkpointFile = file
		this.flushCheckpointOffset = 0
	}

	// Batch as many checkpoint records as possible.  Also, make a copy of
	// checkpoint record list, so that more records can be added after we release
	// the mutex -- which helps in batching.
	batchSize := 0
	var bufferList [][]byte
	for _, entry := range this.checkpointList {
		entrySize := len(entry)
		if batchSize+entrySize > this.opts.MaxWriteSize {
			break
		}
		batchSize += entrySize
		bufferList = append(bufferList, entry)
	}

	// On an error, we don't want to remove this record because it requires
	// adjusting offsets for all the following records, instead we simply zero
	// the failed record, so that it's effect is null.
	this.mutex.Unlock()
	numWrote, errWrite := this.pwritev(this.checkpointFile,
		this.flushCheckpointOffset, bufferList)
	this.mutex.Lock()

	// Remove the entries written in total.
	numEntries := 0
	for _, entry := range this.checkpointList {
		entrySize := len(entry)
		if numWrote >= int64(entrySize) {
			numEntries++
			numWrote -= int64(entrySize)
			this.flushCheckpointOffset += int64(entrySize)
		}
		break
	}

	// Remove completed entries from the checkpoint list.
	this.checkpointList = this.checkpointList[numEntries:]

	// Return an error only when no records could be written.
	if errWrite != nil {
		if numEntries == 0 {
			this.Errorf("could not write to checkpoint file: %v", errWrite)
			return errWrite
		}
		this.Warningf("checkpoint write operation truncated with status "+
			"(ignored): %v", errWrite)
	}
	return nil
}

// abortCheckpoint cancels currently active checkpoint.
//
// Returns nil on success. Since partial abort may not always be undone, abort
// should be considered successful even after a failure.
func (this *WriteAheadLog) abortCheckpoint() (status error) {
	if this.checkpointFile == nil {
		return nil
	}

	if err := this.checkpointFile.Close(); err != nil {
		this.Errorf("could not close checkpoint file: %v", err)
		status = errs.MergeErrors(status, err)
	}
	this.checkpointFile = nil

	filePath := fmt.Sprintf("%s/%s.checkpoint.temp", this.dir, this.name)
	if err := os.Remove(filePath); err != nil {
		this.Errorf("could not remove temporary checkpoint file %s: %v", filePath,
			err)
		status = errs.MergeErrors(status, err)
	}
	return status
}

// commitCheckpoint finalizes currently active checkpoint.
//
// Returns nil on success. Users can retry operation on a failure.
func (this *WriteAheadLog) commitCheckpoint() (status error) {
	if err := this.flushCheckpoints(this.nextCheckpointOffset); err != nil {
		this.Errorf("could not write checkpoint records: %v", err)
		return err
	}

	if err := this.checkpointFile.Sync(); err != nil {
		this.Errorf("could not sync checkpoint file: %v", err)
		return err
	}

	// Rename the checkpoint file to its expected path.
	tempPath := fmt.Sprintf("%s/%s.checkpoint.temp", this.dir, this.name)
	finalPath := fmt.Sprintf("%s/%s.checkpoint.%d", this.dir, this.name,
		this.beginCheckpointOffset)
	if err := os.Rename(tempPath, finalPath); err != nil {
		this.Errorf("could not rename checkpoint file %s to %s: %v", tempPath,
			finalPath, err)
		return err
	}

	if err := this.checkpointFile.Close(); err != nil {
		this.Warningf("closed checkpoint file with status: %v", err)
	}
	this.checkpointFile = nil
	return nil
}

// logChangeData adds a new change record to the pending changes list.
//
// uid: User id for the record.
//
// data: User data.
//
// Returns wal-offset and number of bytes occupied by the change record.
func (this *WriteAheadLog) logChangeData(uid string, data []byte) (int64, int) {
	header := &walpb.RecordHeader{}
	header.Type = walpb.RecordHeader_CHANGE.Enum()
	header.Checksum = this.checksum(data)
	if len(uid) > 0 {
		header.UserId = proto.String(uid)
	}
	return this.addChangeEntry(header, data)
}

// prepareRecord creates a record from record header and user data.
//
// header: Record header.
//
// data: User data for the record. Data can be nil if necessary.
//
// Returns the raw bytes for the record.
func (this *WriteAheadLog) prepareRecord(header *walpb.RecordHeader,
	data []byte) []byte {

	dataSize := len(data)
	headerSize := proto.Size(header)

	recordSize := 8 + headerSize + dataSize
	recordBytes := make([]byte, recordSize)
	binary.BigEndian.PutUint32(recordBytes[0:4], uint32(headerSize))
	binary.BigEndian.PutUint32(recordBytes[4:8], uint32(dataSize))
	headerBytes, errMarshal := proto.Marshal(header)
	if errMarshal != nil {
		this.Fatalf("could not marshal record header proto: %v", errMarshal)
	}
	copy(recordBytes[8:8+headerSize], headerBytes)
	copy(recordBytes[8+headerSize:recordSize], data)
	return recordBytes
}

func (this *WriteAheadLog) checksum(data []byte) []byte {
	return []byte("") // TODO: Implement an user-configurable checksum mechanism.
}

// zeroRecord converts a record into an empty record without effecting the
// offsets for the following records.
//
// recordBytes: raw bytes for the record.
func (this *WriteAheadLog) zeroRecord(recordBytes []byte) {
	// An empty record is encoded with a zero header size.
	binary.BigEndian.PutUint32(recordBytes[0:4], 0)
	binary.BigEndian.PutUint32(recordBytes[4:8], uint32(len(recordBytes)-8))
}

// findEntry returns the index of record starting at 'findOffset' in the
// 'recordList' assuming the first record starts at 'startOffset'
//
// Returns the desired record index. Returns -1 if desired offset does not
// exist in the 'recordList'.
func findEntry(recordList [][]byte, startOffset, findOffset int64) int {
	// Find the record in checkpoint list that starts at offset and convert it
	// into an empty record.
	index := -1
	entryOffset := startOffset
	for ii, entry := range recordList {
		if findOffset == entryOffset {
			index = ii
			break
		}
		entryOffset += int64(len(entry))
	}
	return index
}

// pwritev writes multiple buffers in one operation. Returns number of bytes
// written and any error happened during the write.
func (this *WriteAheadLog) pwritev(file *os.File, offset int64,
	bufferList [][]byte) (int64, error) {

	totalWrote := 0
	for _, buffer := range bufferList {
		numWrote, errWrite := file.WriteAt(buffer, offset)
		totalWrote += numWrote
		offset += int64(totalWrote)

		if errWrite != nil {
			this.Errorf("could not write to file at offset %d: %v", offset,
				errWrite)
			return int64(totalWrote), errWrite
		}
	}
	return int64(totalWrote), nil
}

// addChangeEntry adds a new change record to the change list from record
// header and data.
//
// header: Record header.
//
// data: User data.
//
// Returns wal offset and size (in bytes) for the new change record.
func (this *WriteAheadLog) addChangeEntry(header *walpb.RecordHeader,
	data []byte) (int64, int) {

	recordBytes := this.prepareRecord(header, data)
	recordSize := len(recordBytes)

	offset := this.nextChangeOffset
	this.changeList = append(this.changeList, recordBytes)
	this.nextChangeOffset += int64(recordSize)
	return offset, recordSize
}

// rmChangeEntry removes a change record without effecting the offsets of
// following records queued in the change list.
//
// offset: Wal offset of the change record.
//
// size: Size of the record in bytes.
func (this *WriteAheadLog) rmChangeEntry(offset int64, size int) {
	index := findEntry(this.changeList, this.flushChangeOffset, offset)
	if index == -1 {
		this.Fatalf("could not find change record with offset %d", offset)
	}

	recordBytes := this.changeList[index]
	recordSize := len(recordBytes)
	if recordSize != size {
		this.Fatalf("change record size at offset %d is %d, but %d expected",
			offset, recordSize, size)
	}
	this.zeroRecord(recordBytes)
}

// addCheckpointEntry adds a checkpoint record to the checkpoint list from
// record header and data.
//
// header: Record header.
//
// data: User data.
//
// Returns offet and size (in bytes) for the new checkpoint record.
func (this *WriteAheadLog) addCheckpointEntry(
	header *walpb.RecordHeader, data []byte) (int64, int) {

	recordBytes := this.prepareRecord(header, data)
	recordSize := len(recordBytes)

	offset := this.nextCheckpointOffset
	this.checkpointList = append(this.checkpointList, recordBytes)
	this.nextCheckpointOffset += int64(recordSize)
	return offset, recordSize
}

// rmCheckpointEntry removes a checkpoint record without effecting the offsets
// of following records queued in the checkpoint list.
//
// offset: Offset of the checkpoint record.
//
// size: Size of the record in bytes.
func (this *WriteAheadLog) rmCheckpointEntry(offset int64, size int) {
	index := findEntry(this.changeList, this.flushChangeOffset, offset)
	if index == -1 {
		this.Fatalf("could not find checkpoint entry with offset %d", offset)
	}
	recordBytes := this.checkpointList[index]
	recordSize := len(recordBytes)
	if recordSize != size {
		this.Fatalf("checkpoint record size at offset %d is %d, but %d expected",
			offset, recordSize, size)
	}
	this.zeroRecord(recordBytes)
}

// replay reads records from a file after a given offset and invokes the
// callback function for every record with user data.
//
// filePath: Name of the records file.
//
// fileOffset: Starting record offset for the user callback. Offset must match
//             with the beginning of the record.
//
// cb: User callback. It gets file offset, total size and user data from every
//     record.
//
// Returns the offset for the end-of last record on success. Returns -1 if file
// doesn't have desired offset. Returns non-nil error if records couldn't be
// read or when the user callback returns a non-nil status for any record.
func (this *WriteAheadLog) replay(filePath string, fileOffset int64,
	cb func(int64, *walpb.RecordHeader, []byte) error) (int64, error) {

	file, errOpen := os.Open(filePath)
	if errOpen != nil {
		this.Errorf("could not open file %s for reading: %v", filePath, errOpen)
		return -1, errOpen
	}
	defer file.Close()

	stat, errStat := file.Stat()
	if errStat != nil {
		this.Errorf("could not stat file %s: %v", filePath, errStat)
		return -1, errStat
	}

	fileSize := stat.Size()
	if fileOffset > fileSize {
		// This file doesn't have the desired starting record.
		return -1, nil
	}

	if fileOffset > 0 {
		if _, err := file.Seek(fileOffset, 0 /* whence */); err != nil {
			this.Errorf("could not seek to offset %d: %v", fileOffset, err)
			return -1, err
		}
	}

	nextOffset := fileOffset
	reader := bufio.NewReaderSize(file, this.opts.MaxReadSize)
	for {
		offset := nextOffset
		sizeBytes, errPeek := reader.Peek(8)
		if errPeek != nil {
			if errPeek == io.EOF {
				if len(sizeBytes) > 0 {
					this.Warningf("wal records file %s has extra bytes %v at the end",
						filePath, sizeBytes)
				}
				return offset, nil
			}
			this.Errorf("could not peek in file %s for header and data sizes: %v",
				filePath, errPeek)
			return -1, errPeek
		}
		headerSize := binary.BigEndian.Uint32(sizeBytes[0:4])
		dataSize := binary.BigEndian.Uint32(sizeBytes[4:8])

		recordSize := 8 + headerSize + dataSize
		if recordSize <= 8 {
			this.Errorf("unexpected record with invalid size %d in %s at offset %d",
				recordSize, filePath, offset)
			return -1, errs.ErrCorrupt
		}

		recordBytes := make([]byte, recordSize)
		if _, err := io.ReadFull(reader, recordBytes); err != nil {
			if err == io.EOF {
				this.Warningf("wal records file %s has partial record %v at the end",
					filePath, recordBytes)
				return offset, nil
			}
			this.Errorf("could not read from file %s at offset %d: %v", filePath,
				offset, err)
			return -1, err
		}
		nextOffset = offset + int64(recordSize)

		if headerSize == 0 {
			this.Infof("found a zeroed-record at offset %d", offset)
			// This case represents a zero-ed record, so there is no record header or
			// data.
			continue
		}

		headerBytes := recordBytes[8 : 8+headerSize]
		dataBytes := recordBytes[8+headerSize : recordSize]

		// Unmarshal the record header and verify that data checksum is correct.
		var header walpb.RecordHeader
		if err := proto.Unmarshal(headerBytes, &header); err != nil {
			this.Errorf("could not parse wal record header of %d bytes at "+
				"offset %d in %s: %v", headerSize, offset, filePath, err)
			return -1, err
		}

		// Verify the checksum.
		checksum := this.checksum(dataBytes)
		if bytes.Compare(checksum, header.GetChecksum()) != 0 {
			this.Errorf("checksum mismatch for record at offset %s in file %s",
				offset, filePath)
			return -1, errs.ErrCorrupt
		}

		dataType := header.GetType()
		if dataType == walpb.RecordHeader_CHANGE ||
			dataType == walpb.RecordHeader_CHECKPOINT {

			if err := cb(offset, &header, dataBytes); err != nil {
				this.Errorf("wal recovery callback for record %d failed with "+
					"status: %v", offset, err)
				return -1, err
			}
		}
	}
}
