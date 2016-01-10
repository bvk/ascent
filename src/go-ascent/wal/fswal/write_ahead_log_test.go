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
// This file contains unit tests for WriteAheadLog type.
//

package fswal

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"go-ascent/base/log"
	"go-ascent/wal"
)

var userSeed = flag.Int64("user_seed", 0,
	"Seed for the random input stream used as the data for the wal records.")

var maxRecordSize = flag.Int("max_record_size", 3*1024,
	"Maximum wal record size.")

var numTestRecords = flag.Int("num_wal_records", 1024,
	"Number of wal records to append as part of the test.")

type dataStream struct {
	max    int
	source rand.Source
	rand   *rand.Rand
}

func (this *dataStream) Initialize(seed int64, max int) {
	this.max = max
	this.source = rand.NewSource(seed)
	this.rand = rand.New(this.source)
}

func (this *dataStream) RandomString() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	size := this.rand.Intn(this.max)
	data := make([]byte, size)
	for ii := 0; ii < size; ii++ {
		data[ii] = chars[this.rand.Intn(len(chars))]
	}
	return string(data)
}

func TestDataStream(test *testing.T) {
	seed := time.Now().UnixNano()
	if *userSeed != 0 {
		seed = *userSeed
	}

	var inputStream dataStream
	inputStream.Initialize(seed, *maxRecordSize)

	var checkStream dataStream
	checkStream.Initialize(seed, *maxRecordSize)

	for ii := 0; ii < 1024; ii++ {
		input := inputStream.RandomString()
		check := checkStream.RandomString()
		if input != check {
			test.Fatalf("%d: input string %s != %s check string", ii, input, check)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////

type walState struct {
	log.Logger

	checkpointList []string
	changeList     []string
}

func (this *walState) RecoverCheckpoint(data []byte) error {
	this.checkpointList = append(this.checkpointList, string(data))
	return nil
}

func (this *walState) RecoverChange(lsn wal.LSN, data []byte) error {
	this.changeList = append(this.changeList, string(data))
	return nil
}

///////////////////////////////////////////////////////////////////////////////

func TestRepeatRecoverAppendChanges(test *testing.T) {
	filePath := "/tmp/test_repeat_recover_append_changes.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-repeat-recover-append")

	tmpDir, errTemp := ioutil.TempDir("", "TestWALChanges")
	if errTemp != nil {
		test.Fatalf("could not create temporary directory: %v", errTemp)
	}
	defer func() {
		if !test.Failed() {
			os.RemoveAll(tmpDir)
		}
	}()

	opts := &Options{}
	opts.MaxReadSize = 4096
	opts.MaxWriteSize = 4096
	opts.MaxReadDirNames = 1024
	opts.MaxFileSize = 1024 * 1024
	opts.FileMode = os.FileMode(0600)

	// Recovers current wal state and adds one change record.

	recoverANDappend := func(change string) []string {
		lwal := WriteAheadLog{Logger: logger}
		errInit := lwal.Initialize(opts, tmpDir, "test")
		if errInit != nil {
			test.Errorf("could not initialize local wal: %v", errInit)
			return nil
		}
		defer func() {
			if err := lwal.Close(); err != nil {
				test.Errorf("could not close the wal: %v", err)
			}
		}()

		state := walState{Logger: lwal}
		if err := lwal.Recover(&state); err != nil {
			test.Errorf("could not recover the wal: %v", err)
			return nil
		}
		test.Logf("restored %d checkpoints and %d changes",
			len(state.checkpointList), len(state.changeList))

		// Take a checkpoint for every 100 records -- this will merge changeList
		// into the checkpointList.
		if len(state.changeList)%100 == 0 {
			if err := lwal.BeginCheckpoint(); err != nil {
				test.Errorf("could not begin checkpoint: %v", err)
				return nil
			}
			for _, record := range state.checkpointList {
				if err := lwal.AppendCheckpointRecord([]byte(record)); err != nil {
					test.Errorf("could not append checkpoint record: %v", err)
					return nil
				}
			}
			for _, record := range state.changeList {
				if err := lwal.AppendCheckpointRecord([]byte(record)); err != nil {
					test.Errorf("could not append checkpoint record: %v", err)
					return nil
				}
			}
			if err := lwal.EndCheckpoint(true /* commit */); err != nil {
				test.Errorf("could not commit checkpoint: %v", err)
				return nil
			}
		}

		// After recovery, append one record.
		if _, err := lwal.SyncChangeRecord([]byte(change)); err != nil {
			test.Errorf("could not append change [%s]: %v", change, err)
			return nil
		}

		// Merge checkpoint records and change records to construct all records
		// recovered.
		recoveredList := state.checkpointList
		for _, record := range state.changeList {
			recoveredList = append(recoveredList, record)
		}
		return recoveredList
	}

	// Use a seeded random steam to create records with arbitrary data and verify
	// them against the same random steam for correctness.
	seed := time.Now().UnixNano()
	if *userSeed != 0 {
		seed = *userSeed
	}
	var inputStream dataStream
	inputStream.Initialize(seed, *maxRecordSize)

	// Log the seed so that we can use it to reproduce failures later.
	test.Logf("using seed %d", seed)

	var inputList []string
	for ii := 0; ii < *numTestRecords; ii++ {
		input := inputStream.RandomString()
		inputList = append(inputList, input)

		recoveredList := recoverANDappend(input)
		numRecovered := len(recoveredList)
		if numRecovered != ii {
			test.Errorf("recovery should've received %d entries, but got %d", ii,
				numRecovered)
			return
		}
		// Test that all records are in expected format.
		for jj, record := range recoveredList {
			if record != inputList[jj] {
				test.Errorf("change record %d recovered as [%s] which is unexpected",
					jj, record)
			}
		}
		test.Logf("test for %d wal records has passed", ii)
	}
}
