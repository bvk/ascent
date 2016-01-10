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
// This file implements unit tests for BasicController type.
//

package ctlr

import (
	"testing"
	"time"

	"go-ascent/base/errs"
	"go-ascent/base/log"
)

func TestBasicController(test *testing.T) {
	filePath := "/tmp/test_basic_controller.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-basic-controller")
	logger.Infof("starting new controller test")

	controller := &BasicController{}
	controller.Initialize(logger)
	defer func() {
		if err := controller.Close(); err != nil {
			test.Errorf("could not close the controller: %v", err)
			return
		}

		lock, errLock := controller.LockAll()
		if !errs.IsClosed(errLock) {
			test.Errorf("controller issued lock %v after it is closed", lock)
			return
		}

		// Lock and Unlock work even after a Close. Safety is not expected.
		foobar := controller.ReadLock("foo", "bar")
		foobar.Unlock()
	}()

	lock1, errLock1 := controller.LockAll()
	if errLock1 != nil {
		test.Errorf("could not acquire lock1: %v", errLock1)
		return
	}

	lock2, errLock2 := controller.TimedLockAll(time.Millisecond)
	if !errs.IsTimeout(errLock2) {
		test.Errorf("second lock %v is issued while lock1 %v is active",
			lock2, lock1)
		return
	}
	lock1.Unlock()

	lock3, errLock3 := controller.TimedLock(time.Millisecond, "a")
	if errLock3 != nil {
		test.Errorf("could not acquire lock3: %v", errLock3)
		return
	}

	lock4, errLock4 := controller.TimedLock(time.Millisecond, "b")
	if errLock4 != nil {
		test.Errorf("could not acquire lock4: %v", errLock4)
		return
	}

	lock5, errLock5 := controller.TimedLockAll(time.Millisecond)
	if errLock5 == nil {
		test.Errorf("lock all lock %v issue while locks %v and %v are active",
			lock5, lock3, lock4)
		return
	}

	lock3.Unlock()
	lock4.Unlock()

	foo := controller.ReadLock("foo")
	bar := controller.ReadLock("bar")
	bar.Unlock("bar")
	foo.Unlock("foo")

	baz := controller.ReadLock("baz")
	baz.Unlock()

	test.Logf("returning")
}
