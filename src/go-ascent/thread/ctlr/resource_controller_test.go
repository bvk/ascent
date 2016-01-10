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
// This file implements unit tests for ResourceController type.
//

package ctlr

import (
	"testing"
	"time"

	"go-ascent/base/log"
)

func TestResourceController(test *testing.T) {
	filePath := "/tmp/test_resource_controller.log"
	simpleLog := log.SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		test.Fatalf("could not initialize log backend: %v", err)
		return
	}
	logger := simpleLog.NewLogger("test-resource-controller")
	logger.Infof("starting new test")

	controller := &ResourceController{}
	controller.Initialize(logger)

	foo := controller.LockResources("foo")
	bar := controller.LockResources("bar")

	if xx := controller.TimeLockResources(time.Millisecond, "foo"); xx != nil {
		test.Errorf("resource foo is issued multiple times")
		return
	}

	if xx := controller.TimeLockAll(time.Millisecond); xx != nil {
		test.Errorf("lock all issued when foo and bar are busy")
		return
	}

	bar.Unlock("bar")
	foo.Unlock("foo")

	all := controller.LockAll()
	all.Unlock()

	baz := controller.LockResources("baz")
	baz.Unlock()

	baz2 := controller.LockResources("baz")
	baz2.Unlock()

	// Allow multiple readers on a resource.

	a1 := controller.LockResources("", "aa")
	a2 := controller.LockResources("", "aa")
	if a3 := controller.TimeLockAll(time.Millisecond); a3 != nil {
		test.Errorf("lock all issued when two readers are sharing aa")
		return
	}
	if a4 := controller.TimeLockResources(time.Millisecond, "aa"); a4 != nil {
		test.Errorf("exclusive lock is issued when two readers have aa")
		return
	}
	a1.Unlock()
	a2.Unlock()
}
