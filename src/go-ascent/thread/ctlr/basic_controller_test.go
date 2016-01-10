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

		token, errToken := controller.NewToken("any", nil /* timeout */)
		if !errs.IsClosed(errToken) {
			test.Errorf("controller issued token %v after it is closed", token)
			return
		}

		// Lock and Unlock work even after a Close. Safety is not expected.
		foobar := controller.ReadLock("foo", "bar")
		foobar.Unlock()
	}()

	token1, errToken1 := controller.NewToken("foo", nil)
	if errToken1 != nil {
		test.Errorf("could not acquire token1: %v", errToken1)
		return
	}
	token2, errToken2 := controller.NewToken("bar", time.After(time.Millisecond))
	if !errs.IsTimeout(errToken2) {
		test.Errorf("second token %v is issued while token1 %v is active",
			token2, token1)
		return
	}
	controller.CloseToken(token1)

	token3, errToken3 := controller.NewToken("baz", time.After(time.Millisecond),
		"a")
	if errToken3 != nil {
		test.Errorf("could not acquire token3: %v", errToken3)
		return
	}

	token4, errToken4 := controller.NewToken("foo", time.After(time.Millisecond),
		"b")
	if errToken4 != nil {
		test.Errorf("could not acquire token4: %v", errToken4)
		return
	}

	token5, errToken5 := controller.NewToken("bar", time.After(time.Millisecond))
	if errToken5 == nil {
		test.Errorf("lock all token %v issue while tokens %v and %v are active",
			token5, token3, token4)
		return
	}

	controller.CloseToken(token3)
	controller.CloseToken(token4)

	foo := controller.ReadLock("foo")
	bar := controller.ReadLock("bar")
	bar.Unlock("bar")
	foo.Unlock("foo")

	baz := controller.ReadLock("baz")
	baz.Unlock()

	test.Logf("returning")
}
