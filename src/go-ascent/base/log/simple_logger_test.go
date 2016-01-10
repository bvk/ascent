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
// A simple test case for SimpleLogger module.
//

package log

import (
	"os"
	"testing"
)

func TestSimpleLogger(t *testing.T) {
	filePath := "/tmp/simple_logger_test.log"
	simpleLog := SimpleFileLog{}
	if err := simpleLog.Initialize(filePath); err != nil {
		t.Errorf("could not initialize log backend: %v", err)
		return
	}
	defer func() {
		if err := os.Remove(filePath); err != nil {
			t.Errorf("could not remove log file %s: %v", filePath, err)
		}
	}()

	sub1 := simpleLog.NewLogger("prefix:%d", 1)
	sub1.StdInfo("a log message")
	sub1.StdInfof("int:%d char:%c string:%s error:%v", 10, 'x', "hello",
		os.ErrInvalid)

	sub1.Warningf("a log message")
	sub1.StdWarningf("int:%d char:%c string:%s error:%v", 10, 'x', "hello",
		os.ErrInvalid)

	sub2 := sub1.NewLogger("prefix:%d", 2)
	sub2.Errorf("a sub2 error message")
	sub1.Errorf("a sub1 error message")

	if err := simpleLog.Close(); err != nil {
		t.Errorf("could not close the log backend: %v", err)
	}

	// Logger operations now become no-ops because their backend is destroyed.
	sub2.Errorf("a sub2 error message")
	sub1.Errorf("a sub1 error message")
}
