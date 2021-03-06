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
// Few simple test cases for errs package.
//

package errs

import (
	"os"
	"testing"
)

func TestSimpleError(test *testing.T) {
	if !IsInvalid(ErrInvalid) {
		test.Errorf("IsInvalid(ErrInvalid) is not true")
	}
	if !IsExist(ErrExist) {
		test.Errorf("IsExist(ErrExist) is not true")
	}
	if !IsNotExist(ErrNotExist) {
		test.Errorf("IsNotExist(ErrNotExist) is not true")
	}
	if !IsRetry(ErrRetry) {
		test.Errorf("IsRetry(ErrRetry) is not true")
	}
	if !IsIOError(ErrIOError) {
		test.Errorf("IsIOError(ErrIOError) is not true")
	}
	if !IsTimeout(ErrTimeout) {
		test.Errorf("IsTimeout(ErrTimeout) is not ture")
	}

	err1 := NewErrorf(ErrInvalid, "int %d rune %c string %s", 10, 'x', "message")
	if !IsInvalid(err1) {
		test.Errorf("custom error %v is not classified into proper category", err1)
	}
	if err1.Error() != "ErrInvalid{int 10 rune x string message}" {
		test.Errorf("custom error message for [%v] is in expected format", err1)
	}

	//
	// Protobuf conversion tests
	//

	errInvalidProto := MakeProtoFromError(ErrInvalid)
	if !IsInvalid(MakeErrorFromProto(errInvalidProto)) {
		test.Errorf("couldn't convert ErrInvalid into proto format")
	}

	errMerge := MergeErrors(ErrRetry, ErrTimeout)
	errRetryProto := MakeProtoFromError(errMerge)
	if !IsRetry(MakeErrorFromProto(errRetryProto)) {
		test.Errorf("couldn't convert ErrRetry merged with some other error " +
			"into proto format")
	}

	errOSErrorProto := MakeProtoFromError(os.ErrExist)
	osError := MakeErrorFromProto(errOSErrorProto)
	if !AreEqual(osError, os.ErrExist) {
		test.Errorf("could not convert os.ErrExist into proto format")
	}

	//
	// MergeErrors test
	//

	aa := MergeErrors(ErrInvalid, ErrExist, ErrNotExist)
	if !IsInvalid(aa) {
		test.Errorf("MergeErrors(ErrInvalid, ...) doesn't satisfy IsInvalid")
	}
}
