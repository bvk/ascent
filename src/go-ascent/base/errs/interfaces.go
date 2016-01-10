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

package errs

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	thispb "proto-ascent/base/errs"
)

// Errors interface defines pre-defined errors. They also serve as error
// categories when users choose to create errors with custom messages if
// necessary.
//
// All errors created by this package are serializable, so they can be included
// in network messages.
var (
	ErrInvalid  = &SimpleError{Category: "ErrInvalid"}
	ErrExist    = &SimpleError{Category: "ErrExist"}
	ErrNotExist = &SimpleError{Category: "ErrNotExist"}
	ErrRetry    = &SimpleError{Category: "ErrRetry"}
	ErrIOError  = &SimpleError{Category: "ErrIOError"}
	ErrTimeout  = &SimpleError{Category: "ErrTimeout"}
	ErrClosed   = &SimpleError{Category: "ErrClosed"}
	ErrOverflow = &SimpleError{Category: "ErrOverflow"}
	ErrCorrupt  = &SimpleError{Category: "ErrCorrupt"}

	// If necessary, add new errors above and define one or more Is* functions as
	// necessary.
)

// Is* functions check if an error object belongs to an error category.
func IsInvalid(err error) bool  { return ErrInvalid.isSimilar(err) }
func IsExist(err error) bool    { return ErrExist.isSimilar(err) }
func IsNotExist(err error) bool { return ErrNotExist.isSimilar(err) }
func IsRetry(err error) bool    { return ErrRetry.isSimilar(err) }
func IsIOError(err error) bool  { return ErrIOError.isSimilar(err) }
func IsTimeout(err error) bool  { return ErrTimeout.isSimilar(err) }
func IsClosed(err error) bool   { return ErrClosed.isSimilar(err) }
func IsOverflow(err error) bool { return ErrOverflow.isSimilar(err) }
func IsCorrupt(err error) bool  { return ErrCorrupt.isSimilar(err) }

// MakeErrorFromCategory creates an error object from the name of error
// category.
func MakeErrorFromCategory(category string) *SimpleError {
	switch category {
	case "":
		return nil
	case "ErrInvalid":
		return ErrInvalid
	case "ErrExist":
		return ErrExist
	case "ErrNotExist":
		return ErrNotExist
	case "ErrRetry":
		return ErrRetry
	case "ErrIOError":
		return ErrIOError
	case "ErrTimeout":
		return ErrTimeout
	case "ErrClosed":
		return ErrClosed
	case "ErrOverflow":
		return ErrOverflow
	case "ErrCorrupt":
		return ErrCorrupt
	default:
		panic(fmt.Sprint("unknown error category: %s", category))
	}
}

// New* functions create an error of a specific category with user defined
// message.
func NewErrInvalid(format string, args ...interface{}) error {
	return ErrInvalid.newErrorf(format, args...)
}
func NewErrExist(format string, args ...interface{}) error {
	return ErrExist.newErrorf(format, args...)
}
func NewErrNotExist(format string, args ...interface{}) error {
	return ErrNotExist.newErrorf(format, args...)
}
func NewErrRetry(format string, args ...interface{}) error {
	return ErrRetry.newErrorf(format, args...)
}
func NewErrIOError(format string, args ...interface{}) error {
	return ErrIOError.newErrorf(format, args...)
}
func NewErrTimeout(format string, args ...interface{}) error {
	return ErrTimeout.newErrorf(format, args...)
}
func NewErrClosed(format string, args ...interface{}) error {
	return ErrClosed.newErrorf(format, args...)
}
func NewErrOverflow(format string, args ...interface{}) error {
	return ErrOverflow.newErrorf(format, args...)
}
func NewErrCorrupt(format string, args ...interface{}) error {
	return ErrCorrupt.newErrorf(format, args...)
}

// NewErrorf creates an error of pre-defined error category with an
// user-defined error message.
func NewErrorf(category *SimpleError, format string,
	args ...interface{}) error {

	return category.newErrorf(format, args...)
}

// MergeErrors collects multiple errors into a single error. First error
// dominates other errors when a merged error is used in IsInvalid, IsExist,
// etc. functions.
func MergeErrors(errFirst error, rest ...error) error {
	return NewErrorList(errFirst, rest...)
}

// MakeErrorFromProto creates an error object from its protobuf representation.
func MakeErrorFromProto(errProto *thispb.Error) error {
	if errProto.Category == nil {
		if errProto.Message == nil {
			return nil
		}
		return fmt.Errorf("%s", *errProto.Message)
	}
	errCategory := MakeErrorFromCategory(*errProto.Category)
	if errProto.Message == nil {
		return errCategory
	}
	return errCategory.newErrorf("%s", *errProto.Message)
}

// MakeProtoFromError converts an error object into its protobuf
// representation.
func MakeProtoFromError(err error) *thispb.Error {
	if err == nil {
		return nil
	}
	if simple, ok := err.(*SimpleError); ok {
		return simple.toProto()
	}
	if errList, ok := err.(*ErrorList); ok {
		return errList.toProto()
	}
	errProto := &thispb.Error{}
	errProto.Message = proto.String(err.Error())
	return errProto
}

// AreEqual test if two errors are the same.
func AreEqual(errOne, errTwo error) bool {
	return errOne.Error() == errTwo.Error()
}
