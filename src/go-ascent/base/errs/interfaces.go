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
