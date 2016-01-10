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
// This file defines ErrorList type which is used to collect multiple errors
// into one error object.
//

package errs

import (
	"fmt"
)

type ErrorList struct {
	errList []error
}

func NewErrorList(first error, rest ...error) *ErrorList {
	if first == nil {
		switch len(rest) {
		case 0:
			return nil
		case 1:
			first, rest = rest[0], nil
		default:
			first, rest = rest[0], rest[1:]
		}
	}

	if xx, ok := first.(*ErrorList); ok {
		xx.errList = append(xx.errList, rest...)
		return xx
	}

	var errList []error
	errList = append(errList, first)
	if rest != nil {
		errList = append(errList, rest...)
	}
	return &ErrorList{errList}
}

// FirstError returns the first error list of errors.
func (this *ErrorList) FirstError() error {
	return this.errList[0]
}

// Error implements the Go language's standard error interface.
func (this *ErrorList) Error() string {
	return fmt.Sprint(this.errList)
}

// TODO: Add encoder and decoder functionality.
