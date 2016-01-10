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
// This file defines SimpleError and SimpleErrorList types which implement
// helper functions for managing errors.
//

package errs

import (
	"fmt"
)

// SimpleError type implements serializable errors.
type SimpleError struct {
	Category string
	Message  *string
}

// Error implements the Go language's standard error interface.
func (this *SimpleError) Error() string {
	if this.Message == nil {
		return this.Category
	}
	return fmt.Sprintf("%s{%s}", this.Category, *this.Message)
}

func (this *SimpleError) newErrorf(format string,
	args ...interface{}) *SimpleError {

	message := fmt.Sprintf(format, args...)
	newErr := &SimpleError{
		Category: this.Category,
		Message:  &message,
	}
	return newErr
}

func (this *SimpleError) isSimilar(err error) bool {
	if x, ok := err.(*SimpleError); ok {
		return x.Category == this.Category
	}
	return false
}

// TODO: Add encoder and decoder functionality.
