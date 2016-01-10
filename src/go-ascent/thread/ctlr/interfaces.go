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
// This file defines Controller interface which implements admission control
// and resource synchronization.
//

package ctlr

import (
	"time"
)

// Token is a unique ticket issued when it is safe to perform an operation.
type Token interface {
}

// Controller implements admission control and resource access serialization
// using tokens.
type Controller interface {
	// Close destroys the controller. No new tokens will be issued after this
	// operation.
	Close() error

	// NewToken issues a token when all requested resources are available.
	// opName: Name of the operation asking for the token.
	//
	// timeout: When non-zero indicates the maximum time to block for token.
	//
	// resourceList: When non-nil indicates the list of resources to lock before
	//               issuing the token. A nil value indicates all resources must
	//               be locked, effectively serializing to one token at a time.
	//
	// Returns a non-nil token on success.
	NewToken(opName string, timeout time.Duration, resourceList ...string) (
		Token, error)

	// CloseToken releases a token issued previously. A closed-token can be
	// closed again -- which translates to a no-op.
	CloseToken(token Token)
}
