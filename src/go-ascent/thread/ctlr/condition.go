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
// This file defines Condition data type, which implements condition variable
// functionality with timeouts.
//
// THREAD SAFETY
//
// This module is not thread-safe. User must hold the lock before performing
// options on the condition variable.
//
// NOTES
//
// Signal and Broadcast callers MUST hold the lock.
//

package ctlr

import (
	"sync"
	"time"

	"go-ascent/base/errs"
)

// Condition implements the condition variable.
type Condition struct {
	// The lock associated with the condition variable.
	locker sync.Locker

	// A channel to send signals to the waiters.
	signalCh chan struct{}

	// A channel to broadcast close operation
	closeCh chan struct{}
}

// Initialize initializes a condition variable.
func (this *Condition) Initialize(locker sync.Locker) {
	this.locker = locker
	this.signalCh = make(chan struct{}, 1)
	this.closeCh = make(chan struct{})
}

// Close unblocks all waiters with ErrClosed.
func (this *Condition) Close() error {
	select {
	case <-this.closeCh:
		return errs.ErrClosed
	default:
		close(this.closeCh)
	}
	return nil
}

// IsClosed returns true if condition variable is closed.
func (this *Condition) IsClosed() bool {
	select {
	case <-this.closeCh:
		return true
	default:
		return false
	}
}

// WaitTimeout blocks the caller for specified time for a signal. Caller is
// required to hold the lock before calling this function.
//
// timeoutCh: A timeout channel to signal timeout or close.
//
// Returns nil if wake up was due to a signal; returns ErrTimeout or ErrClosed
// otherwise.
func (this *Condition) WaitTimeout(timeoutCh <-chan time.Time) (status error) {
	signalCh := this.signalCh

	this.locker.Unlock()
	select {
	case <-this.closeCh:
		status = errs.ErrClosed
	case <-timeoutCh:
		status = errs.ErrTimeout
	case <-signalCh:
	}
	this.locker.Lock()

	return status
}

// Wait blocks the caller for a signal. Caller is required to hold the lock
// before calling this function.
//
// Returns non-nil error if wake up was due to a Close operation.
func (this *Condition) Wait() (status error) {
	signalCh := this.signalCh
	this.locker.Unlock()
	select {
	case <-this.closeCh:
		status = errs.ErrClosed
	case <-signalCh:
	}
	this.locker.Lock()
	return status
}

// Broadcast wakes up all waiters. Caller is required to hold the lock before
// calling this function.
func (this *Condition) Broadcast() {
	signalCh := this.signalCh
	this.signalCh = make(chan struct{}, 1)
	close(signalCh)
}

// Signal wakes up one waiter. Caller is required to hold the lock before
// calling this function.
func (this *Condition) Signal() {
	select {
	case this.signalCh <- struct{}{}:
	default:
	}
}
