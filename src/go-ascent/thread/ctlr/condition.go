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
)

// Condition implements the condition variable.
type Condition struct {
	// The lock associated with the condition variable.
	locker sync.Locker

	// A channel to send signals to the waiters.
	signalCh chan struct{}
}

// Initialize initializes a condition variable.
func (this *Condition) Initialize(locker sync.Locker) {
	this.locker = locker
	this.signalCh = make(chan struct{}, 1)
}

// WaitTimeout blocks the caller for specified time for a signal. Caller is
// required to hold the lock before calling this function.
//
// timeout: Maximum amount of time to wait for the signal.
//
// Returns true if wake up was due to a signal; returns false otherwise.
func (this *Condition) WaitTimeout(timeout time.Duration) bool {
	timeoutCh := time.After(timeout)
	signaled := true

	signalCh := this.signalCh
	this.locker.Unlock()
	select {
	case <-timeoutCh:
		signaled = false
	case <-signalCh:
	}
	this.locker.Lock()

	return signaled
}

// Wait blocks the caller for a signal. Caller is required to hold the lock
// before calling this function.
func (this *Condition) Wait() {
	signalCh := this.signalCh
	this.locker.Unlock()
	<-signalCh
	this.locker.Lock()
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
