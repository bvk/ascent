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
// This file defines BasicController type which implements admission control,
// resource synchronization, etc. features.
//
// THREAD SAFETY
//
// All public functions are thread-safe.
//
// NOTES
//
// BasicController objects provide a high-level abstraction for concurrency
// control in the form of tokens.
//
// Tokens are acquired for each intended operation along with shared or
// exclusive access to the resources necessary for that operation.
//
// BasicController is similar to ResourceController, with extra control over
// the operations. For example, tokens cannot be issued after the controller
// object is closed. This provides safe semantics over the object destruction
// for garbage collected environments.
//
// Also, users can lock resources for read-only access without acquiring the
// tokens. Such locks are allowed even after the controller is closed. Users
// can do read-only operations as long as a reference to the object exists --
// which is safe because a live reference will stop the garbage collection
// anyway.
//

package ctlr

import (
	"math"
	"sync"
	"time"

	"go-ascent/base/errs"
	"go-ascent/base/log"
)

// ClosingFullLock defines an all resource lock for the BasicController.
type ClosingFullLock struct {
	*FullLock
	owner *BasicController
}

// Unlock releases the lock.
func (this *ClosingFullLock) Unlock() {
	if this.FullLock != nil {
		this.FullLock.Unlock()
		this.FullLock = nil
		this.owner.wg.Done()
	}
}

// ClosingResourceLock defines a BasicController lock for limited number of
// resources.
type ClosingResourceLock struct {
	*ResourceLock
	owner *BasicController
}

// Unlock releases the lock.
func (this *ClosingResourceLock) Unlock(resourceList ...string) {
	if this.ResourceLock != nil {
		this.ResourceLock.Unlock(resourceList...)
		if resourceList == nil {
			this.ResourceLock = nil
			this.owner.wg.Done()
		}
	}
}

// BasicController implements admission control and resource synchronization.
type BasicController struct {
	log.Logger

	// Resource controller to manage resource synchronization.
	resourcer ResourceController

	// Mutex to protect access to the following variables.
	mutex sync.Mutex

	// Timeout control channels for full lock waiters.
	timerMap map[chan time.Time]struct{}

	// Wait group to wait for live operations to complete.
	wg sync.WaitGroup

	// Broadcast channel to signal closing the controller.
	closeCh chan struct{}
}

// Initialize initializes the controller.
func (this *BasicController) Initialize(logger log.Logger) {
	this.timerMap = make(map[chan time.Time]struct{})
	this.closeCh = make(chan struct{})
	this.resourcer.Initialize(logger)
	this.Logger = logger
}

// Close destroys waits for all live operations to finish and destroys the
// controller. No new tokens can be issued after this.
func (this *BasicController) Close() error {
	this.mutex.Lock()
	select {
	case <-this.closeCh:
		this.mutex.Unlock()
		return errs.ErrClosed

	default:
		close(this.closeCh)
	}

	for timerCh := range this.timerMap {
		close(timerCh)
	}
	this.timerMap = nil
	this.mutex.Unlock()

	this.wg.Wait()
	return nil
}

// IsClosed returns true if controller is closed and false otherwise.
func (this *BasicController) IsClosed() bool {
	select {
	case <-this.closeCh:
		return true
	default:
		return false
	}
}

// GetCloseChannel returns the channel that broadcasts close operation.
func (this *BasicController) GetCloseChannel() <-chan struct{} {
	return this.closeCh
}

// Lock acquires given resources for write access. It will fail with a non-nil
// error if controller is closed.
func (this *BasicController) Lock(first string, rest ...string) (
	*ClosingResourceLock, error) {

	return this.TimedLock(math.MaxInt64, first, rest...)
}

// LockAll locks all resources for write access. It will fail with a non-nil
// error if controller is closed.
func (this *BasicController) LockAll() (*ClosingFullLock, error) {
	return this.TimedLockAll(math.MaxInt64)
}

// TimedLock acquires exclusive lock on given resources within the given
// timeout. It will fail with a non-nil error if controller is closed.
func (this *BasicController) TimedLock(timeout time.Duration, first string,
	rest ...string) (*ClosingResourceLock, error) {

	this.mutex.Lock()
	if this.IsClosed() {
		this.mutex.Unlock()
		return nil, errs.ErrClosed
	}
	this.wg.Add(1)
	var timeoutCh <-chan time.Time
	if timeout == math.MaxInt64 {
		timerCh := make(chan time.Time)
		this.timerMap[timerCh] = struct{}{}
		timeoutCh = timerCh
	} else {
		timeoutCh = time.After(timeout)
	}
	this.mutex.Unlock()

	resourceList := []string{first}
	resourceList = append(resourceList, rest...)
	resLock, errLock := this.resourcer.TimeLockResources(timeoutCh,
		resourceList...)
	if errLock != nil {
		this.wg.Done()
		return nil, errLock
	}
	lock := &ClosingResourceLock{ResourceLock: resLock, owner: this}
	return lock, nil
}

// TimedLockAll acquires exclusive lock on all resources within the given
// timeout. It will fail with a non-nil error if controller is closed.
func (this *BasicController) TimedLockAll(timeout time.Duration) (
	*ClosingFullLock, error) {

	this.mutex.Lock()
	if this.IsClosed() {
		this.mutex.Unlock()
		return nil, errs.ErrClosed
	}
	this.wg.Add(1)
	var timeoutCh <-chan time.Time
	if timeout == math.MaxInt64 {
		timerCh := make(chan time.Time)
		this.timerMap[timerCh] = struct{}{}
		timeoutCh = timerCh
	} else {
		timeoutCh = time.After(timeout)
	}
	this.mutex.Unlock()

	fullLock, errLock := this.resourcer.TimeLockAll(timeoutCh)
	if errLock != nil {
		this.wg.Done()
		return nil, errLock
	}
	lock := &ClosingFullLock{FullLock: fullLock, owner: this}
	return lock, nil
}

// ReadLock acquires resources for read-only access. This method can be used
// even after the controller is closed.
func (this *BasicController) ReadLock(first string,
	rest ...string) *ResourceLock {

	readList := make([]string, len(rest)+2)
	readList[0] = ""
	readList[1] = first
	copy(readList[2:], rest)
	return this.resourcer.LockResources(readList...)
}

// ReadLockAll acquires all resources for read-only access. This method can be
// used even after the controller is closed.
func (this *BasicController) ReadLockAll() *FullLock {
	// TODO: Implement read/write full locks.
	return this.resourcer.LockAll()
}
