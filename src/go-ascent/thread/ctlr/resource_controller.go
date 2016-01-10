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
// This file defines ResourceController type, which implements resource locking
// with timeouts.
//
// THREAD SAFETY
//
// All public functions of ResourceController are thread-safe.
//
// NOTES
//
// ResourceContoller provides read/write locking for arbitrary resources based
// on unique names. It also provides an interface to lock all resources at
// once.
//
// List of resource names are separated into two groups at the first empty
// string as the resource name. Otherwise, all empty strings are ignored.
//
// Exclusive access is obtained for the first group of resources and shared
// access is obtained for the rest.
//
// USAGE
//
// ctlr ResourceController
// ...
// ctlr.Initialize(logger)
// ...
// lock := ctlr.LockResources("a", "b", "", "c", "d")
// defer lock.Unlock()
// ...
// lock := ctlr.LockAll()
// defer lock.Unlock()
//

package ctlr

import (
	"sync"
	"time"

	"go-ascent/base/log"
)

// ResourceLock represents a lock on a finite list of resources.
type ResourceLock struct {
	owner *ResourceController

	cond Condition

	readList  []string
	writeList []string
}

// FullLock represents a lock on all resources.
type FullLock struct {
	owner *ResourceController

	cond Condition
}

type anyLock interface {
	ready() bool
	issue()
	wakeup()
}

// ResourceController manages resource locks.
type ResourceController struct {
	log.Logger

	mutex sync.Mutex

	waitingList []anyLock

	lockAll bool

	sharingMap   map[string]int
	exclusiveMap map[string]bool
}

// Initialize initializes are ResourceController.
func (this *ResourceController) Initialize(logger log.Logger) {
	this.Logger = logger
	this.sharingMap = make(map[string]int)
	this.exclusiveMap = make(map[string]bool)
}

// LockAll obtains write lock on all resources.
func (this *ResourceController) LockAll() *FullLock {
	lock, _ := this.TimeLockAll(nil)
	return lock
}

// LockResources obtains read/write locks on the given resources. Resources are
// separated into write and read sets at the first empty resource id.
func (this *ResourceController) LockResources(
	resourceList ...string) *ResourceLock {

	lock, _ := this.TimeLockResources(nil, resourceList...)
	return lock
}

// TimeLockAll tries to obtain write lock on all resources in a given time
// limit.
func (this *ResourceController) TimeLockAll(timeoutCh <-chan time.Time) (
	*FullLock, error) {

	lock := &FullLock{owner: this}
	lock.cond.Initialize(&this.mutex)

	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.waitingList = append(this.waitingList, lock)
	defer this.remove(lock)

	for {
		if len(this.waitingList) == 1 {
			if lock.ready() {
				lock.issue()
				return lock, nil
			}
			if err := lock.cond.WaitTimeout(timeoutCh); err != nil {
				return nil, err
			}
			return lock, nil
		}
	}
}

// TimeLockResources tries to obtain the resource lock in a given time limit.
func (this *ResourceController) TimeLockResources(timeoutCh <-chan time.Time,
	resourceList ...string) (*ResourceLock, error) {

	// Split resources into readList and writeList based on separator.
	var writeList []string
	var readList []string
	separator := false
	for _, resource := range resourceList {
		if resource == "" {
			separator = true
			continue
		}
		if separator {
			readList = append(readList, resource)
		} else {
			writeList = append(writeList, resource)
		}
	}

	lock := &ResourceLock{owner: this}
	lock.cond.Initialize(&this.mutex)
	lock.readList = readList
	lock.writeList = writeList

	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.waitingList = append(this.waitingList, lock)
	defer this.remove(lock)

	for {
		if len(this.waitingList) == 1 {
			if lock.ready() {
				lock.issue()
				return lock, nil
			}
		}
		if err := lock.cond.WaitTimeout(timeoutCh); err != nil {
			return nil, err
		}
		return lock, nil
	}
}

// Unlock gives up lock on all or the specified list of resources. Locks must
// have been acquired earlier.
func (this *ResourceLock) Unlock(resourceList ...string) {
	this.owner.mutex.Lock()
	defer this.owner.mutex.Unlock()

	this.release(resourceList...)
	this.owner.scan()
}

// Unlock gives up full lock on all resources. Lock must have been acquired
// earlier.
func (this *FullLock) Unlock() {
	this.owner.mutex.Lock()
	defer this.owner.mutex.Unlock()

	this.release()
	this.owner.scan()
}

///////////////////////////////////////////////////////////////////////////////

func (this *ResourceController) scan() {
	for _, lock := range this.waitingList {
		if lock.ready() {
			lock.issue()
			lock.wakeup()
			continue
		}
		return
	}
}

func (this *ResourceLock) wakeup() {
	this.cond.Signal()
}

func (this *ResourceLock) release(resourceList ...string) {
	for ii, read := range this.readList {
		if len(read) == 0 {
			continue
		}
		if resourceList == nil {
			this.owner.sharingMap[read]--
			this.readList[ii] = ""
			continue
		}
		for _, resource := range resourceList {
			if resource == read {
				this.owner.sharingMap[read]--
				this.readList[ii] = ""
			}
		}
	}

	for ii, write := range this.writeList {
		if len(write) == 0 {
			continue
		}
		if resourceList == nil {
			this.owner.exclusiveMap[write] = false
			this.writeList[ii] = ""
			continue
		}
		for _, resource := range resourceList {
			if resource == write {
				this.owner.exclusiveMap[write] = false
				this.writeList[ii] = ""
			}
		}
	}
}

func (this *ResourceLock) issue() {
	for _, read := range this.readList {
		this.owner.sharingMap[read]++
	}
	for _, write := range this.writeList {
		this.owner.exclusiveMap[write] = true
	}
}

func (this *ResourceLock) ready() bool {
	if this.owner.lockAll {
		return false
	}
	for _, read := range this.readList {
		if inuse, ok := this.owner.exclusiveMap[read]; ok && inuse {
			return false
		}
	}
	for _, write := range this.writeList {
		if count, ok := this.owner.sharingMap[write]; ok && count > 0 {
			return false
		}
		if inuse, ok := this.owner.exclusiveMap[write]; ok && inuse {
			return false
		}
	}
	return true
}

func (this *FullLock) release() {
	this.owner.lockAll = false
}

func (this *FullLock) issue() {
	this.owner.lockAll = true
}

func (this *FullLock) ready() bool {
	if this.owner.lockAll {
		return false
	}
	for _, count := range this.owner.sharingMap {
		if count > 0 {
			return false
		}
	}
	for _, inuse := range this.owner.exclusiveMap {
		if inuse {
			return false
		}
	}
	return true
}

func (this *FullLock) wakeup() {
	this.cond.Signal()
}

func (this *ResourceController) remove(lock anyLock) {
	numWaiting := len(this.waitingList)
	if numWaiting == 1 {
		if this.waitingList[0] != lock {
			panic("bad remove: lock is not in the list")
		}
		this.waitingList = nil
		return
	}

	if lock == this.waitingList[numWaiting-1] {
		this.waitingList = this.waitingList[:numWaiting-1]
		return
	}

	for index, entry := range this.waitingList {
		if entry == lock {
			this.waitingList[index] = this.waitingList[numWaiting-1]
			this.waitingList = this.waitingList[:numWaiting-1]
			return
		}
	}
}
