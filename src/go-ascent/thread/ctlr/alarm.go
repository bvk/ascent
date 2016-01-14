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
// This file implements Alarm type which can be used to schedule functions to
// run at a later time.
//
// THREAD SAFETY
//
// All public functions are thread-safe.
//

package ctlr

import (
	"sort"
	"sync"
	"time"

	"go-ascent/base/errs"
)

// Alarm type implements the alarm scheduler.
type Alarm struct {
	// WaitGroup to wait for the go routine to complete.
	wg sync.WaitGroup

	// Mutex to protect access to the alarm state.
	mutex sync.Mutex

	// Condition variable used to wait for the timeouts to complete.
	cond Condition

	// Sorted list of timestamps where jobs are pending.
	timestampList []time.Time

	// Mapping of timestamps to the list of job ids to run at that time.
	pendingMap map[time.Time][]string

	// Mapping from job id to the alarm handler function.
	jobMap map[string]func() error
}

// Initialize initializes the alarm object.
func (this *Alarm) Initialize() {
	this.pendingMap = make(map[time.Time][]string)
	this.jobMap = make(map[string]func() error)
	this.cond.Initialize(&this.mutex)
	this.wg.Add(1)
	go this.goSchedule()
}

// Close destroys the alarm object. It waits for all live alarms to complete.
func (this *Alarm) Close() error {
	if err := this.cond.Close(); err != nil {
		return err
	}
	this.wg.Wait()
	return nil
}

// ScheduleAt adds a new function to run at the specified time.
//
// uid: An unique id for the alarm.
//
// time: The timestamp when alarm handler should be invoked.
//
// fn: The alarm handler function.
//
// Returns nil if there is a pending alarm already with the same uid.
func (this *Alarm) ScheduleAt(uid string, time time.Time,
	fn func() error) error {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.cond.IsClosed() {
		return errs.ErrClosed
	}

	if _, ok := this.jobMap[uid]; ok {
		return errs.ErrExist
	}

	this.jobMap[uid] = fn
	pendingList, found := this.pendingMap[time]
	this.pendingMap[time] = append(pendingList, uid)
	if !found {
		this.timestampList = append(this.timestampList, time)
		if len(this.timestampList) > 1 {
			sort.Sort(TimeSlice(this.timestampList))
		}
	}

	this.cond.Signal()
	return nil
}

// goSchedule waits for timeouts to complete and runs the alarms.
func (this *Alarm) goSchedule() {
	defer this.wg.Done()

	this.mutex.Lock()
	defer this.mutex.Unlock()

	for {
		for len(this.timestampList) == 0 {
			if err := this.cond.Wait(); errs.IsClosed(err) {
				return
			}
		}

		now := time.Now()
		timestamp := this.timestampList[0]
		if now.After(timestamp) {
			pendingList := this.pendingMap[timestamp]

			delete(this.pendingMap, timestamp)
			numTimestamps := len(this.timestampList)
			for ii := 1; ii < numTimestamps; ii++ {
				this.timestampList[ii-1] = this.timestampList[ii]
			}
			this.timestampList = this.timestampList[:numTimestamps-1]

			jobList := make([]func() error, len(pendingList))
			for ii, uid := range pendingList {
				jobList[ii] = this.jobMap[uid]
				delete(this.jobMap, uid)
			}

			this.mutex.Unlock()
			for _, fn := range jobList {
				fn()
			}
			this.mutex.Lock()
			continue
		}

		timeout := timestamp.Sub(now)
		timeoutCh := time.After(timeout)
		if err := this.cond.WaitTimeout(timeoutCh); errs.IsClosed(err) {
			return
		}
	}
}
