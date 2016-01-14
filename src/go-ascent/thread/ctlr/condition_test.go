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

package ctlr

import (
	"sync"
	"testing"
	"time"

	"go-ascent/base/errs"
)

func TestCondition(test *testing.T) {
	mutex := sync.Mutex{}
	cond := &Condition{}
	cond.Initialize(&mutex)

	// A signal must wake up waiting condition.
	mutex.Lock()
	go func() {
		mutex.Lock()
		cond.Signal()
		mutex.Unlock()
	}()
	if err := cond.Wait(); err != nil {
		test.Errorf("condition woke up with a non-nil error: %v", err)
		return
	}
	mutex.Unlock()

	// A signal must wake up timed wait.
	mutex.Lock()
	go func() {
		mutex.Lock()
		cond.Signal()
		mutex.Unlock()
	}()
	if err := cond.WaitTimeout(time.After(time.Second)); err != nil {
		test.Errorf("timed wait woke up with a non-nil error on signal: %v", err)
		return
	}
	mutex.Unlock()

	// A broadcast must wake up all waiters.
	wg := sync.WaitGroup{}
	for ii := 0; ii < 10; ii++ {
		doneCh := make(chan bool)
		wg.Add(1)
		go func() {
			mutex.Lock()
			doneCh <- true
			if err := cond.Wait(); err != nil {
				test.Errorf("wait returned with unexpected status: %v", err)
			}
			mutex.Unlock()
			wg.Done()
		}()
		<-doneCh
	}
	mutex.Lock()
	cond.Broadcast()
	mutex.Unlock()
	wg.Wait()

	// A broadcast must wake up all timed waiters.
	for ii := 0; ii < 10; ii++ {
		doneCh := make(chan bool)
		wg.Add(1)
		go func() {
			mutex.Lock()
			doneCh <- true
			if err := cond.WaitTimeout(time.After(time.Minute)); err != nil {
				test.Errorf("timed wait returned with unexpected status: %v", err)
			}
			mutex.Unlock()
			wg.Done()
		}()
		<-doneCh
	}
	mutex.Lock()
	cond.Broadcast()
	mutex.Unlock()
	wg.Wait()

	// A timeout must wake up the condition.
	mutex.Lock()
	if err := cond.WaitTimeout(time.After(time.Second)); !errs.IsTimeout(err) {
		test.Errorf("timed wait woke up with unexpected status: %v", err)
		return
	}
	mutex.Unlock()

	// Closing a condition must wake up all timed waiters and normal waiters.
	for ii := 0; ii < 10; ii++ {
		doneCh := make(chan bool)
		wg.Add(1)
		go func() {
			mutex.Lock()
			doneCh <- true
			if err := cond.Wait(); !errs.IsClosed(err) {
				test.Errorf("wait returned with unexpected status: %v", err)
			}
			mutex.Unlock()
			wg.Done()
		}()
		<-doneCh
	}
	for ii := 0; ii < 10; ii++ {
		doneCh := make(chan bool)
		wg.Add(1)
		go func() {
			mutex.Lock()
			doneCh <- true
			errWait := cond.WaitTimeout(time.After(time.Minute))
			if !errs.IsClosed(errWait) {
				test.Errorf("timed wait returned with unexpected status: %v", errWait)
			}
			mutex.Unlock()
			wg.Done()
		}()
		<-doneCh
	}
	cond.Close()
	wg.Wait()
}
