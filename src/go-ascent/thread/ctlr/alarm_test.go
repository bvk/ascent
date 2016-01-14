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
	"testing"
	"time"
)

func TestAlarm(test *testing.T) {
	alarm := &Alarm{}
	alarm.Initialize()
	defer alarm.Close()

	doneCh := make(chan bool)
	fun := func() {
		test.Logf("alarm invoked at %v", time.Now())
		doneCh <- true
	}

	now := time.Now()
	test.Logf("installing alarms at %v to run after a second", now)
	timestamp := now.Add(time.Second)
	alarm.ScheduleAt(timestamp, fun)
	alarm.ScheduleAt(timestamp, fun)

	test.Logf("installing alarms at %v to run after two seconds", now)
	timestamp = now.Add(2 * time.Second)
	alarm.ScheduleAt(timestamp, fun)
	alarm.ScheduleAt(timestamp, fun)

	test.Logf("installing an alarm at %v to never run cause alarm will "+
		"be closed", now)
	alarm.ScheduleAt(now.Add(time.Minute), fun)

	<-doneCh
	<-doneCh
	<-doneCh
	<-doneCh
}
