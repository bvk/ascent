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
// This file defines client interface for this package.
//

package log

// Logger interface defines logging functions supported by this package.
//
// THREAD SAFETY
//
// All Logger methods are thread safe.
//
// NOTES
//
// Logger interface does not report any errors to the callers. This makes the
// client code simpler because it doesn't need to worry about handling log
// failures.
//
// On any form of failures, further operations on the Loggers become no-ops or
// panics the process. This choice is left to the log backend implementations.
//
// All messages are logged in the increasing timestamp order. Since system
// clock can go backward (due to NTP synchronization), users may sometimes find
// the log messages in the wrong timestamp order.
type Logger interface {
	// NewLogger creates a sub-logger with a new context.
	NewLogger(format string, args ...interface{}) Logger

	// Info functions log informative messages.
	Info(args ...interface{})
	Infof(format string, args ...interface{})

	// StdInfo functions are similar to Info, except that message is also written
	// to os.Stderr, either synchronously or asynchronously depending on the log
	// backend.
	StdInfo(args ...interface{})
	StdInfof(format string, args ...interface{})

	// Warning functions log warning messages.
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})

	// StdWarning functions are similar to Warning, except that message is also
	// written to os.Stderr, either synchronously or asynchronously depending on
	// the log backend.
	StdWarning(args ...interface{})
	StdWarningf(format string, args ...interface{})

	// Error functions log messages representing program errors. Error messages
	// are always logged to os.Stderr, either synchronously or asynchronously
	// depending on the log backend.
	Error(args ...interface{})
	Errorf(format string, args ...interface{})

	// Fatal functions log panic messages and kill the current process with a
	// panic. Fatal messages are also logged to os.Stderr. Since the current
	// process is killed, control never returns from this function.
	//
	// Fatal operations are always synchronous. Asynchronous backends would flush
	// all prior messages happened before the fatal.
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}
