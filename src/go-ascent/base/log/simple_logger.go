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
// This file defines SimpleFileLog and SimpleLogger types which provide a
// simple implementation for the log package.
//
// SimpleFileLog type implements a single file based, thread-safe log backend.
// All messages are appended to the file synchronously (not O_SYNC terms).
//
// SimpleLogger type implements the Logger interface using the SimpleFileLog
// backend objects.
//
// THREAD SAFETY
//
// All SimpleLogger methods are thread-safe.
//
// NOTES
//
// Initialize() method on SimpleFileLog type is not thread safe because it is
// the constructor. It should be called to initialize SimpleFileLog objects
// before using any other methods.
//
// All SimpleLogger objects are tied to the root SimpleFileLog object which is
// their ancestor. Logger operations become no-ops when their corresponding log
// backend is destroyed.
//

package log

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

type SimpleFileLog struct {
	// Mutex for thread-safety.
	mutex sync.Mutex

	// Log file opened for writing.
	file *os.File
}

func (this *SimpleFileLog) Initialize(filePath string) error {
	flags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	file, errOpen := os.OpenFile(filePath, flags, os.FileMode(0600))
	if errOpen != nil {
		return errOpen
	}

	this.file = file
	return nil
}

func (this *SimpleFileLog) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.file != nil {
		this.file.Close()
		this.file = nil
	}
	return nil
}

func (this *SimpleFileLog) NewLogger(format string,
	args ...interface{}) *SimpleLogger {

	prefix := fmt.Sprintf(format, args...)

	this.mutex.Lock()
	defer this.mutex.Unlock()

	logger := &SimpleLogger{
		backend: this,
		prefix:  prefix,
	}
	return logger
}

// logMessage prepares and writes a message to the log file.
func (this *SimpleFileLog) logMessage(stderr, die bool, msgType rune,
	format, prefix string, args ...interface{}) {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	// Return silently if log backend is closed.
	if this.file == nil {
		return
	}

	// Find the log message location.
	_, filePath, line, _ := runtime.Caller(2)
	fileName := path.Base(filePath)

	timestamp := time.Now()
	year, month, day := timestamp.Date()
	hour, minute, second := timestamp.Clock()
	nanoseconds := timestamp.Nanosecond()

	// Prepare a buffer with the log message. TODO: Reuse buffers using a pool.
	buffer := &bytes.Buffer{}
	fmt.Fprintf(buffer,
		"%c%02d%02d%04d %02d:%02d:%02d.%09d %s:%d {%s} ",
		msgType, day, month, year, hour, minute, second, nanoseconds, fileName,
		line, prefix)
	messageStart := buffer.Len()
	hasNewline := true
	if len(format) > 0 {
		fmt.Fprintf(buffer, format, args...)
		hasNewline = (format[len(format)-1] == '\n')
	} else {
		fmt.Fprintln(buffer, args...)
	}
	if !hasNewline {
		buffer.WriteRune('\n')
	}
	logMessage := buffer.Bytes()
	userMessage := logMessage[messageStart:]

	// Write the log message to file and user message to stderr if necessary.
	this.file.Write(logMessage)
	if stderr {
		switch msgType {
		case FATAL:
			fmt.Fprintf(os.Stderr, "fatal:%s: %s", prefix, userMessage)
		case ERROR:
			fmt.Fprintf(os.Stderr, "error:%s: %s", prefix, userMessage)
		case WARNING:
			fmt.Fprintf(os.Stderr, "warning:%s: %s", prefix, userMessage)
		default:
			os.Stderr.Write(userMessage)
		}
	}

	// Kill the process if necessary.
	if die {
		panic(userMessage)
	}
}

///////////////////////////////////////////////////////////////////////////////

const (
	INFO    = 'I'
	WARNING = 'W'
	ERROR   = 'E'
	FATAL   = 'F'
)

type SimpleLogger struct {
	// Reference to the log backend.
	backend *SimpleFileLog

	// Context string prefixed to every log message.
	prefix string
}

func (this *SimpleLogger) NewLogger(format string,
	args ...interface{}) Logger {

	return this.backend.NewLogger(format, args...)
}

func (this *SimpleLogger) Info(args ...interface{}) {
	this.backend.logMessage(false, false, INFO, "", this.prefix, args...)
}

func (this *SimpleLogger) Infof(format string, args ...interface{}) {
	this.backend.logMessage(false, false, INFO, format, this.prefix, args...)
}

func (this *SimpleLogger) StdInfo(args ...interface{}) {
	this.backend.logMessage(true, false, INFO, "", this.prefix, args...)
}

func (this *SimpleLogger) StdInfof(format string, args ...interface{}) {
	this.backend.logMessage(true, false, INFO, format, this.prefix, args...)
}

func (this *SimpleLogger) Warning(args ...interface{}) {
	this.backend.logMessage(false, false, WARNING, "", this.prefix, args...)
}

func (this *SimpleLogger) Warningf(format string, args ...interface{}) {
	this.backend.logMessage(false, false, WARNING, format, this.prefix, args...)
}

func (this *SimpleLogger) StdWarning(args ...interface{}) {
	this.backend.logMessage(true, false, WARNING, "", this.prefix, args...)
}

func (this *SimpleLogger) StdWarningf(format string, args ...interface{}) {
	this.backend.logMessage(true, false, WARNING, format, this.prefix, args...)
}

func (this *SimpleLogger) Error(args ...interface{}) {
	this.backend.logMessage(true, false, ERROR, "", this.prefix, args...)
}

func (this *SimpleLogger) Errorf(format string, args ...interface{}) {
	this.backend.logMessage(true, false, ERROR, format, this.prefix, args...)
}

func (this *SimpleLogger) Fatal(args ...interface{}) {
	this.backend.logMessage(true, true, FATAL, "", this.prefix, args...)
}

func (this *SimpleLogger) Fatalf(format string, args ...interface{}) {
	this.backend.logMessage(true, true, FATAL, format, this.prefix, args...)
}
