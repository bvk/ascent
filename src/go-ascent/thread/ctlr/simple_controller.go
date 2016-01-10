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
// This file defines Controller type which implements admission control, alarm
// handlers and synchronization features.
//
// THREAD SAFETY
//
// All public functions are thread safe.
//
// NOTES
//
// Controller objects provide a high-level abstraction for concurrency
// control, in the form of tokens.
//

package ctlr

import (
	"fmt"
	"sync"
	"time"

	"go-ascent/base/errs"
	"go-ascent/base/log"
)

type SimpleToken struct {
	*SimpleController

	// Name of the operation represented by this token.
	name string

	// Flag indicating if token was closed already.
	closed bool

	// List of resource names locked by this operation. A nil represents that all
	// resources are locked.
	resourceMap map[string]struct{}

	// Channels for coordinating with token manager go-routine.
	resultCh chan error
	cancelCh chan struct{}
}

type SimpleController struct {
	log.Logger

	// Wait group to wait for live operations to complete.
	sync.WaitGroup

	// A special token to implement Lock/Unlock operations.
	lockToken Token

	// Broadcast channel to signal closing the controller.
	closeCh chan struct{}

	// Channels to send new token and token close operations to the token
	// manager.
	newCh  chan *SimpleToken
	doneCh chan *SimpleToken

	// Channel to send a resource early release request, without closing the
	// corresponding token.
	releaseCh chan string

	// Flag to indicate that all resources are locked.
	lockAll bool

	// Mapping from resource name to its status.
	resourceMap map[string]bool

	// List of tokens waiting because one or more resources aren't available.
	waitingList []*SimpleToken
}

// Initialize initializes the controller.
func (this *SimpleController) Initialize() {
	this.doneCh = make(chan *SimpleToken)
	this.newCh = make(chan *SimpleToken)
	this.closeCh = make(chan struct{})
	this.resourceMap = make(map[string]bool)
	this.releaseCh = make(chan string)
	this.Logger = this.Logger.NewLogger("ctlr")

	this.Add(1)
	go this.goManageTokens()
}

// Close destroys waits for all live operations to finish and destroys the
// controller.
func (this *SimpleController) Close() error {
	select {
	case <-this.closeCh:
		return errs.ErrClosed

	default:
		close(this.closeCh)
	}
	return nil
}

// GetCloseChannel returns the channel that broadcasts close operation.
func (this *SimpleController) GetCloseChannel() <-chan struct{} {
	return this.closeCh
}

// NewToken requests the controller for a new token for given timeout.
//
// opName: Name of the operation asking for the token.
//
// timeout: When non-zero indicates the maximum time to block for token.
//
// resourceList: When non-nil indicates the list of resources to lock before
//               issuing the token. A nil value indicates all resources must be
//               locked, effectively serializing to one token at a time.
//
// Returns a non-nil token on success.
func (this *SimpleController) NewToken(opName string, timeout time.Duration,
	resourceList ...string) (next Token, status error) {

	// TODO: While this implementation is safe, it is very inefficient. Think of
	// a way to get timeouts working with sync.Cond variables and re-implement
	// this with plain mutexes.

	token := &SimpleToken{
		SimpleController: this,
		name:             opName,
		cancelCh:         make(chan struct{}),
		resultCh:         make(chan error),
		resourceMap:      make(map[string]struct{}),
	}
	defer func() {
		if status != nil {
			close(token.cancelCh)
		}
	}()

	for _, resource := range resourceList {
		token.resourceMap[resource] = struct{}{}
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	newCh := this.newCh
	for {
		select {
		case <-this.closeCh:
			return nil, errs.ErrClosed

		case <-timeoutCh:
			return nil, errs.ErrTimeout

		case newCh <- token:
			newCh = nil
			continue

		case status = <-token.resultCh:
			return token, status
		}
	}
}

func (this *SimpleController) ReleaseResource(tok Token, resource string) bool {
	token := tok.(*SimpleToken)
	if _, ok := token.resourceMap[resource]; ok {
		select {
		case <-this.closeCh:
			return false
		case this.releaseCh <- resource:
			delete(token.resourceMap, resource)
			return true
		}
	}
	return false
}

// CloseToken releases previously issued token.
func (this *SimpleController) CloseToken(tok Token) {
	token := tok.(*SimpleToken)
	// Just return if token was closed already.
	if token.closed {
		return
	}
	token.closed = true

	select {
	case <-this.closeCh:
		return
	case this.doneCh <- token:
		return
	}
}

// Lock locks all resources. This works even after closing the controller.
func (this *SimpleController) Lock(resourceList ...string) *SimpleToken {
	token, errToken := this.NewToken("lock", 0 /* timeout */, resourceList...)
	if errToken != nil {
		if errs.IsClosed(errToken) {
			// Since object was closed, lets assume all accesses are read-only.
			resourceMap := make(map[string]struct{})
			for _, resource := range resourceList {
				resourceMap[resource] = struct{}{}
			}
			return &SimpleToken{
				SimpleController: this,
				resourceMap:      resourceMap,
			}
		}
		panic(fmt.Sprintf("unexpected token failure: %v", errToken))
	}
	return token.(*SimpleToken)
}

// Unlock unlocks previous lock. Lock locks all resources. This works even
// after closing the controller.
func (this *SimpleToken) Unlock(resourceList ...string) {
	select {
	case <-this.closeCh:
		// Since object was closed, lets assume all accesses are read-only.
		return
	default:
		for _, resource := range resourceList {
			if !this.ReleaseResource(this, resource) {
				panic(fmt.Sprintf("resource %s was not locked", resource))
			}
		}

		if resourceList == nil || len(this.resourceMap) == 0 {
			this.CloseToken(this)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////

// issueToken issues a token, if the receive hasn't timed out and is still
// waiting.
func (this *SimpleController) issueToken(token *SimpleToken) {
	select {
	case <-token.cancelCh:
		return

	case token.resultCh <- nil:
		this.Add(1)
		if len(token.resourceMap) == 0 {
			this.lockAll = true
			return
		}

		for resource := range token.resourceMap {
			this.resourceMap[resource] = true
		}
	}
}

// isReady returns true if all resources necessary for a token are available.
func (this *SimpleController) isReady(token *SimpleToken) bool {
	// Lock all token.
	if len(token.resourceMap) == 0 {
		if this.lockAll == true {
			return false
		}

		for _, inuse := range this.resourceMap {
			if inuse {
				return false
			}
		}
		return true
	}
	// Normal token.
	for resource := range token.resourceMap {
		if inuse, ok := this.resourceMap[resource]; ok && inuse {
			return false
		}
	}
	return true
}

// cancelToken issues a error to the waiter of the token asynchronously.
func (this *SimpleController) cancelToken(token *SimpleToken, status error) {
	select {
	case <-token.cancelCh:
		return
	case token.resultCh <- status:
		return
	}
}

// releaseToken closes a token and releases all resources locked by that token.
func (this *SimpleController) releaseToken(token *SimpleToken) {
	defer this.Done()

	if len(token.resourceMap) == 0 {
		this.lockAll = false
		return
	}

	for resource := range token.resourceMap {
		this.resourceMap[resource] = false
	}
}

// removeWaiter removes a token from the waiting list by its index.
func (this *SimpleController) removeWaiter(index int) {
	size := len(this.waitingList)
	if size > 1 {
		this.waitingList[index] = this.waitingList[size-1]
		this.waitingList[size-1] = nil
		this.waitingList = this.waitingList[:size-1]
		return
	}
	this.waitingList = nil
}

// nextReady returns the next first token whose resources are all available.
func (this *SimpleController) nextReady() *SimpleToken {
	for ii, token := range this.waitingList {
		if this.isReady(token) {
			this.removeWaiter(ii)
			return token
		}
	}
	return nil
}

// cancelAllWaiting cancels all waiting tokens.
func (this *SimpleController) cancelAllWaiting(status error) {
	for _, token := range this.waitingList {
		this.cancelToken(token, status)
	}
	this.waitingList = nil
}

// goManageTokens handles all token management operations.
func (this *SimpleController) goManageTokens() {
	defer this.Done()

	for {
		select {
		case <-this.closeCh:
			this.cancelAllWaiting(errs.ErrClosed)
			return

		case token := <-this.newCh:
			if this.isReady(token) {
				this.issueToken(token)
			} else {
				this.waitingList = append(this.waitingList, token)
			}

		case token := <-this.doneCh:
			this.releaseToken(token)
			for token := this.nextReady(); token != nil; token = this.nextReady() {
				this.issueToken(token)
			}

		case resource := <-this.releaseCh:
			this.resourceMap[resource] = false
			if ready := this.nextReady(); ready != nil {
				this.issueToken(ready)
			}
		}
	}
}
