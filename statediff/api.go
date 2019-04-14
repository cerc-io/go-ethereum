// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package statediff

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

const APIName = "statediff"
const APIVersion = "0.0.1"

// PublicStateDiffAPI provides the a websocket service
// that can be used to stream out state diffs as they
// are produced by a full node
type PublicStateDiffAPI struct {
	sds SDS

	mu       sync.Mutex
	lastUsed map[string]time.Time // keeps track when a filter was polled for the last time.
}

// NewPublicStateDiffAPI create a new state diff websocket streaming service.
func NewPublicStateDiffAPI(sds SDS) *PublicStateDiffAPI {
	return &PublicStateDiffAPI{
		sds:      sds,
		lastUsed: make(map[string]time.Time),
		mu:       sync.Mutex{},
	}
}

// StreamData set up a subscription that fires off state-diffs when they are created
func (api *PublicStateDiffAPI) StreamStateDiffs(ctx context.Context) (*rpc.Subscription, error) {
	// ensure that the RPC connection supports subscriptions
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, rpc.ErrNotificationsUnsupported
	}

	// create subscription and start waiting for statediff events
	rpcSub := notifier.CreateSubscription()
	id := rpcSub.ID

	go func() {
		// subscribe to events from the state diff service
		payloadChannel := make(chan StateDiffPayload)
		quitChan := make(chan bool)
		api.sds.Subscribe(id, payloadChannel, quitChan)

		// loop and await state diff payloads and relay them to the subscriber with then notifier
		for {
			select {
			case packet := <-payloadChannel:
				if err := notifier.Notify(id, packet); err != nil {
					log.Error("Failed to send state diff packet", "err", err)
				}
			case <-rpcSub.Err():
				err := api.sds.Unsubscribe(id)
				if err != nil {
					log.Error("Failed to unsubscribe from the state diff service", err)
				}
				return
			case <-notifier.Closed():
				err := api.sds.Unsubscribe(id)
				if err != nil {
					log.Error("Failed to unsubscribe from the state diff service", err)
				}
				return
			case <-quitChan:
				// don't need to unsubscribe, statediff service does so before sending the quit signal
				return
			}
		}
	}()

	return rpcSub, nil
}
