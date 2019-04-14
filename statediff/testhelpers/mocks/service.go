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

package mocks

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff"
)

type MockStateDiffService struct {
	sync.Mutex
	Builder             statediff.Builder
	ReturnProtocol      []p2p.Protocol
	ReturnAPIs          []rpc.API
	MockBlockChan       chan *types.Block
	MockParentBlockChan chan *types.Block
	QuitChan            chan bool
	Subscriptions       statediff.Subscriptions
}

func (sds *MockStateDiffService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the Whisper implementation offers
func (sds *MockStateDiffService) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: statediff.APIName,
			Version:   statediff.APIVersion,
			Service:   statediff.NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

func (sds *MockStateDiffService) Loop(chan core.ChainEvent) {
	//loop through chain events until no more
HandleBlockChLoop:
	for {
		select {
		case block := <-sds.MockBlockChan:
			currentBlock := block
			parentBlock := <-sds.MockParentBlockChan
			parentHash := parentBlock.Hash()
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block",
					"parent block hash", parentHash.String(),
					"current block number", currentBlock.Number())
				break HandleBlockChLoop
			}

			stateDiff, err := sds.Builder.BuildStateDiff(parentBlock.Root(), currentBlock.Root(), currentBlock.Number().Int64(), currentBlock.Hash())
			if err != nil {
				log.Error("Error building statediff", "block number", currentBlock.Number(), "error", err)
			}
			rlpBuff := new(bytes.Buffer)
			currentBlock.EncodeRLP(rlpBuff)
			blockRlp := rlpBuff.Bytes()
			payload := statediff.StateDiffPayload{
				BlockRlp:  blockRlp,
				StateDiff: stateDiff,
				Err:       err,
			}
			// If we have any websocket subscription listening in, send the data to them
			sds.send(payload)
		case <-sds.QuitChan:
			log.Debug("Quitting the statediff block channel")
			sds.close()
			return
		}
	}
}

func (sds *MockStateDiffService) Subscribe(id rpc.ID, sub chan<- statediff.StateDiffPayload, quitChan chan<- bool) {
	log.Info("Subscribing to the statediff service")
	sds.Lock()
	sds.Subscriptions[id] = statediff.Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.Unlock()
}

func (sds *MockStateDiffService) Unsubscribe(id rpc.ID) error {
	log.Info("Unsubscribing from the statediff service")
	sds.Lock()
	_, ok := sds.Subscriptions[id]
	if !ok {
		return fmt.Errorf("cannot unsubscribe; subscription for id %s does not exist", id)
	}
	delete(sds.Subscriptions, id)
	sds.Unlock()
	return nil
}

func (sds *MockStateDiffService) send(payload statediff.StateDiffPayload) {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.PayloadChan <- payload:
			log.Info("sending state diff payload to subscription %s", id)
		default:
			log.Info("unable to send payload to subscription %s; channel has no receiver", id)
		}
	}
	sds.Unlock()
}

func (sds *MockStateDiffService) close() {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.QuitChan <- true:
			delete(sds.Subscriptions, id)
			log.Info("closing subscription %s", id)
		default:
			log.Info("unable to close subscription %s; channel has no receiver", id)
		}
	}
	sds.Unlock()
}

func (sds *MockStateDiffService) Start(server *p2p.Server) error {
	log.Info("Starting statediff service")
	if sds.MockParentBlockChan == nil || sds.MockBlockChan == nil {
		return errors.New("mock StateDiffingService requires preconfiguration with a MockParentBlockChan and MockBlockChan")
	}
	chainEventCh := make(chan core.ChainEvent, 10)
	go sds.Loop(chainEventCh)

	return nil
}

func (sds *MockStateDiffService) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}
