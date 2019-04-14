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
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/node"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
)

type BlockChain interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	GetBlockByHash(hash common.Hash) *types.Block
	AddToStateDiffProcessedCollection(hash common.Hash)
}

type SDS interface {
	// APIs(), Protocols(), Start() and Stop()
	node.Service
	// Main event loop for processing state diffs
	Loop(chainEventCh chan core.ChainEvent)
	// Method to subscribe to receive state diff processing output
	Subscribe(id rpc.ID, sub chan<- StateDiffPayload, quitChan chan<- bool)
	// Method to unsubscribe from state diff processing
	Unsubscribe(id rpc.ID) error
}

type StateDiffingService struct {
	sync.Mutex
	Builder       Builder
	BlockChain    BlockChain
	QuitChan      chan bool
	Subscriptions Subscriptions
}

type Subscriptions map[rpc.ID]Subscription

type Subscription struct {
	PayloadChan chan<- StateDiffPayload
	QuitChan    chan<- bool
}

type StateDiffPayload struct {
	BlockRlp  []byte    `json:"block"`
	StateDiff StateDiff `json:"state_diff"`
	Err       error     `json:"error"`
}

func NewStateDiffService(db ethdb.Database, blockChain *core.BlockChain) (*StateDiffingService, error) {
	return &StateDiffingService{
		Mutex:         sync.Mutex{},
		BlockChain:    blockChain,
		Builder:       NewBuilder(db, blockChain),
		QuitChan:      make(chan bool),
		Subscriptions: make(Subscriptions),
	}, nil
}

func (sds *StateDiffingService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the Whisper implementation offers
func (sds *StateDiffingService) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: APIName,
			Version:   APIVersion,
			Service:   NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

func (sds *StateDiffingService) Loop(chainEventCh chan core.ChainEvent) {
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()

	blocksCh := make(chan *types.Block, 10)
	errCh := chainEventSub.Err()

	go func() {
	HandleChainEventChLoop:
		for {
			select {
			//Notify chain event channel of events
			case chainEvent := <-chainEventCh:
				log.Debug("Event received from chainEventCh", "event", chainEvent)
				blocksCh <- chainEvent.Block
				//if node stopped
			case err := <-errCh:
				log.Warn("Error from chain event subscription, breaking loop.", "error", err)
				close(sds.QuitChan)
				break HandleChainEventChLoop
			case <-sds.QuitChan:
				break HandleChainEventChLoop
			}
		}
	}()

	//loop through chain events until no more
HandleBlockChLoop:
	for {
		select {
		case block := <-blocksCh:
			currentBlock := block
			parentHash := currentBlock.ParentHash()
			parentBlock := sds.BlockChain.GetBlockByHash(parentHash)
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
			payload := StateDiffPayload{
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

func (sds *StateDiffingService) Subscribe(id rpc.ID, sub chan<- StateDiffPayload, quitChan chan<- bool) {
	log.Info("Subscribing to the statediff service")
	sds.Lock()
	sds.Subscriptions[id] = Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.Unlock()
}

func (sds *StateDiffingService) Unsubscribe(id rpc.ID) error {
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

func (sds *StateDiffingService) send(payload StateDiffPayload) {
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

func (sds *StateDiffingService) close() {
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

func (sds *StateDiffingService) Start(server *p2p.Server) error {
	log.Info("Starting statediff service")

	chainEventCh := make(chan core.ChainEvent, 10)
	go sds.Loop(chainEventCh)

	return nil
}

func (sds *StateDiffingService) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}
