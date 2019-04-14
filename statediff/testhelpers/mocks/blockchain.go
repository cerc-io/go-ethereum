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
	"errors"

	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

type BlockChain struct {
	ParentHashesLookedUp []common.Hash
	parentBlocksToReturn []*types.Block
	callCount            int
	ChainEvents          []core.ChainEvent
}

func (mc *BlockChain) AddToStateDiffProcessedCollection(hash common.Hash) {}

func (mc *BlockChain) SetParentBlocksToReturn(blocks []*types.Block) {
	mc.parentBlocksToReturn = blocks
}

func (mc *BlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	mc.ParentHashesLookedUp = append(mc.ParentHashesLookedUp, hash)

	var parentBlock *types.Block
	if len(mc.parentBlocksToReturn) > 0 {
		parentBlock = mc.parentBlocksToReturn[mc.callCount]
	}

	mc.callCount++
	return parentBlock
}

func (bc *BlockChain) SetChainEvents(chainEvents []core.ChainEvent) {
	bc.ChainEvents = chainEvents
}

func (bc *BlockChain) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	subErr := errors.New("Subscription Error")

	var eventCounter int
	subscription := event.NewSubscription(func(quit <-chan struct{}) error {
		for _, chainEvent := range bc.ChainEvents {
			if eventCounter > 1 {
				time.Sleep(250 * time.Millisecond)
				return subErr
			}
			select {
			case ch <- chainEvent:
			case <-quit:
				return nil
			}
			eventCounter++
		}
		return nil
	})

	return subscription
}
