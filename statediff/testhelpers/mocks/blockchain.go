// Copyright 2019 The go-ethereum Authors
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
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/state"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// BlockChain is a mock blockchain for testing
type BlockChain struct {
	HashesLookedUp         []common.Hash
	blocksToReturnByHash   map[common.Hash]*types.Block
	blocksToReturnByNumber map[uint64]*types.Block
	callCount              int
	ChainEvents            []core.ChainEvent
	Receipts               map[common.Hash]types.Receipts
	TDByHash               map[common.Hash]*big.Int
	TDByNum                map[uint64]*big.Int
	currentBlock           *types.Block
}

// SetBlocksForHashes mock method
func (bc *BlockChain) SetBlocksForHashes(blocks map[common.Hash]*types.Block) {
	if bc.blocksToReturnByHash == nil {
		bc.blocksToReturnByHash = make(map[common.Hash]*types.Block)
	}
	bc.blocksToReturnByHash = blocks
}

// GetBlockByHash mock method
func (bc *BlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	bc.HashesLookedUp = append(bc.HashesLookedUp, hash)

	var block *types.Block
	if len(bc.blocksToReturnByHash) > 0 {
		block = bc.blocksToReturnByHash[hash]
	}

	return block
}

// SetChainEvents mock method
func (bc *BlockChain) SetChainEvents(chainEvents []core.ChainEvent) {
	bc.ChainEvents = chainEvents
}

// SubscribeChainEvent mock method
func (bc *BlockChain) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	subErr := errors.New("subscription error")

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

// SetReceiptsForHash test method
func (bc *BlockChain) SetReceiptsForHash(hash common.Hash, receipts types.Receipts) {
	if bc.Receipts == nil {
		bc.Receipts = make(map[common.Hash]types.Receipts)
	}
	bc.Receipts[hash] = receipts
}

// GetReceiptsByHash mock method
func (bc *BlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	return bc.Receipts[hash]
}

// SetBlockForNumber test method
func (bc *BlockChain) SetBlockForNumber(block *types.Block, number uint64) {
	if bc.blocksToReturnByNumber == nil {
		bc.blocksToReturnByNumber = make(map[uint64]*types.Block)
	}
	bc.blocksToReturnByNumber[number] = block
}

// GetBlockByNumber mock method
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	return bc.blocksToReturnByNumber[number]
}

// GetTd mock method
func (bc *BlockChain) GetTd(hash common.Hash, blockNum uint64) *big.Int {
	if td, ok := bc.TDByHash[hash]; ok {
		return td
	}

	if td, ok := bc.TDByNum[blockNum]; ok {
		return td
	}
	return nil
}

// SetCurrentBlock test method
func (bc *BlockChain) SetCurrentBlock(block *types.Block) {
	bc.currentBlock = block
}

// CurrentBlock mock method
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock
}

func (bc *BlockChain) SetTd(hash common.Hash, blockNum uint64, td *big.Int) {
	if bc.TDByHash == nil {
		bc.TDByHash = make(map[common.Hash]*big.Int)
	}
	bc.TDByHash[hash] = td

	if bc.TDByNum == nil {
		bc.TDByNum = make(map[uint64]*big.Int)
	}
	bc.TDByNum[blockNum] = td
}

func (bc *BlockChain) UnlockTrie(root common.Hash) {}

func (bc *BlockChain) StateCache() state.Database {
	return nil
}
