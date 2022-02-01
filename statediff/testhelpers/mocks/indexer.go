// Copyright 2022 The go-ethereum Authors
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
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

// Indexer is a mock state diff indexer
type Indexer struct{}

func (sdi *Indexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (*indexer.BlockTx, error) {
	return nil, nil
}

func (sdi *Indexer) PushStateNode(tx *indexer.BlockTx, stateNode sdtypes.StateNode) error {
	return nil
}

func (sdi *Indexer) PushCodeAndCodeHash(tx *indexer.BlockTx, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	return nil
}

func (sdi *Indexer) ReportDBMetrics(delay time.Duration, quit <-chan bool) {}

func (sdi *Indexer) InsertWatchedAddresses(addresses []sdtypes.WatchAddressArg, currentBlock *big.Int, kind sdtypes.WatchedAddressType) error {
	return nil
}

func (sdi *Indexer) RemoveWatchedAddresses(addresses []sdtypes.WatchAddressArg, kind sdtypes.WatchedAddressType) error {
	return nil
}

func (sdi *Indexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int, kind sdtypes.WatchedAddressType) error {
	return nil
}

func (sdi *Indexer) ClearWatchedAddresses(kind sdtypes.WatchedAddressType) error {
	return nil
}
