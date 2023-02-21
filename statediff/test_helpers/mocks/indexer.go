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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

// StateDiffIndexer is a mock state diff indexer
type StateDiffIndexer struct{}

func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (interfaces.Batch, error) {
	return nil, nil
}

func (sdi *StateDiffIndexer) PushStateNode(tx interfaces.Batch, stateNode sdtypes.StateNode, headerID string) error {
	return nil
}

func (sdi *StateDiffIndexer) PushCodeAndCodeHash(tx interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	return nil
}

func (sdi *StateDiffIndexer) ReportDBMetrics(delay time.Duration, quit <-chan bool) {}

func (sdi *StateDiffIndexer) LoadWatchedAddresses() ([]common.Address, error) {
	return nil, nil
}

func (sdi *StateDiffIndexer) InsertWatchedAddresses(addresses []sdtypes.WatchAddressArg, currentBlock *big.Int) error {
	return nil
}

func (sdi *StateDiffIndexer) RemoveWatchedAddresses(addresses []sdtypes.WatchAddressArg) error {
	return nil
}

func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	return nil
}

func (sdi *StateDiffIndexer) ClearWatchedAddresses() error {
	return nil
}

func (sdi *StateDiffIndexer) Close() error {
	return nil
}
