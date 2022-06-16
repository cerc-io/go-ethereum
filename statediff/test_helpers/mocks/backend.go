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
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/bloombits"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// Builder is a mock state diff builder
type Backend struct {
	StartingBlock       uint64
	CurrBlock           uint64
	HighestBlock        uint64
	SyncedAccounts      uint64
	SyncedAccountBytes  uint64
	SyncedBytecodes     uint64
	SyncedBytecodeBytes uint64
	SyncedStorage       uint64
	SyncedStorageBytes  uint64
	HealedTrienodes     uint64
	HealedTrienodeBytes uint64
	HealedBytecodes     uint64
	HealedBytecodeBytes uint64
	HealingTrienodes    uint64
	HealingBytecode     uint64
}

// General Ethereum API
func (backend *Backend) SyncProgress() ethereum.SyncProgress {
	l := ethereum.SyncProgress{
		StartingBlock:       backend.StartingBlock,
		CurrentBlock:        backend.CurrBlock,
		HighestBlock:        backend.HighestBlock,
		SyncedAccounts:      backend.SyncedAccounts,
		SyncedAccountBytes:  backend.SyncedAccountBytes,
		SyncedBytecodes:     backend.SyncedBytecodes,
		SyncedBytecodeBytes: backend.SyncedBytecodeBytes,
		SyncedStorage:       backend.SyncedStorage,
		SyncedStorageBytes:  backend.SyncedStorageBytes,
		HealedTrienodes:     backend.HealedTrienodes,
		HealedTrienodeBytes: backend.HealedTrienodeBytes,
		HealedBytecodes:     backend.HealedBytecodes,
		HealedBytecodeBytes: backend.HealedBytecodeBytes,
		HealingTrienodes:    backend.HealingTrienodes,
		HealingBytecode:     backend.HealingBytecode,
	}
	return l
}

func (backend *Backend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) FeeHistory(ctx context.Context, blockCount int, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*big.Int, [][]*big.Int, []*big.Int, []float64, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) ChainDb() ethdb.Database {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) AccountManager() *accounts.Manager {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) ExtRPCEnabled() bool {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) RPCGasCap() uint64 {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) RPCEVMTimeout() time.Duration {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) RPCTxFeeCap() float64 {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) UnprotectedAllowed() bool {
	panic("not implemented") // TODO: Implement
}

// Blockchain API
func (backend *Backend) SetHead(number uint64) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) HeaderByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) CurrentHeader() *types.Header {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) CurrentBlock() *types.Block {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) BlockByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*types.Block, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) StateAndHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) StateAndHeaderByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetTd(ctx context.Context, hash common.Hash) *big.Int {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetEVM(ctx context.Context, msg core.Message, state *state.StateDB, header *types.Header, vmConfig *vm.Config) (*vm.EVM, func() error, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeChainSideEvent(ch chan<- core.ChainSideEvent) event.Subscription {
	panic("not implemented") // TODO: Implement
}

// Transaction pool API
func (backend *Backend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetTransaction(ctx context.Context, txHash common.Hash) (*types.Transaction, common.Hash, uint64, uint64, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetPoolTransactions() (types.Transactions, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetPoolTransaction(txHash common.Hash) *types.Transaction {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) Stats() (pending int, queued int) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) TxPoolContent() (map[common.Address]types.Transactions, map[common.Address]types.Transactions) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) TxPoolContentFrom(addr common.Address) (types.Transactions, types.Transactions) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeNewTxsEvent(_ chan<- core.NewTxsEvent) event.Subscription {
	panic("not implemented") // TODO: Implement
}

// Filter API
func (backend *Backend) BloomStatus() (uint64, uint64) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) GetLogs(ctx context.Context, blockHash common.Hash) ([][]*types.Log, error) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) ServiceFilter(ctx context.Context, session *bloombits.MatcherSession) {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribePendingLogsEvent(ch chan<- []*types.Log) event.Subscription {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) ChainConfig() *params.ChainConfig {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) Engine() consensus.Engine {
	panic("not implemented") // TODO: Implement
}

func (backend *Backend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}
