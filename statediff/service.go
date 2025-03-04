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

package statediff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	ind "github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	types2 "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/thoas/go-funk"
)

const (
	chainEventChanSize  = 20000
	genesisBlockNumber  = 0
	defaultRetryLimit   = 3                   // default retry limit once deadlock is detected.
	deadlockDetected    = "deadlock detected" // 40P01 https://www.postgresql.org/docs/current/errcodes-appendix.html
	typeAssertionFailed = "type assertion failed"
	unexpectedOperation = "unexpected operation"
)

var writeLoopParams = ParamsWithMutex{
	Params: Params{
		IncludeBlock:    true,
		IncludeReceipts: true,
		IncludeTD:       true,
		IncludeCode:     true,
	},
}

type blockChain interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	CurrentBlock() *types.Header
	GetBlockByHash(hash common.Hash) *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GetReceiptsByHash(hash common.Hash) types.Receipts
	GetTd(hash common.Hash, number uint64) *big.Int
	UnlockTrie(root common.Hash)
	StateCache() state.Database
}

// IService is the state-diffing service interface
type IService interface {
	// Lifecycle Start() and Stop() methods
	node.Lifecycle
	// APIs method for getting API(s) for this service
	APIs() []rpc.API
	// Loop is the main event loop for processing state diffs
	Loop(chainEventCh chan core.ChainEvent)
	// Subscribe method to subscribe to receive state diff processing output
	Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params)
	// Unsubscribe method to unsubscribe from state diff processing
	Unsubscribe(id rpc.ID) error
	// StateDiffAt method to get state diff object at specific block
	StateDiffAt(blockNumber uint64, params Params) (*Payload, error)
	// StateDiffFor method to get state diff object at specific block
	StateDiffFor(blockHash common.Hash, params Params) (*Payload, error)
	// WriteStateDiffAt method to write state diff object directly to DB
	WriteStateDiffAt(blockNumber uint64, params Params) JobID
	// WriteStateDiffFor method to write state diff object directly to DB
	WriteStateDiffFor(blockHash common.Hash, params Params) error
	// WriteLoop event loop for progressively processing and writing diffs directly to DB
	WriteLoop(chainEventCh chan core.ChainEvent)
	// WatchAddress method to change the addresses being watched in write loop params
	WatchAddress(operation types2.OperationType, args []types2.WatchAddressArg) error
	// StreamCodeAndCodeHash method to export all the codehash => code mappings at a block height
	StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- types2.CodeAndCodeHash, quitChan chan<- bool)

	// SubscribeWriteStatus method to subscribe to receive state diff processing output
	SubscribeWriteStatus(id rpc.ID, sub chan<- JobStatus, quitChan chan<- bool)
	// UnsubscribeWriteStatus method to unsubscribe from state diff processing
	UnsubscribeWriteStatus(id rpc.ID) error
}

// Service is the underlying struct for the state diffing service
type Service struct {
	// Used to sync access to the Subscriptions
	sync.Mutex
	// Used to build the state diff objects
	Builder Builder
	// Used to subscribe to chain events (blocks)
	BlockChain blockChain
	// Used to signal shutdown of the service
	QuitChan chan bool
	// A mapping of rpc.IDs to their subscription channels, mapped to their subscription type (hash of the Params rlp)
	Subscriptions map[common.Hash]map[rpc.ID]Subscription
	// A mapping of subscription params rlp hash to the corresponding subscription params
	SubscriptionTypes map[common.Hash]Params
	// Cache the last block so that we can avoid having to lookup the next block's parent
	BlockCache BlockCache
	// The publicBackendAPI which provides useful information about the current state
	BackendAPI ethapi.Backend
	// Should the statediff service wait for geth to sync to head?
	WaitForSync bool
	// Whether we have any subscribers
	subscribers int32
	// Interface for publishing statediffs as PG-IPLD objects
	indexer interfaces.StateDiffIndexer
	// Whether to enable writing state diffs directly to track blockchain head.
	enableWriteLoop bool
	// Settings to use for backfilling state diffs (plugging gaps when tracking head)
	backfillMaxDepth        uint64
	backfillCheckPastBlocks uint64
	// Size of the worker pool
	numWorkers uint
	// Number of retry for aborted transactions due to deadlock.
	maxRetry uint
	// Write job status subscriptions
	jobStatusSubs map[rpc.ID]statusSubscription
	// Job ID ticker
	lastJobID uint64
	// In flight jobs (for WriteStateDiffAt)
	currentJobs        map[uint64]JobID
	currentJobsMutex   sync.Mutex
	currentBlocks      map[string]bool
	currentBlocksMutex sync.Mutex
}

// IDs used for tracking in-progress jobs (0 for invalid)
type JobID uint64

// JobStatus represents the status of a completed job
type JobStatus struct {
	ID  JobID
	Err error
}

type statusSubscription struct {
	statusChan chan<- JobStatus
	quitChan   chan<- bool
}

// Utility type for showing the relative positions of the blockchain and the statediff indexer.
type servicePosition struct {
	chainBlockNumber   uint64
	indexerBlockNumber uint64
}

// BlockCache caches the last block for safe access from different service loops
type BlockCache struct {
	sync.Mutex
	blocks  map[common.Hash]*types.Block
	maxSize uint
}

func NewBlockCache(max uint) BlockCache {
	return BlockCache{
		blocks:  make(map[common.Hash]*types.Block),
		maxSize: max,
	}
}

// New creates a new statediff.Service
// func New(stack *node.Node, ethServ *eth.Ethereum, dbParams *DBParams, enableWriteLoop bool) error {
// func New(stack *node.Node, blockChain *core.BlockChain, networkID uint64, params Config, backend ethapi.Backend) error {
func New(stack *node.Node, ethServ *eth.Ethereum, cfg *ethconfig.Config, params Config, backend ethapi.Backend) error {
	blockChain := ethServ.BlockChain()
	var indexer interfaces.StateDiffIndexer
	var err error
	quitCh := make(chan bool)
	indexerConfigAvailable := params.IndexerConfig != nil
	if indexerConfigAvailable {
		info := nodeinfo.Info{
			GenesisBlock: blockChain.Genesis().Hash().Hex(),
			NetworkID:    strconv.FormatUint(cfg.NetworkId, 10),
			ChainID:      blockChain.Config().ChainID.Uint64(),
			ID:           params.ID,
			ClientName:   params.ClientName,
		}
		var err error
		_, indexer, err = ind.NewStateDiffIndexer(params.Context, blockChain.Config(), info, params.IndexerConfig)
		if err != nil {
			return err
		}
		indexer.ReportDBMetrics(10*time.Second, quitCh)
	}

	workers := params.NumWorkers
	if workers == 0 {
		workers = 1
	}

	sds := &Service{
		Mutex:                   sync.Mutex{},
		BlockChain:              blockChain,
		Builder:                 NewBuilder(blockChain.StateCache()),
		QuitChan:                quitCh,
		Subscriptions:           make(map[common.Hash]map[rpc.ID]Subscription),
		SubscriptionTypes:       make(map[common.Hash]Params),
		BlockCache:              NewBlockCache(workers),
		BackendAPI:              backend,
		WaitForSync:             params.WaitForSync,
		indexer:                 indexer,
		enableWriteLoop:         params.EnableWriteLoop,
		backfillMaxDepth:        params.BackfillMaxDepth,
		backfillCheckPastBlocks: params.BackfillCheckPastBlocks,
		numWorkers:              workers,
		maxRetry:                defaultRetryLimit,
		jobStatusSubs:           map[rpc.ID]statusSubscription{},
		currentJobs:             map[uint64]JobID{},
		currentJobsMutex:        sync.Mutex{},
		currentBlocks:           map[string]bool{},
		currentBlocksMutex:      sync.Mutex{},
	}
	stack.RegisterLifecycle(sds)
	stack.RegisterAPIs(sds.APIs())

	if indexerConfigAvailable {
		err = loadWatchedAddresses(indexer)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewService(blockChain blockChain, cfg Config, backend ethapi.Backend, indexer interfaces.StateDiffIndexer) *Service {
	workers := cfg.NumWorkers
	if workers == 0 {
		workers = 1
	}

	quitCh := make(chan bool)
	sds := &Service{
		Mutex:                   sync.Mutex{},
		BlockChain:              blockChain,
		Builder:                 NewBuilder(blockChain.StateCache()),
		QuitChan:                quitCh,
		Subscriptions:           make(map[common.Hash]map[rpc.ID]Subscription),
		SubscriptionTypes:       make(map[common.Hash]Params),
		BlockCache:              NewBlockCache(workers),
		BackendAPI:              backend,
		WaitForSync:             cfg.WaitForSync,
		indexer:                 indexer,
		enableWriteLoop:         cfg.EnableWriteLoop,
		backfillMaxDepth:        cfg.BackfillMaxDepth,
		backfillCheckPastBlocks: cfg.BackfillCheckPastBlocks,
		numWorkers:              workers,
		maxRetry:                defaultRetryLimit,
		jobStatusSubs:           map[rpc.ID]statusSubscription{},
		currentJobs:             map[uint64]JobID{},
		currentJobsMutex:        sync.Mutex{},
		currentBlocks:           map[string]bool{},
		currentBlocksMutex:      sync.Mutex{},
	}

	if indexer != nil {
		indexer.ReportDBMetrics(10*time.Second, quitCh)
	}
	return sds
}

// Protocols exports the services p2p protocols, this service has none
func (sds *Service) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the statediff.Service offers
func (sds *Service) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: APIName,
			Version:   APIVersion,
			Service:   NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

// Return the parent block of currentBlock, using the cached block if available;
// and cache the passed block
func (lbc *BlockCache) getParentBlock(currentBlock *types.Block, bc blockChain) *types.Block {
	lbc.Lock()
	parentHash := currentBlock.ParentHash()
	var parentBlock *types.Block
	if block, ok := lbc.blocks[parentHash]; ok {
		parentBlock = block
		if len(lbc.blocks) > int(lbc.maxSize) {
			delete(lbc.blocks, parentHash)
		}
	} else {
		parentBlock = bc.GetBlockByHash(parentHash)
	}
	lbc.blocks[currentBlock.Hash()] = currentBlock
	lbc.Unlock()
	return parentBlock
}

type workerParams struct {
	blockCh <-chan *types.Block
	wg      *sync.WaitGroup
	id      uint
}

func (sds *Service) WriteLoop(chainEventCh chan core.ChainEvent) {
	initialPos := sds.currentPosition()
	log.Info(
		"WriteLoop: initial positions",
		"chain", initialPos.chainBlockNumber,
		"indexer", initialPos.indexerBlockNumber,
	)
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()
	errCh := chainEventSub.Err()
	var wg sync.WaitGroup
	// Process metrics for chain events, then forward to workers
	blockFwd := make(chan *types.Block, chainEventChanSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case chainEvent := <-chainEventCh:
				lastHeight := uint64(defaultStatediffMetrics.lastEventHeight.Value())
				if lastHeight == 0 {
					lastHeight = initialPos.indexerBlockNumber
				}
				nextHeight := chainEvent.Block.Number().Uint64()
				if nextHeight > lastHeight {
					distance := nextHeight - lastHeight
					if distance == 1 {
						log.Info("WriteLoop: received expected block", "block height", nextHeight, "last height", lastHeight)
						blockFwd <- chainEvent.Block
						defaultStatediffMetrics.lastEventHeight.Update(int64(nextHeight))
					} else {
						log.Warn("WriteLoop: received unexpected block from the future", "block height", nextHeight, "last height", lastHeight)
						blockFwd <- chainEvent.Block
						defaultStatediffMetrics.lastEventHeight.Update(int64(nextHeight))
					}
				} else {
					log.Warn("WriteLoop: received unexpected block from the past", "block height", nextHeight, "last height", lastHeight)
					blockFwd <- chainEvent.Block
				}
				defaultStatediffMetrics.writeLoopChannelLen.Update(int64(len(chainEventCh)))
			case err := <-errCh:
				log.Error("Error from chain event subscription", "error", err)
				close(sds.QuitChan)
				log.Info("Quitting the statediffing writing loop")
				if err := sds.indexer.Close(); err != nil {
					log.Error("Error closing indexer", "err", err)
				}
				return
			case <-sds.QuitChan:
				log.Info("Quitting the statediffing writing loop")
				if err := sds.indexer.Close(); err != nil {
					log.Error("Error closing indexer", "err", err)
				}
				return
			}
		}
	}()
	wg.Add(int(sds.numWorkers))
	for worker := uint(0); worker < sds.numWorkers; worker++ {
		params := workerParams{blockCh: blockFwd, wg: &wg, id: worker}
		go sds.writeLoopWorker(params)
	}
	wg.Wait()
}

func (sds *Service) writeGenesisStateDiff(currBlock *types.Block, workerId uint) {
	// For genesis block we need to return the entire state trie hence we diff it with an empty trie.
	log.Info("Writing state diff", "block height", genesisBlockNumber, "worker", workerId)
	err := sds.writeStateDiffWithRetry(currBlock, common.Hash{}, writeLoopParams.CopyParams())
	if err != nil {
		log.Error("statediff.Service.WriteLoop: processing error", "block height",
			genesisBlockNumber, "error", err.Error(), "worker", workerId)
		return
	}
	defaultStatediffMetrics.lastStatediffHeight.Update(genesisBlockNumber)
}

func (sds *Service) writeLoopWorker(params workerParams) {
	defer params.wg.Done()

	// statediffs the indicated block, and while maxBackfill > 0, backfills missing parent blocks.
	var writeBlockWithParents func(*types.Block, uint64, Params) error
	writeBlockWithParents = func(block *types.Block, maxBackfill uint64, writeParams Params) error {
		parentBlock := sds.BlockCache.getParentBlock(block, sds.BlockChain)
		if parentBlock == nil {
			log.Error("Parent block is nil, skipping this block", "block height", block.Number())
			return nil
		}

		parentIsGenesis := parentBlock.Number().Uint64() == genesisBlockNumber

		// chainEvent streams block from block 1, but we also need to include data from the genesis block.
		if parentIsGenesis {
			sds.writeGenesisStateDiff(parentBlock, params.id)
		}

		log.Info("Writing state diff", "block height", block.Number().Uint64(), "worker", params.id)
		err := sds.writeStateDiffWithRetry(block, parentBlock.Root(), writeParams)
		if err != nil {
			log.Error("statediff.Service.WriteLoop: processing error",
				"number", block.Number().Uint64(),
				"hash", block.Hash().Hex(),
				"error", err.Error(),
				"worker", params.id)
			return err
		}

		if !parentIsGenesis {
			// We do this _after_ indexing the requested block.  This makes sure that if a child of ours arrives for
			// statediffing while we are still working on missing ancestors, its regress stops at us, and only we
			// continue working backward.
			parentIndexed, err := sds.indexedOrInProgress(parentBlock)
			if err != nil {
				log.Error("Error checking for indexing status of parent block.",
					"number", block.Number(), "hash", block.Hash().String(),
					"parent number", parentBlock.NumberU64(), "parent hash", parentBlock.Hash().Hex(),
					"error", err.Error(),
					"worker", params.id)
			} else if !parentIndexed {
				if maxBackfill > 0 {
					log.Info("Parent block not indexed. Indexing now.",
						"number", block.Number(), "hash", block.Hash().Hex(),
						"parent number", parentBlock.NumberU64(), "parent hash", parentBlock.Hash().Hex(),
						"worker", params.id)
					err = writeBlockWithParents(parentBlock, maxBackfill-1, writeParams)
					if err != nil {
						log.Error("Error indexing parent block.",
							"number", block.Number(), "hash", block.Hash().Hex(),
							"parent number", parentBlock.NumberU64(), "parent hash", parentBlock.Hash().Hex(),
							"error", err.Error(),
							"worker", params.id)
					}
				} else {
					log.Error("ERROR: Parent block not indexed but max backfill depth exceeded. Index MUST be corrected manually.",
						"number", block.Number(), "hash", block.Hash().String(),
						"parent number", parentBlock.NumberU64(), "parent hash", parentBlock.Hash().String(),
						"worker", params.id)
				}
			}
		}

		return nil
	}

	for {
		select {
		//Notify chain event channel of events
		case chainEvent := <-params.blockCh:
			log.Debug("WriteLoop(): chain event received", "event", chainEvent)
			currentBlock := chainEvent
			err := writeBlockWithParents(currentBlock, sds.backfillMaxDepth, writeLoopParams.CopyParams())
			if err != nil {
				log.Error("statediff.Service.WriteLoop: processing error",
					"block height", currentBlock.Number().Uint64(),
					"block hash", currentBlock.Hash().Hex(),
					"error", err.Error(),
					"worker", params.id)
				continue
			}

			// TODO: how to handle with concurrent workers
			defaultStatediffMetrics.lastStatediffHeight.Update(int64(currentBlock.Number().Uint64()))
		case <-sds.QuitChan:
			log.Info("Quitting the statediff writing process", "worker", params.id)
			return
		}
	}
}

// Loop is the main processing method
func (sds *Service) Loop(chainEventCh chan core.ChainEvent) {
	log.Info("Starting statediff listening loop")
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()
	errCh := chainEventSub.Err()
	for {
		select {
		//Notify chain event channel of events
		case chainEvent := <-chainEventCh:
			// TODO: Do we need to track the last streamed block as we do for the WriteLoop  so that we can detect
			// and plug any gaps in the events?  If not, we risk presenting an incomplete record.
			defaultStatediffMetrics.serviceLoopChannelLen.Update(int64(len(chainEventCh)))
			log.Debug("Loop(): chain event received", "event", chainEvent)
			// if we don't have any subscribers, do not process a statediff
			if atomic.LoadInt32(&sds.subscribers) == 0 {
				log.Debug("Currently no subscribers to the statediffing service; processing is halted")
				continue
			}
			currentBlock := chainEvent.Block
			parentBlock := sds.BlockCache.getParentBlock(currentBlock, sds.BlockChain)

			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block", "block height", currentBlock.Number())
				continue
			}

			// chainEvent streams block from block 1, but we also need to include data from the genesis block.
			if parentBlock.Number().Uint64() == genesisBlockNumber {
				// For genesis block we need to return the entire state trie hence we diff it with an empty trie.
				sds.streamStateDiff(parentBlock, common.Hash{})
			}

			sds.streamStateDiff(currentBlock, parentBlock.Root())
		case err := <-errCh:
			log.Error("Error from chain event subscription", "error", err)
			close(sds.QuitChan)
			log.Info("Quitting the statediffing listening loop")
			sds.close()
			return
		case <-sds.QuitChan:
			log.Info("Quitting the statediffing listening loop")
			sds.close()
			return
		}
	}
}

// streamStateDiff method builds the state diff payload for each subscription according to their subscription type and sends them the result
func (sds *Service) streamStateDiff(currentBlock *types.Block, parentRoot common.Hash) {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		params, ok := sds.SubscriptionTypes[ty]
		if !ok {
			log.Error("no parameter set associated with this subscription", "subscription type", ty.Hex())
			sds.closeType(ty)
			continue
		}
		// create payload for this subscription type
		payload, err := sds.processStateDiff(currentBlock, parentRoot, params)
		if err != nil {
			log.Error("statediff processing error", "block height", currentBlock.Number().Uint64(), "parameters", params, "error", err.Error())
			continue
		}
		for id, sub := range subs {
			select {
			case sub.PayloadChan <- *payload:
				log.Debug("sending statediff payload at head", "height", currentBlock.Number(), "subscription id", id)
			default:
				log.Info("unable to send statediff payload; channel has no receiver", "subscription id", id)
			}
		}
	}
	sds.Unlock()
}

// StateDiffAt returns a state diff object payload at the specific blockheight
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateDiffAt(blockNumber uint64, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending state diff", "block height", blockNumber)

	// use watched addresses from statediffing write loop if not provided
	if params.WatchedAddresses == nil && writeLoopParams.WatchedAddresses != nil {
		writeLoopParams.RLock()
		params.WatchedAddresses = make([]common.Address, len(writeLoopParams.WatchedAddresses))
		copy(params.WatchedAddresses, writeLoopParams.WatchedAddresses)
		writeLoopParams.RUnlock()
	}
	// compute leaf paths of watched addresses in the params
	params.ComputeWatchedAddressesLeafPaths()

	if blockNumber == 0 {
		return sds.processStateDiff(currentBlock, common.Hash{}, params)
	}
	parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
	return sds.processStateDiff(currentBlock, parentBlock.Root(), params)
}

// StateDiffFor returns a state diff object payload for the specific blockhash
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateDiffFor(blockHash common.Hash, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByHash(blockHash)
	log.Info("sending state diff", "block hash", blockHash)

	// use watched addresses from statediffing write loop if not provided
	if params.WatchedAddresses == nil && writeLoopParams.WatchedAddresses != nil {
		writeLoopParams.RLock()
		params.WatchedAddresses = make([]common.Address, len(writeLoopParams.WatchedAddresses))
		copy(params.WatchedAddresses, writeLoopParams.WatchedAddresses)
		writeLoopParams.RUnlock()
	}
	// compute leaf paths of watched addresses in the params
	params.ComputeWatchedAddressesLeafPaths()

	if currentBlock.NumberU64() == 0 {
		return sds.processStateDiff(currentBlock, common.Hash{}, params)
	}
	parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
	return sds.processStateDiff(currentBlock, parentBlock.Root(), params)
}

// processStateDiff method builds the state diff payload from the current block, parent state root, and provided params
func (sds *Service) processStateDiff(currentBlock *types.Block, parentRoot common.Hash, params Params) (*Payload, error) {
	stateDiff, err := sds.Builder.BuildStateDiffObject(Args{
		NewStateRoot: currentBlock.Root(),
		OldStateRoot: parentRoot,
		BlockHash:    currentBlock.Hash(),
		BlockNumber:  currentBlock.Number(),
	}, params)
	// allow dereferencing of parent, keep current locked as it should be the next parent
	sds.BlockChain.UnlockTrie(parentRoot)
	if err != nil {
		return nil, err
	}
	stateDiffRlp, err := rlp.EncodeToBytes(&stateDiff)
	if err != nil {
		return nil, err
	}
	log.Info("state diff size", "at block height", currentBlock.Number().Uint64(), "rlp byte size", len(stateDiffRlp))
	return sds.newPayload(stateDiffRlp, currentBlock, params)
}

func (sds *Service) newPayload(stateObject []byte, block *types.Block, params Params) (*Payload, error) {
	payload := &Payload{
		StateObjectRlp: stateObject,
	}
	if params.IncludeBlock {
		blockBuff := new(bytes.Buffer)
		if err := block.EncodeRLP(blockBuff); err != nil {
			return nil, err
		}
		payload.BlockRlp = blockBuff.Bytes()
	}
	if params.IncludeTD {
		payload.TotalDifficulty = sds.BlockChain.GetTd(block.Hash(), block.NumberU64())
	}
	if params.IncludeReceipts {
		receiptBuff := new(bytes.Buffer)
		receipts := sds.BlockChain.GetReceiptsByHash(block.Hash())
		if err := rlp.Encode(receiptBuff, receipts); err != nil {
			return nil, err
		}
		payload.ReceiptsRlp = receiptBuff.Bytes()
	}
	return payload, nil
}

// Subscribe is used by the API to subscribe to the service loop
func (sds *Service) Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params) {
	log.Info("Subscribing to the statediff service")
	if atomic.CompareAndSwapInt32(&sds.subscribers, 0, 1) {
		log.Info("State diffing subscription received; beginning statediff processing")
	}

	// compute leaf paths of watched addresses in the params
	params.ComputeWatchedAddressesLeafPaths()

	// Subscription type is defined as the hash of the rlp-serialized subscription params
	by, err := rlp.EncodeToBytes(&params)
	if err != nil {
		log.Error("State diffing params need to be rlp-serializable")
		return
	}
	subscriptionType := crypto.Keccak256Hash(by)
	// Add subscriber
	sds.Lock()
	if sds.Subscriptions[subscriptionType] == nil {
		sds.Subscriptions[subscriptionType] = make(map[rpc.ID]Subscription)
	}
	sds.Subscriptions[subscriptionType][id] = Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.SubscriptionTypes[subscriptionType] = params
	sds.Unlock()
}

// Unsubscribe is used to unsubscribe from the service loop
func (sds *Service) Unsubscribe(id rpc.ID) error {
	log.Info("Unsubscribing from the statediff service", "subscription id", id)
	sds.Lock()
	for ty := range sds.Subscriptions {
		delete(sds.Subscriptions[ty], id)
		if len(sds.Subscriptions[ty]) == 0 {
			// If we removed the last subscription of this type, remove the subscription type outright
			delete(sds.Subscriptions, ty)
			delete(sds.SubscriptionTypes, ty)
		}
	}
	if len(sds.Subscriptions) == 0 {
		if atomic.CompareAndSwapInt32(&sds.subscribers, 1, 0) {
			log.Info("No more subscriptions; halting statediff processing")
		}
	}
	sds.Unlock()
	return nil
}

// GetSyncStatus will check the status of geth syncing.
// It will return false if geth has finished syncing.
// It will return a true Geth is still syncing.
func (sds *Service) GetSyncStatus(pubEthAPI *ethapi.EthereumAPI) (bool, error) {
	syncStatus, err := pubEthAPI.Syncing()
	if err != nil {
		return true, err
	}

	if syncStatus != false {
		return true, err
	}
	return false, err
}

// WaitingForSync calls GetSyncStatus to check if we have caught up to head.
// It will keep looking and checking if we have caught up to head.
// It will only complete if we catch up to head, otherwise it will keep looping forever.
func (sds *Service) WaitingForSync() error {
	log.Info("We are going to wait for geth to sync to head!")

	// Has the geth node synced to head?
	Synced := false
	pubEthAPI := ethapi.NewEthereumAPI(sds.BackendAPI)
	for !Synced {
		syncStatus, err := sds.GetSyncStatus(pubEthAPI)
		if err != nil {
			return err
		}
		if !syncStatus {
			log.Info("Geth has caught up to the head of the chain")
			Synced = true
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

// Start is used to begin the service
func (sds *Service) Start() error {
	log.Info("Starting statediff service")

	if sds.WaitForSync {
		log.Info("Statediff service will wait until geth has caught up to the head of the chain.")
		err := sds.WaitingForSync()
		if err != nil {
			return err
		}
		log.Info("Continuing with startdiff start process")
	}
	chainEventCh := make(chan core.ChainEvent, chainEventChanSize)
	go sds.Loop(chainEventCh)

	if sds.enableWriteLoop {
		log.Info("Starting statediff DB backfill", "params", writeLoopParams.Params)
		go sds.Backfill()
		log.Info("Starting statediff DB write loop", "params", writeLoopParams.Params)
		chainEventCh := make(chan core.ChainEvent, chainEventChanSize)
		go sds.WriteLoop(chainEventCh)
	}

	return nil
}

// Stop is used to close down the service
func (sds *Service) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}

// close is used to close all listening subscriptions
func (sds *Service) close() {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		for id, sub := range subs {
			select {
			case sub.QuitChan <- true:
				log.Info("closing subscription", "id", id)
			default:
				log.Info("unable to close subscription; channel has no receiver", "subscription id", id)
			}
			delete(sds.Subscriptions[ty], id)
		}
		delete(sds.Subscriptions, ty)
		delete(sds.SubscriptionTypes, ty)
	}
	sds.Unlock()
}

// closeType is used to close all subscriptions of given type
// closeType needs to be called with subscription access locked
func (sds *Service) closeType(subType common.Hash) {
	subs := sds.Subscriptions[subType]
	for id, sub := range subs {
		sendNonBlockingQuit(id, sub)
	}
	delete(sds.Subscriptions, subType)
	delete(sds.SubscriptionTypes, subType)
}

func sendNonBlockingQuit(id rpc.ID, sub Subscription) {
	select {
	case sub.QuitChan <- true:
		log.Info("closing subscription", "id", id)
	default:
		log.Info("unable to close subscription; channel has no receiver", "subscription id", id)
	}
}

// WriteStateDiffAt writes a state diff at the specific blockheight directly to the database
// This operation cannot be performed back past the point of db pruning; it requires an archival node
// for historical data
func (sds *Service) WriteStateDiffAt(blockNumber uint64, params Params) JobID {
	sds.currentJobsMutex.Lock()
	defer sds.currentJobsMutex.Unlock()
	if id, has := sds.currentJobs[blockNumber]; has {
		return id
	}
	id := JobID(atomic.AddUint64(&sds.lastJobID, 1))
	sds.currentJobs[blockNumber] = id
	go func() {
		err := sds.writeStateDiffAt(blockNumber, params)
		sds.currentJobsMutex.Lock()
		delete(sds.currentJobs, blockNumber)
		sds.currentJobsMutex.Unlock()
		for _, sub := range sds.jobStatusSubs {
			sub.statusChan <- JobStatus{id, err}
		}
	}()
	return id
}

func (sds *Service) writeStateDiffAt(blockNumber uint64, params Params) error {
	log.Info("writing state diff at", "block height", blockNumber)

	// use watched addresses from statediffing write loop if not provided
	if params.WatchedAddresses == nil && writeLoopParams.WatchedAddresses != nil {
		writeLoopParams.RLock()
		params.WatchedAddresses = make([]common.Address, len(writeLoopParams.WatchedAddresses))
		copy(params.WatchedAddresses, writeLoopParams.WatchedAddresses)
		writeLoopParams.RUnlock()
	}
	// compute leaf paths of watched addresses in the params
	params.ComputeWatchedAddressesLeafPaths()

	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	parentRoot := common.Hash{}
	if blockNumber != 0 {
		parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
		parentRoot = parentBlock.Root()
	}
	return sds.writeStateDiffWithRetry(currentBlock, parentRoot, params)
}

// WriteStateDiffFor writes a state diff for the specific blockhash directly to the database
// This operation cannot be performed back past the point of db pruning; it requires an archival node
// for historical data
func (sds *Service) WriteStateDiffFor(blockHash common.Hash, params Params) error {
	log.Info("writing state diff for", "block hash", blockHash)

	// use watched addresses from statediffing write loop if not provided
	if params.WatchedAddresses == nil && writeLoopParams.WatchedAddresses != nil {
		writeLoopParams.RLock()
		params.WatchedAddresses = make([]common.Address, len(writeLoopParams.WatchedAddresses))
		copy(params.WatchedAddresses, writeLoopParams.WatchedAddresses)
		writeLoopParams.RUnlock()
	}
	// compute leaf paths of watched addresses in the params
	params.ComputeWatchedAddressesLeafPaths()

	currentBlock := sds.BlockChain.GetBlockByHash(blockHash)
	parentRoot := common.Hash{}
	if currentBlock.NumberU64() != 0 {
		parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
		parentRoot = parentBlock.Root()
	}
	return sds.writeStateDiffWithRetry(currentBlock, parentRoot, params)
}

// indexedOrInProgress returns true if the block has already been statediffed or is in progress, else false.
func (sds *Service) indexedOrInProgress(block *types.Block) (bool, error) {
	if sds.statediffInProgress(block) {
		return true, nil
	}
	return sds.indexer.HasBlock(block.Hash(), block.NumberU64())
}

// Claim exclusive access for state diffing the specified block.
// Returns true and a function to release access if successful, else false, nil.
func (sds *Service) claimExclusiveAccess(block *types.Block) (bool, func()) {
	sds.currentBlocksMutex.Lock()
	defer sds.currentBlocksMutex.Unlock()

	key := fmt.Sprintf("%s,%d", block.Hash().Hex(), block.NumberU64())
	if sds.currentBlocks[key] {
		return false, nil
	}
	sds.currentBlocks[key] = true
	return true, func() {
		sds.currentBlocksMutex.Lock()
		defer sds.currentBlocksMutex.Unlock()
		delete(sds.currentBlocks, key)
	}
}

// statediffInProgress returns true if statediffing is currently in progress for the block, else false.
func (sds *Service) statediffInProgress(block *types.Block) bool {
	sds.currentBlocksMutex.Lock()
	defer sds.currentBlocksMutex.Unlock()

	key := fmt.Sprintf("%s,%d", block.Hash().Hex(), block.NumberU64())
	return sds.currentBlocks[key]
}

// Writes a state diff from the current block, parent state root, and provided params
func (sds *Service) writeStateDiff(block *types.Block, parentRoot common.Hash, params Params) error {
	if granted, relinquish := sds.claimExclusiveAccess(block); granted {
		defer relinquish()
	} else {
		log.Info("Not writing, statediff in progress.", "number", block.NumberU64(), "hash", block.Hash().Hex())
		return nil
	}

	if done, _ := sds.indexer.HasBlock(block.Hash(), block.NumberU64()); done {
		log.Info("Not writing, statediff already done.", "number", block.NumberU64(), "hash", block.Hash().Hex())
		return nil
	}

	var totalDifficulty *big.Int
	var receipts types.Receipts
	var err error
	var tx interfaces.Batch
	start, logger := countStateDiffBegin(block)
	defer countStateDiffEnd(start, logger, &err)
	if params.IncludeTD {
		totalDifficulty = sds.BlockChain.GetTd(block.Hash(), block.NumberU64())
	}
	if params.IncludeReceipts {
		receipts = sds.BlockChain.GetReceiptsByHash(block.Hash())
	}
	tx, err = sds.indexer.PushBlock(block, receipts, totalDifficulty)
	if err != nil {
		return err
	}

	output := func(node types2.StateLeafNode) error {
		defer func() {
			// This is very noisy so we log at Trace.
			since := metrics.UpdateDuration(time.Now(), metrics.IndexerMetrics.OutputTimer)
			logger.Trace(fmt.Sprintf("statediff output duration=%dms", since.Milliseconds()))
		}()
		return sds.indexer.PushStateNode(tx, node, block.Hash().String())
	}
	ipldOutput := func(c types2.IPLD) error {
		defer metrics.ReportAndUpdateDuration("statediff ipldOutput", time.Now(), logger, metrics.IndexerMetrics.IPLDOutputTimer)
		return sds.indexer.PushIPLD(tx, c)
	}

	err = sds.Builder.WriteStateDiffObject(Args{
		NewStateRoot: block.Root(),
		OldStateRoot: parentRoot,
		BlockHash:    block.Hash(),
		BlockNumber:  block.Number(),
	}, params, output, ipldOutput)
	// TODO this anti-pattern needs to be sorted out eventually
	if err = tx.Submit(err); err != nil {
		return fmt.Errorf("batch transaction submission failed: %w", err)
	}

	// allow dereferencing of parent, keep current locked as it should be the next parent
	sds.BlockChain.UnlockTrie(parentRoot)
	return nil
}

// Wrapper function on writeStateDiff to retry when the deadlock is detected.
func (sds *Service) writeStateDiffWithRetry(block *types.Block, parentRoot common.Hash, params Params) error {
	var err error
	for i := uint(0); i < sds.maxRetry; i++ {
		err = sds.writeStateDiff(block, parentRoot, params)
		if err != nil && strings.Contains(err.Error(), deadlockDetected) {
			// Retry only when the deadlock is detected.
			if i+1 < sds.maxRetry {
				log.Warn("dead lock detected while writing statediff", "err", err, "retry number", i)
			}
			continue
		}
		break
	}
	return err
}

// SubscribeWriteStatus is used by the API to subscribe to the job status updates
func (sds *Service) SubscribeWriteStatus(id rpc.ID, sub chan<- JobStatus, quitChan chan<- bool) {
	log.Info("Subscribing to job status updates", "subscription id", id)
	sds.Lock()
	sds.jobStatusSubs[id] = statusSubscription{
		statusChan: sub,
		quitChan:   quitChan,
	}
	sds.Unlock()
}

// UnsubscribeWriteStatus is used to unsubscribe from job status updates
func (sds *Service) UnsubscribeWriteStatus(id rpc.ID) error {
	log.Info("Unsubscribing from job status updates", "subscription id", id)
	sds.Lock()
	close(sds.jobStatusSubs[id].quitChan)
	delete(sds.jobStatusSubs, id)
	sds.Unlock()
	return nil
}

// StreamCodeAndCodeHash subscription method for extracting all the codehash=>code mappings that exist in the trie at the provided height
func (sds *Service) StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- types2.CodeAndCodeHash, quitChan chan<- bool) {
	current := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending code and codehash", "block height", blockNumber)
	currentTrie, err := sds.BlockChain.StateCache().OpenTrie(current.Root())
	if err != nil {
		log.Error("error creating trie for block", "block height", current.Number(), "err", err)
		close(quitChan)
		return
	}
	it := currentTrie.NodeIterator([]byte{})
	leafIt := trie.NewIterator(it)
	go func() {
		defer close(quitChan)
		for leafIt.Next() {
			select {
			case <-sds.QuitChan:
				return
			default:
			}
			account := new(types.StateAccount)
			if err := rlp.DecodeBytes(leafIt.Value, account); err != nil {
				log.Error("error decoding state account", "err", err)
				return
			}
			codeHash := common.BytesToHash(account.CodeHash)
			code, err := sds.BlockChain.StateCache().ContractCode(common.Hash{}, codeHash)
			if err != nil {
				log.Error("error collecting contract code", "err", err)
				return
			}
			outChan <- types2.CodeAndCodeHash{
				Hash: codeHash,
				Code: code,
			}
		}
	}()
}

// WatchAddress performs one of following operations on the watched addresses in writeLoopParams and the db:
// add | remove | set | clear
func (sds *Service) WatchAddress(operation types2.OperationType, args []types2.WatchAddressArg) error {
	// lock writeLoopParams for a write
	writeLoopParams.Lock()
	defer writeLoopParams.Unlock()

	// get the current block number
	currentBlockNumber := sds.BlockChain.CurrentBlock().Number

	switch operation {
	case types2.Add:
		// filter out args having an already watched address with a warning
		filteredArgs, ok := funk.Filter(args, func(arg types2.WatchAddressArg) bool {
			if funk.Contains(writeLoopParams.WatchedAddresses, common.HexToAddress(arg.Address)) {
				log.Warn("Address already being watched", "address", arg.Address)
				return false
			}
			return true
		}).([]types2.WatchAddressArg)
		if !ok {
			return fmt.Errorf("add: filtered args %s", typeAssertionFailed)
		}

		// get addresses from the filtered args
		filteredAddresses, err := MapWatchAddressArgsToAddresses(filteredArgs)
		if err != nil {
			return fmt.Errorf("add: filtered addresses %s", err.Error())
		}

		// update the db
		if sds.indexer != nil {
			err = sds.indexer.InsertWatchedAddresses(filteredArgs, currentBlockNumber)
			if err != nil {
				return err
			}
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = append(writeLoopParams.WatchedAddresses, filteredAddresses...)
		writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case types2.Remove:
		// get addresses from args
		argAddresses, err := MapWatchAddressArgsToAddresses(args)
		if err != nil {
			return fmt.Errorf("remove: mapped addresses %s", err.Error())
		}

		// remove the provided addresses from currently watched addresses
		addresses, ok := funk.Subtract(writeLoopParams.WatchedAddresses, argAddresses).([]common.Address)
		if !ok {
			return fmt.Errorf("remove: filtered addresses %s", typeAssertionFailed)
		}

		// update the db
		if sds.indexer != nil {
			err = sds.indexer.RemoveWatchedAddresses(args)
			if err != nil {
				return err
			}
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = addresses
		writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case types2.Set:
		// get addresses from args
		argAddresses, err := MapWatchAddressArgsToAddresses(args)
		if err != nil {
			return fmt.Errorf("set: mapped addresses %s", err.Error())
		}

		// update the db
		if sds.indexer != nil {
			err = sds.indexer.SetWatchedAddresses(args, currentBlockNumber)
			if err != nil {
				return err
			}
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = argAddresses
		writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case types2.Clear:
		// update the db
		if sds.indexer != nil {
			err := sds.indexer.ClearWatchedAddresses()
			if err != nil {
				return err
			}
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = []common.Address{}
		writeLoopParams.ComputeWatchedAddressesLeafPaths()

	default:
		return fmt.Errorf("%s %s", unexpectedOperation, operation)
	}

	return nil
}

// loadWatchedAddresses loads watched addresses to in-memory write loop params
func loadWatchedAddresses(indexer interfaces.StateDiffIndexer) error {
	watchedAddresses, err := indexer.LoadWatchedAddresses()
	if err != nil {
		return err
	}

	writeLoopParams.Lock()
	defer writeLoopParams.Unlock()

	writeLoopParams.WatchedAddresses = watchedAddresses
	writeLoopParams.ComputeWatchedAddressesLeafPaths()

	return nil
}

// MapWatchAddressArgsToAddresses maps []WatchAddressArg to corresponding []common.Address
func MapWatchAddressArgsToAddresses(args []types2.WatchAddressArg) ([]common.Address, error) {
	addresses, ok := funk.Map(args, func(arg types2.WatchAddressArg) common.Address {
		return common.HexToAddress(arg.Address)
	}).([]common.Address)
	if !ok {
		return nil, fmt.Errorf(typeAssertionFailed)
	}

	return addresses, nil
}

// Backfill is executed on startup to make sure there are no gaps in the recent past when tracking head.
func (sds *Service) Backfill() {
	pos := sds.currentPosition()
	if pos.chainBlockNumber == 0 {
		log.Info("Backfill: At start of chain, nothing to backfill.")
		return
	}

	log.Info(
		"Backfill: initial positions",
		"chain", pos.chainBlockNumber,
		"indexer", pos.indexerBlockNumber,
	)

	if sds.backfillCheckPastBlocks > 0 {
		var gapCheckBeginNumber uint64 = 0
		if pos.indexerBlockNumber > sds.backfillCheckPastBlocks {
			gapCheckBeginNumber = pos.indexerBlockNumber - sds.backfillCheckPastBlocks
		}
		blockGaps, err := sds.indexer.DetectGaps(gapCheckBeginNumber, pos.chainBlockNumber)
		if nil != err {
			log.Error("Backfill error: " + err.Error())
			return
		}

		if nil != blockGaps && len(blockGaps) > 0 {
			gapsMsg, _ := json.Marshal(blockGaps)
			log.Info("Backfill: detected gaps in range", "begin", gapCheckBeginNumber, "end", pos.chainBlockNumber, "gaps", string(gapsMsg))
			sds.backfillDetectedGaps(blockGaps)
			log.Info("Backfill: done processing detected gaps in range", "begin", gapCheckBeginNumber, "end", pos.chainBlockNumber, "gaps", string(gapsMsg))
		} else {
			log.Info("Backfill: no gaps detected in range", "begin", gapCheckBeginNumber, "end", pos.chainBlockNumber)
		}
	}
}

// backfillDetectedGaps fills gaps which have occurred in the recent past.  These gaps can happen because of
// transient errors, such as DB errors that are later corrected (so head statediffing continues, but with a hole)
// a missed ChainEvent (happens sometimes when debugging), or if the process is terminated when an earlier block
// is still in-flight, but a later block was already written.
func (sds *Service) backfillDetectedGaps(blockGaps []*interfaces.BlockGap) {
	var ch = make(chan uint64)
	var wg sync.WaitGroup
	for i := uint(0); i < sds.numWorkers; i++ {
		wg.Add(1)
		go func(w uint) {
			defer wg.Done()
			for {
				select {
				case num, ok := <-ch:
					if !ok {
						log.Info("Backfill: detected gap fill done", "worker", w)
						return
					}
					log.Info("Backfill: backfilling detected gap", "block", num, "worker", w)
					err := sds.writeStateDiffAt(num, writeLoopParams.CopyParams())
					if err != nil {
						log.Error("Backfill error: " + err.Error())
					}
				case <-sds.QuitChan:
					log.Info("Backfill: quitting before finish", "worker", w)
					return
				}
			}
		}(i)
	}

	for _, gap := range blockGaps {
		for num := gap.FirstMissing; num <= gap.LastMissing; num++ {
			ch <- num
		}
	}
	close(ch)
	wg.Wait()
}

// currentPosition returns the current block height for both the BlockChain and the statediff indexer.
func (sds *Service) currentPosition() servicePosition {
	ret := servicePosition{}
	chainBlock := sds.BlockChain.CurrentBlock()
	if nil != chainBlock {
		ret.chainBlockNumber = chainBlock.Number.Uint64()
	}

	indexerBlock, _ := sds.indexer.CurrentBlock()
	if nil != indexerBlock {
		indexerBlockNumber, err := strconv.ParseUint(indexerBlock.BlockNumber, 10, 64)
		if nil == err {
			ret.indexerBlockNumber = indexerBlockNumber
		} else {
			log.Error("Error parsing indexer block number", "block", indexerBlock.BlockNumber)
		}
	}

	return ret
}
