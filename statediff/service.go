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
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	ind "github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
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
		IntermediateStateNodes:   true,
		IntermediateStorageNodes: true,
		IncludeBlock:             true,
		IncludeReceipts:          true,
		IncludeTD:                true,
		IncludeCode:              true,
	},
}

var statediffMetrics = RegisterStatediffMetrics(metrics.DefaultRegistry)

type blockChain interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	CurrentBlock() *types.Block
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
	// Subscribe method to subscribe to receive state diff processing output`
	Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params)
	// Unsubscribe method to unsubscribe from state diff processing
	Unsubscribe(id rpc.ID) error
	// StateDiffAt method to get state diff object at specific block
	StateDiffAt(blockNumber uint64, params Params) (*Payload, error)
	// StateDiffFor method to get state diff object at specific block
	StateDiffFor(blockHash common.Hash, params Params) (*Payload, error)
	// StateTrieAt method to get state trie object at specific block
	StateTrieAt(blockNumber uint64, params Params) (*Payload, error)
	// StreamCodeAndCodeHash method to stream out all code and codehash pairs
	StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- types2.CodeAndCodeHash, quitChan chan<- bool)
	// WriteStateDiffAt method to write state diff object directly to DB
	WriteStateDiffAt(blockNumber uint64, params Params) error
	// WriteStateDiffFor method to write state diff object directly to DB
	WriteStateDiffFor(blockHash common.Hash, params Params) error
	// WriteLoop event loop for progressively processing and writing diffs directly to DB
	WriteLoop(chainEventCh chan core.ChainEvent)
	// Method to change the addresses being watched in write loop params
	WatchAddress(operation types2.OperationType, args []types2.WatchAddressArg) error
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
	// Used to signal if we should check for KnownGaps
	KnownGaps KnownGapsState
	// Whether or not we have any subscribers; only if we do, do we processes state diffs
	subscribers int32
	// Interface for publishing statediffs as PG-IPLD objects
	indexer interfaces.StateDiffIndexer
	// Whether to enable writing state diffs directly to track blockchain head.
	enableWriteLoop bool
	// Size of the worker pool
	numWorkers uint
	// Number of retry for aborted transactions due to deadlock.
	maxRetry uint
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
func New(stack *node.Node, ethServ *eth.Ethereum, cfg *ethconfig.Config, params Config, backend ethapi.Backend) error {
	blockChain := ethServ.BlockChain()
	var indexer interfaces.StateDiffIndexer
	var db sql.Database
	var err error
	quitCh := make(chan bool)
	if params.IndexerConfig != nil {
		info := nodeinfo.Info{
			GenesisBlock: blockChain.Genesis().Hash().Hex(),
			NetworkID:    strconv.FormatUint(cfg.NetworkId, 10),
			ChainID:      blockChain.Config().ChainID.Uint64(),
			ID:           params.ID,
			ClientName:   params.ClientName,
		}
		var err error
		db, indexer, err = ind.NewStateDiffIndexer(params.Context, blockChain.Config(), info, params.IndexerConfig)
		if err != nil {
			return err
		}
		indexer.ReportDBMetrics(10*time.Second, quitCh)
	}

	workers := params.NumWorkers
	if workers == 0 {
		workers = 1
	}
	// If we ever have multiple processingKeys we can update them here
	// along with the expectedDifference
	knownGaps := &KnownGapsState{
		processingKey:          0,
		expectedDifference:     big.NewInt(1),
		errorState:             false,
		writeFilePath:          params.KnownGapsFilePath,
		db:                     db,
		statediffMetrics:       statediffMetrics,
		sqlFileWaitingForWrite: false,
	}
	if params.IndexerConfig.Type() == shared.POSTGRES {
		knownGaps.checkForGaps = true
	} else {
		log.Info("We are not going to check for gaps on start up since we are not connected to Postgres!")
		knownGaps.checkForGaps = false
	}
	sds := &Service{
		Mutex:             sync.Mutex{},
		BlockChain:        blockChain,
		Builder:           NewBuilder(blockChain.StateCache()),
		QuitChan:          quitCh,
		Subscriptions:     make(map[common.Hash]map[rpc.ID]Subscription),
		SubscriptionTypes: make(map[common.Hash]Params),
		BlockCache:        NewBlockCache(workers),
		BackendAPI:        backend,
		WaitForSync:       params.WaitForSync,
		KnownGaps:         *knownGaps,
		indexer:           indexer,
		enableWriteLoop:   params.EnableWriteLoop,
		numWorkers:        workers,
		maxRetry:          defaultRetryLimit,
	}
	stack.RegisterLifecycle(sds)
	stack.RegisterAPIs(sds.APIs())

	err = loadWatchedAddresses(indexer)
	if err != nil {
		return err
	}

	return nil
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
	chainEventCh <-chan core.ChainEvent
	wg           *sync.WaitGroup
	id           uint
}

func (sds *Service) WriteLoop(chainEventCh chan core.ChainEvent) {
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()
	errCh := chainEventSub.Err()
	var wg sync.WaitGroup
	// Process metrics for chain events, then forward to workers
	chainEventFwd := make(chan core.ChainEvent, chainEventChanSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case chainEvent := <-chainEventCh:
				statediffMetrics.lastEventHeight.Update(int64(chainEvent.Block.Number().Uint64()))
				statediffMetrics.writeLoopChannelLen.Update(int64(len(chainEventCh)))
				chainEventFwd <- chainEvent
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
		params := workerParams{chainEventCh: chainEventFwd, wg: &wg, id: worker}
		go sds.writeLoopWorker(params)
	}
	wg.Wait()
}

func (sds *Service) writeGenesisStateDiff(currBlock *types.Block, workerId uint) {
	// For genesis block we need to return the entire state trie hence we diff it with an empty trie.
	log.Info("Writing state diff", "block height", genesisBlockNumber, "worker", workerId)
	writeLoopParams.RLock()
	err := sds.writeStateDiffWithRetry(currBlock, common.Hash{}, writeLoopParams.Params)
	writeLoopParams.RUnlock()
	if err != nil {
		log.Error("statediff.Service.WriteLoop: processing error", "block height",
			genesisBlockNumber, "error", err.Error(), "worker", workerId)
		return
	}
	statediffMetrics.lastStatediffHeight.Update(genesisBlockNumber)
}

func (sds *Service) writeLoopWorker(params workerParams) {
	defer params.wg.Done()
	for {
		select {
		//Notify chain event channel of events
		case chainEvent := <-params.chainEventCh:
			log.Debug("WriteLoop(): chain event received", "event", chainEvent)
			currentBlock := chainEvent.Block
			parentBlock := sds.BlockCache.getParentBlock(currentBlock, sds.BlockChain)
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block", "block height", currentBlock.Number())
				continue
			}

			// chainEvent streams block from block 1, but we also need to include data from the genesis block.
			if parentBlock.Number().Uint64() == genesisBlockNumber {
				sds.writeGenesisStateDiff(parentBlock, params.id)
			}

			// If for any reason we need to check for gaps,
			// Check and update the gaps table.
			if sds.KnownGaps.checkForGaps && !sds.KnownGaps.errorState {
				log.Info("Checking for Gaps at", "current block", currentBlock.Number())
				go sds.KnownGaps.findAndUpdateGaps(currentBlock.Number(), sds.KnownGaps.expectedDifference, sds.KnownGaps.processingKey)
				sds.KnownGaps.checkForGaps = false
			}

			log.Info("Writing state diff", "block height", currentBlock.Number().Uint64(), "worker", params.id)
			writeLoopParams.RLock()
			err := sds.writeStateDiffWithRetry(currentBlock, parentBlock.Root(), writeLoopParams.Params)
			writeLoopParams.RUnlock()
			if err != nil {
				log.Error("statediff.Service.WriteLoop: processing error", "block height", currentBlock.Number().Uint64(), "error", err.Error(), "worker", params.id)
				sds.KnownGaps.errorState = true
				log.Warn("Updating the following block to knownErrorBlocks to be inserted into knownGaps table", "blockNumber", currentBlock.Number())
				sds.KnownGaps.knownErrorBlocks = append(sds.KnownGaps.knownErrorBlocks, currentBlock.Number())
				// Write object to startdiff
				continue
			}
			sds.KnownGaps.errorState = false
			if sds.KnownGaps.knownErrorBlocks != nil {
				// We must pass in parameters by VALUE not reference.
				// If we pass them in my reference, the references can change before the computation is complete!
				staticKnownErrorBlocks := make([]*big.Int, len(sds.KnownGaps.knownErrorBlocks))
				copy(staticKnownErrorBlocks, sds.KnownGaps.knownErrorBlocks)
				sds.KnownGaps.knownErrorBlocks = nil
				go sds.KnownGaps.captureErrorBlocks(staticKnownErrorBlocks)
			}

			if sds.KnownGaps.sqlFileWaitingForWrite {
				log.Info("There are entries in the SQL file for knownGaps that should be written")
				err := sds.KnownGaps.writeSqlFileStmtToDb()
				if err != nil {
					log.Error("Unable to write KnownGap sql file to DB")
				}
			}

			// TODO: how to handle with concurrent workers
			statediffMetrics.lastStatediffHeight.Update(int64(currentBlock.Number().Uint64()))
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
			statediffMetrics.serviceLoopChannelLen.Update(int64(len(chainEventCh)))
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

	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

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

	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

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

// StateTrieAt returns a state trie object payload at the specified blockheight
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateTrieAt(blockNumber uint64, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending state trie", "block height", blockNumber)

	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

	return sds.processStateTrie(currentBlock, params)
}

func (sds *Service) processStateTrie(block *types.Block, params Params) (*Payload, error) {
	stateNodes, err := sds.Builder.BuildStateTrieObject(block)
	if err != nil {
		return nil, err
	}
	stateTrieRlp, err := rlp.EncodeToBytes(&stateNodes)
	if err != nil {
		return nil, err
	}
	log.Info("state trie size", "at block height", block.Number().Uint64(), "rlp byte size", len(stateTrieRlp))
	return sds.newPayload(stateTrieRlp, block, params)
}

// Subscribe is used by the API to subscribe to the service loop
func (sds *Service) Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params) {
	log.Info("Subscribing to the statediff service")
	if atomic.CompareAndSwapInt32(&sds.subscribers, 0, 1) {
		log.Info("State diffing subscription received; beginning statediff processing")
	}

	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

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

// This function will check the status of geth syncing.
// It will return false if geth has finished syncing.
// It will return a true Geth is still syncing.
func (sds *Service) GetSyncStatus(pubEthAPI *ethapi.PublicEthereumAPI) (bool, error) {
	syncStatus, err := pubEthAPI.Syncing()
	if err != nil {
		return true, err
	}

	if syncStatus != false {
		return true, err
	}
	return false, err
}

// This function calls GetSyncStatus to check if we have caught up to head.
// It will keep looking and checking if we have caught up to head.
// It will only complete if we catch up to head, otherwise it will keep looping forever.
func (sds *Service) WaitingForSync() error {
	log.Info("We are going to wait for geth to sync to head!")

	// Has the geth node synced to head?
	Synced := false
	pubEthAPI := ethapi.NewPublicEthereumAPI(sds.BackendAPI)
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

// WriteStateDiffAt writes a state diff at the specific blockheight directly to the database
// This operation cannot be performed back past the point of db pruning; it requires an archival node
// for historical data
func (sds *Service) WriteStateDiffAt(blockNumber uint64, params Params) error {
	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

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
	// compute leaf keys of watched addresses in the params
	params.ComputeWatchedAddressesLeafKeys()

	currentBlock := sds.BlockChain.GetBlockByHash(blockHash)
	parentRoot := common.Hash{}
	if currentBlock.NumberU64() != 0 {
		parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
		parentRoot = parentBlock.Root()
	}
	return sds.writeStateDiffWithRetry(currentBlock, parentRoot, params)
}

// Writes a state diff from the current block, parent state root, and provided params
func (sds *Service) writeStateDiff(block *types.Block, parentRoot common.Hash, params Params) error {
	// log.Info("Writing state diff", "block height", block.Number().Uint64())
	var totalDifficulty *big.Int
	var receipts types.Receipts
	var err error
	var tx interfaces.Batch
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
	// defer handling of commit/rollback for any return case
	defer func() {
		if err := tx.Submit(err); err != nil {
			log.Error("batch transaction submission failed", "err", err)
		}
	}()
	output := func(node types2.StateNode) error {
		return sds.indexer.PushStateNode(tx, node, block.Hash().String())
	}
	codeOutput := func(c types2.CodeAndCodeHash) error {
		return sds.indexer.PushCodeAndCodeHash(tx, c)
	}
	err = sds.Builder.WriteStateDiffObject(types2.StateRoots{
		NewStateRoot: block.Root(),
		OldStateRoot: parentRoot,
	}, params, output, codeOutput)

	// allow dereferencing of parent, keep current locked as it should be the next parent
	sds.BlockChain.UnlockTrie(parentRoot)
	if err != nil {
		return err
	}
	return nil
}

// Wrapper function on writeStateDiff to retry when the deadlock is detected.
func (sds *Service) writeStateDiffWithRetry(block *types.Block, parentRoot common.Hash, params Params) error {
	var err error
	for i := uint(0); i < sds.maxRetry; i++ {
		err = sds.writeStateDiff(block, parentRoot, params)
		if err != nil && strings.Contains(err.Error(), deadlockDetected) {
			// Retry only when the deadlock is detected.
			if i != sds.maxRetry {
				log.Info("dead lock detected while writing statediff", "err", err, "retry number", i)
			}
			continue
		}
		break
	}
	return err
}

// Performs one of following operations on the watched addresses in writeLoopParams and the db:
// add | remove | set | clear
func (sds *Service) WatchAddress(operation types2.OperationType, args []types2.WatchAddressArg) error {
	// lock writeLoopParams for a write
	writeLoopParams.Lock()
	defer writeLoopParams.Unlock()

	// get the current block number
	currentBlockNumber := sds.BlockChain.CurrentBlock().Number()

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
		err = sds.indexer.InsertWatchedAddresses(filteredArgs, currentBlockNumber)
		if err != nil {
			return err
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = append(writeLoopParams.WatchedAddresses, filteredAddresses...)
		funk.ForEach(filteredAddresses, func(address common.Address) {
			writeLoopParams.watchedAddressesLeafKeys[crypto.Keccak256Hash(address.Bytes())] = struct{}{}
		})
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
		err = sds.indexer.RemoveWatchedAddresses(args)
		if err != nil {
			return err
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = addresses
		funk.ForEach(argAddresses, func(address common.Address) {
			delete(writeLoopParams.watchedAddressesLeafKeys, crypto.Keccak256Hash(address.Bytes()))
		})
	case types2.Set:
		// get addresses from args
		argAddresses, err := MapWatchAddressArgsToAddresses(args)
		if err != nil {
			return fmt.Errorf("set: mapped addresses %s", err.Error())
		}

		// update the db
		err = sds.indexer.SetWatchedAddresses(args, currentBlockNumber)
		if err != nil {
			return err
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = argAddresses
		writeLoopParams.ComputeWatchedAddressesLeafKeys()
	case types2.Clear:
		// update the db
		err := sds.indexer.ClearWatchedAddresses()
		if err != nil {
			return err
		}

		// update in-memory params
		writeLoopParams.WatchedAddresses = []common.Address{}
		writeLoopParams.ComputeWatchedAddressesLeafKeys()

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
	writeLoopParams.ComputeWatchedAddressesLeafKeys()

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
