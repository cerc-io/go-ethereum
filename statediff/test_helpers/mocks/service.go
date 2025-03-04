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
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/thoas/go-funk"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var (
	typeAssertionFailed = "type assertion failed"
	unexpectedOperation = "unexpected operation"
)

var _ statediff.IService = &MockStateDiffService{}

// MockStateDiffService is a mock state diff service
type MockStateDiffService struct {
	sync.Mutex
	Builder           statediff.Builder
	BlockChain        *BlockChain
	ReturnProtocol    []p2p.Protocol
	ReturnAPIs        []rpc.API
	BlockChan         chan *types.Block
	ParentBlockChan   chan *types.Block
	QuitChan          chan bool
	Subscriptions     map[common.Hash]map[rpc.ID]statediff.Subscription
	SubscriptionTypes map[common.Hash]statediff.Params
	Indexer           interfaces.StateDiffIndexer
	writeLoopParams   statediff.ParamsWithMutex
}

// Protocols mock method
func (sds *MockStateDiffService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs mock method
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

// Loop mock method
func (sds *MockStateDiffService) Loop(chan core.ChainEvent) {
	//loop through chain events until no more
	for {
		select {
		case block := <-sds.BlockChan:
			currentBlock := block
			parentBlock := <-sds.ParentBlockChan
			parentHash := parentBlock.Hash()
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block",
					"parent block hash", parentHash.String(),
					"current block number", currentBlock.Number())
				continue
			}
			sds.streamStateDiff(currentBlock, parentBlock.Root())
		case <-sds.QuitChan:
			log.Debug("Quitting the statediff block channel")
			sds.close()
			return
		}
	}
}

// streamStateDiff method builds the state diff payload for each subscription according to their subscription type and sends them the result
func (sds *MockStateDiffService) streamStateDiff(currentBlock *types.Block, parentRoot common.Hash) {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		params, ok := sds.SubscriptionTypes[ty]
		if !ok {
			log.Error(fmt.Sprintf("subscriptions type %s do not have a parameter set associated with them", ty.Hex()))
			sds.closeType(ty)
			continue
		}
		// create payload for this subscription type
		payload, err := sds.processStateDiff(currentBlock, parentRoot, params)
		if err != nil {
			log.Error(fmt.Sprintf("statediff processing error for subscriptions with parameters: %+v", params))
			sds.closeType(ty)
			continue
		}
		for id, sub := range subs {
			select {
			case sub.PayloadChan <- *payload:
				log.Debug(fmt.Sprintf("sending statediff payload to subscription %s", id))
			default:
				log.Info(fmt.Sprintf("unable to send statediff payload to subscription %s; channel has no receiver", id))
			}
		}
	}
	sds.Unlock()
}

// StateDiffAt mock method
func (sds *MockStateDiffService) StateDiffAt(blockNumber uint64, params statediff.Params) (*statediff.Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info(fmt.Sprintf("sending state diff at %d", blockNumber))
	if blockNumber == 0 {
		return sds.processStateDiff(currentBlock, common.Hash{}, params)
	}
	parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
	return sds.processStateDiff(currentBlock, parentBlock.Root(), params)
}

// StateDiffFor mock method
func (sds *MockStateDiffService) StateDiffFor(blockHash common.Hash, params statediff.Params) (*statediff.Payload, error) {
	// TODO: something useful here
	return nil, nil
}

// processStateDiff method builds the state diff payload from the current block, parent state root, and provided params
func (sds *MockStateDiffService) processStateDiff(currentBlock *types.Block, parentRoot common.Hash, params statediff.Params) (*statediff.Payload, error) {
	stateDiff, err := sds.Builder.BuildStateDiffObject(statediff.Args{
		NewStateRoot: currentBlock.Root(),
		OldStateRoot: parentRoot,
		BlockHash:    currentBlock.Hash(),
		BlockNumber:  currentBlock.Number(),
	}, params)
	if err != nil {
		return nil, err
	}
	stateDiffRlp, err := rlp.EncodeToBytes(&stateDiff)
	if err != nil {
		return nil, err
	}
	return sds.newPayload(stateDiffRlp, currentBlock, params)
}

func (sds *MockStateDiffService) newPayload(stateObject []byte, block *types.Block, params statediff.Params) (*statediff.Payload, error) {
	payload := &statediff.Payload{
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

// WriteStateDiffAt mock method
func (sds *MockStateDiffService) WriteStateDiffAt(blockNumber uint64, params statediff.Params) statediff.JobID {
	// TODO: something useful here
	return 0
}

// WriteStateDiffFor mock method
func (sds *MockStateDiffService) WriteStateDiffFor(blockHash common.Hash, params statediff.Params) error {
	// TODO: something useful here
	return nil
}

// Loop mock method
func (sds *MockStateDiffService) WriteLoop(chan core.ChainEvent) {
	//loop through chain events until no more
	for {
		select {
		case block := <-sds.BlockChan:
			currentBlock := block
			parentBlock := <-sds.ParentBlockChan
			parentHash := parentBlock.Hash()
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block",
					"parent block hash", parentHash.String(),
					"current block number", currentBlock.Number())
				continue
			}
			// TODO:
			// sds.writeStateDiff(currentBlock, parentBlock.Root(), statediff.Params{})
		case <-sds.QuitChan:
			log.Debug("Quitting the statediff block channel")
			sds.close()
			return
		}
	}
}

// Subscribe is used by the API to subscribe to the service loop
func (sds *MockStateDiffService) Subscribe(id rpc.ID, sub chan<- statediff.Payload, quitChan chan<- bool, params statediff.Params) {
	// Subscription type is defined as the hash of the rlp-serialized subscription params
	by, err := rlp.EncodeToBytes(&params)
	if err != nil {
		return
	}
	subscriptionType := crypto.Keccak256Hash(by)
	// Add subscriber
	sds.Lock()
	if sds.Subscriptions[subscriptionType] == nil {
		sds.Subscriptions[subscriptionType] = make(map[rpc.ID]statediff.Subscription)
	}
	sds.Subscriptions[subscriptionType][id] = statediff.Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.SubscriptionTypes[subscriptionType] = params
	sds.Unlock()
}

// Unsubscribe is used to unsubscribe from the service loop
func (sds *MockStateDiffService) Unsubscribe(id rpc.ID) error {
	sds.Lock()
	for ty := range sds.Subscriptions {
		delete(sds.Subscriptions[ty], id)
		if len(sds.Subscriptions[ty]) == 0 {
			// If we removed the last subscription of this type, remove the subscription type outright
			delete(sds.Subscriptions, ty)
			delete(sds.SubscriptionTypes, ty)
		}
	}
	sds.Unlock()
	return nil
}

// close is used to close all listening subscriptions
func (sds *MockStateDiffService) close() {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		for id, sub := range subs {
			select {
			case sub.QuitChan <- true:
				log.Info(fmt.Sprintf("closing subscription %s", id))
			default:
				log.Info(fmt.Sprintf("unable to close subscription %s; channel has no receiver", id))
			}
			delete(sds.Subscriptions[ty], id)
		}
		delete(sds.Subscriptions, ty)
		delete(sds.SubscriptionTypes, ty)
	}
	sds.Unlock()
}

// Start mock method
func (sds *MockStateDiffService) Start() error {
	log.Info("Starting mock statediff service")
	if sds.ParentBlockChan == nil || sds.BlockChan == nil {
		return errors.New("MockStateDiffingService needs to be configured with a MockParentBlockChan and MockBlockChan")
	}
	chainEventCh := make(chan core.ChainEvent, 10)
	go sds.Loop(chainEventCh)

	return nil
}

// Stop mock method
func (sds *MockStateDiffService) Stop() error {
	log.Info("Stopping mock statediff service")
	close(sds.QuitChan)
	return nil
}

// closeType is used to close all subscriptions of given type
// closeType needs to be called with subscription access locked
func (sds *MockStateDiffService) closeType(subType common.Hash) {
	subs := sds.Subscriptions[subType]
	for id, sub := range subs {
		sendNonBlockingQuit(id, sub)
	}
	delete(sds.Subscriptions, subType)
	delete(sds.SubscriptionTypes, subType)
}

func (sds *MockStateDiffService) StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- sdtypes.CodeAndCodeHash, quitChan chan<- bool) {
	panic("implement me")
}

func sendNonBlockingQuit(id rpc.ID, sub statediff.Subscription) {
	select {
	case sub.QuitChan <- true:
		log.Info(fmt.Sprintf("closing subscription %s", id))
	default:
		log.Info("unable to close subscription %s; channel has no receiver", id)
	}
}

// Performs one of following operations on the watched addresses in writeLoopParams and the db:
// add | remove | set | clear
func (sds *MockStateDiffService) WatchAddress(operation sdtypes.OperationType, args []sdtypes.WatchAddressArg) error {
	// lock writeLoopParams for a write
	sds.writeLoopParams.Lock()
	defer sds.writeLoopParams.Unlock()

	// get the current block number
	currentBlockNumber := sds.BlockChain.CurrentBlock().Number

	switch operation {
	case sdtypes.Add:
		// filter out args having an already watched address with a warning
		filteredArgs, ok := funk.Filter(args, func(arg sdtypes.WatchAddressArg) bool {
			if funk.Contains(sds.writeLoopParams.WatchedAddresses, common.HexToAddress(arg.Address)) {
				log.Warn("Address already being watched", "address", arg.Address)
				return false
			}
			return true
		}).([]sdtypes.WatchAddressArg)
		if !ok {
			return fmt.Errorf("add: filtered args %s", typeAssertionFailed)
		}

		// get addresses from the filtered args
		filteredAddresses, err := statediff.MapWatchAddressArgsToAddresses(filteredArgs)
		if err != nil {
			return fmt.Errorf("add: filtered addresses %s", err.Error())
		}

		// update the db
		err = sds.Indexer.InsertWatchedAddresses(filteredArgs, currentBlockNumber)
		if err != nil {
			return err
		}

		// update in-memory params
		sds.writeLoopParams.WatchedAddresses = append(sds.writeLoopParams.WatchedAddresses, filteredAddresses...)
		sds.writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case sdtypes.Remove:
		// get addresses from args
		argAddresses, err := statediff.MapWatchAddressArgsToAddresses(args)
		if err != nil {
			return fmt.Errorf("remove: mapped addresses %s", err.Error())
		}

		// remove the provided addresses from currently watched addresses
		addresses, ok := funk.Subtract(sds.writeLoopParams.WatchedAddresses, argAddresses).([]common.Address)
		if !ok {
			return fmt.Errorf("remove: filtered addresses %s", typeAssertionFailed)
		}

		// update the db
		err = sds.Indexer.RemoveWatchedAddresses(args)
		if err != nil {
			return err
		}

		// update in-memory params
		sds.writeLoopParams.WatchedAddresses = addresses
		sds.writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case sdtypes.Set:
		// get addresses from args
		argAddresses, err := statediff.MapWatchAddressArgsToAddresses(args)
		if err != nil {
			return fmt.Errorf("set: mapped addresses %s", err.Error())
		}

		// update the db
		err = sds.Indexer.SetWatchedAddresses(args, currentBlockNumber)
		if err != nil {
			return err
		}

		// update in-memory params
		sds.writeLoopParams.WatchedAddresses = argAddresses
		sds.writeLoopParams.ComputeWatchedAddressesLeafPaths()
	case sdtypes.Clear:
		// update the db
		err := sds.Indexer.ClearWatchedAddresses()
		if err != nil {
			return err
		}

		// update in-memory params
		sds.writeLoopParams.WatchedAddresses = []common.Address{}
		sds.writeLoopParams.ComputeWatchedAddressesLeafPaths()

	default:
		return fmt.Errorf("%s %s", unexpectedOperation, operation)
	}

	return nil
}

// SubscribeWriteStatus is used by the API to subscribe to the job status updates
func (sds *MockStateDiffService) SubscribeWriteStatus(id rpc.ID, sub chan<- statediff.JobStatus, quitChan chan<- bool) {
	// TODO when WriteStateDiff methods are implemented
}

// UnsubscribeWriteStatus is used to unsubscribe from job status updates
func (sds *MockStateDiffService) UnsubscribeWriteStatus(id rpc.ID) error {
	// TODO when WriteStateDiff methods are implemented
	return nil
}
