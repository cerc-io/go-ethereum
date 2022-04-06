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

package statediff_test

import (
	"bytes"
	"math/big"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	statediff "github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/test_helpers/mocks"
	types2 "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

func TestServiceLoop(t *testing.T) {
	testErrorInChainEventLoop(t)
	testErrorInBlockLoop(t)
}

var (
	eventsChannel = make(chan core.ChainEvent, 1)

	parentRoot1   = common.HexToHash("0x01")
	parentRoot2   = common.HexToHash("0x02")
	parentHeader1 = types.Header{Number: big.NewInt(rand.Int63()), Root: parentRoot1}
	parentHeader2 = types.Header{Number: big.NewInt(rand.Int63()), Root: parentRoot2}

	parentBlock1 = types.NewBlock(&parentHeader1, nil, nil, nil, new(trie.Trie))
	parentBlock2 = types.NewBlock(&parentHeader2, nil, nil, nil, new(trie.Trie))

	parentHash1 = parentBlock1.Hash()
	parentHash2 = parentBlock2.Hash()

	testRoot1 = common.HexToHash("0x03")
	testRoot2 = common.HexToHash("0x04")
	testRoot3 = common.HexToHash("0x04")
	header1   = types.Header{ParentHash: parentHash1, Root: testRoot1, Number: big.NewInt(1)}
	header2   = types.Header{ParentHash: parentHash2, Root: testRoot2, Number: big.NewInt(2)}
	header3   = types.Header{ParentHash: common.HexToHash("parent hash"), Root: testRoot3, Number: big.NewInt(3)}

	testBlock1 = types.NewBlock(&header1, nil, nil, nil, new(trie.Trie))
	testBlock2 = types.NewBlock(&header2, nil, nil, nil, new(trie.Trie))
	testBlock3 = types.NewBlock(&header3, nil, nil, nil, new(trie.Trie))

	receiptRoot1  = common.HexToHash("0x05")
	receiptRoot2  = common.HexToHash("0x06")
	receiptRoot3  = common.HexToHash("0x07")
	testReceipts1 = []*types.Receipt{types.NewReceipt(receiptRoot1.Bytes(), false, 1000), types.NewReceipt(receiptRoot2.Bytes(), false, 2000)}
	testReceipts2 = []*types.Receipt{types.NewReceipt(receiptRoot3.Bytes(), false, 3000)}

	event1 = core.ChainEvent{Block: testBlock1}
	event2 = core.ChainEvent{Block: testBlock2}
	event3 = core.ChainEvent{Block: testBlock3}

	defaultParams = statediff.Params{
		IncludeBlock:    true,
		IncludeReceipts: true,
		IncludeTD:       true,
	}
)

func testErrorInChainEventLoop(t *testing.T) {
	//the first chain event causes and error (in blockchain mock)
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	serviceQuit := make(chan bool)
	service := statediff.Service{
		Mutex:             sync.Mutex{},
		Builder:           &builder,
		BlockChain:        &blockChain,
		QuitChan:          serviceQuit,
		Subscriptions:     make(map[common.Hash]map[rpc.ID]statediff.Subscription),
		SubscriptionTypes: make(map[common.Hash]statediff.Params),
		BlockCache:        statediff.NewBlockCache(1),
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan, defaultParams)
	testRoot2 = common.HexToHash("0xTestRoot2")
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockMapping[parentBlock2.Hash()] = parentBlock2
	blockChain.SetBlocksForHashes(blockMapping)
	blockChain.SetChainEvents([]core.ChainEvent{event1, event2, event3})
	blockChain.SetReceiptsForHash(testBlock1.Hash(), testReceipts1)
	blockChain.SetReceiptsForHash(testBlock2.Hash(), testReceipts2)

	payloads := make([]statediff.Payload, 0, 2)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		for i := 0; i < 2; i++ {
			select {
			case payload := <-payloadChan:
				payloads = append(payloads, payload)
			case <-quitChan:
			}
		}
		wg.Done()
	}()
	service.Loop(eventsChannel)
	wg.Wait()
	if len(payloads) != 2 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 3", len(payloads))
	}

	testReceipts1Rlp, err := rlp.EncodeToBytes(&testReceipts1)
	if err != nil {
		t.Error(err)
	}
	testReceipts2Rlp, err := rlp.EncodeToBytes(&testReceipts2)
	if err != nil {
		t.Error(err)
	}
	expectedReceiptsRlp := [][]byte{testReceipts1Rlp, testReceipts2Rlp, nil}
	for i, payload := range payloads {
		if !bytes.Equal(payload.ReceiptsRlp, expectedReceiptsRlp[i]) {
			t.Error("Test failure:", t.Name())
			t.Logf("Actual receipt rlp for payload %d does not equal expected.\nactual: %+v\nexpected: %+v", i, payload.ReceiptsRlp, expectedReceiptsRlp[i])
		}
	}

	defaultParams.ComputeWatchedAddressesLeafKeys()
	if !reflect.DeepEqual(builder.Params, defaultParams) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual params does not equal expected.\nactual:%+v\nexpected: %+v", builder.Params, defaultParams)
	}
	if !bytes.Equal(builder.Args.BlockHash.Bytes(), testBlock2.Hash().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual blockhash does not equal expected.\nactual:%x\nexpected: %x", builder.Args.BlockHash.Bytes(), testBlock2.Hash().Bytes())
	}
	if !bytes.Equal(builder.Args.OldStateRoot.Bytes(), parentBlock2.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual root does not equal expected.\nactual:%x\nexpected: %x", builder.Args.OldStateRoot.Bytes(), parentBlock2.Root().Bytes())
	}
	if !bytes.Equal(builder.Args.NewStateRoot.Bytes(), testBlock2.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual root does not equal expected.\nactual:%x\nexpected: %x", builder.Args.NewStateRoot.Bytes(), testBlock2.Root().Bytes())
	}
	//look up the parent block from its hash
	expectedHashes := []common.Hash{testBlock1.ParentHash(), testBlock2.ParentHash()}
	if !reflect.DeepEqual(blockChain.HashesLookedUp, expectedHashes) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual looked up parent hashes does not equal expected.\nactual:%+v\nexpected: %+v", blockChain.HashesLookedUp, expectedHashes)
	}
}

func testErrorInBlockLoop(t *testing.T) {
	//second block's parent block can't be found
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Builder:           &builder,
		BlockChain:        &blockChain,
		QuitChan:          make(chan bool),
		Subscriptions:     make(map[common.Hash]map[rpc.ID]statediff.Subscription),
		SubscriptionTypes: make(map[common.Hash]statediff.Params),
		BlockCache:        statediff.NewBlockCache(1),
	}
	payloadChan := make(chan statediff.Payload)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan, defaultParams)
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockChain.SetBlocksForHashes(blockMapping)
	blockChain.SetChainEvents([]core.ChainEvent{event1, event2})
	// Need to have listeners on the channels or the subscription will be closed and the processing halted
	go func() {
		select {
		case <-payloadChan:
		case <-quitChan:
		}
	}()
	service.Loop(eventsChannel)

	defaultParams.ComputeWatchedAddressesLeafKeys()
	if !reflect.DeepEqual(builder.Params, defaultParams) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual params does not equal expected.\nactual:%+v\nexpected: %+v", builder.Params, defaultParams)
	}
	if !bytes.Equal(builder.Args.BlockHash.Bytes(), testBlock1.Hash().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual blockhash does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.BlockHash.Bytes(), testBlock1.Hash().Bytes())
	}
	if !bytes.Equal(builder.Args.OldStateRoot.Bytes(), parentBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual old state root does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.OldStateRoot.Bytes(), parentBlock1.Root().Bytes())
	}
	if !bytes.Equal(builder.Args.NewStateRoot.Bytes(), testBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual new state root does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.NewStateRoot.Bytes(), testBlock1.Root().Bytes())
	}
}

func TestGetStateDiffAt(t *testing.T) {
	testErrorInStateDiffAt(t)
}

func testErrorInStateDiffAt(t *testing.T) {
	mockStateDiff := types2.StateObject{
		BlockNumber: testBlock1.Number(),
		BlockHash:   testBlock1.Hash(),
	}
	expectedStateDiffRlp, err := rlp.EncodeToBytes(&mockStateDiff)
	if err != nil {
		t.Error(err)
	}
	expectedReceiptsRlp, err := rlp.EncodeToBytes(&testReceipts1)
	if err != nil {
		t.Error(err)
	}
	expectedBlockRlp, err := rlp.EncodeToBytes(testBlock1)
	if err != nil {
		t.Error(err)
	}
	expectedStateDiffPayload := statediff.Payload{
		StateObjectRlp: expectedStateDiffRlp,
		ReceiptsRlp:    expectedReceiptsRlp,
		BlockRlp:       expectedBlockRlp,
	}
	expectedStateDiffPayloadRlp, err := rlp.EncodeToBytes(&expectedStateDiffPayload)
	if err != nil {
		t.Error(err)
	}
	builder := mocks.Builder{}
	builder.SetStateDiffToBuild(mockStateDiff)
	blockChain := mocks.BlockChain{}
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockChain.SetBlocksForHashes(blockMapping)
	blockChain.SetBlockForNumber(testBlock1, testBlock1.NumberU64())
	blockChain.SetReceiptsForHash(testBlock1.Hash(), testReceipts1)
	service := statediff.Service{
		Mutex:             sync.Mutex{},
		Builder:           &builder,
		BlockChain:        &blockChain,
		QuitChan:          make(chan bool),
		Subscriptions:     make(map[common.Hash]map[rpc.ID]statediff.Subscription),
		SubscriptionTypes: make(map[common.Hash]statediff.Params),
		BlockCache:        statediff.NewBlockCache(1),
	}
	stateDiffPayload, err := service.StateDiffAt(testBlock1.NumberU64(), defaultParams)
	if err != nil {
		t.Error(err)
	}
	stateDiffPayloadRlp, err := rlp.EncodeToBytes(stateDiffPayload)
	if err != nil {
		t.Error(err)
	}

	defaultParams.ComputeWatchedAddressesLeafKeys()
	if !reflect.DeepEqual(builder.Params, defaultParams) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual params does not equal expected.\nactual:%+v\nexpected: %+v", builder.Params, defaultParams)
	}
	if !bytes.Equal(builder.Args.BlockHash.Bytes(), testBlock1.Hash().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual blockhash does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.BlockHash.Bytes(), testBlock1.Hash().Bytes())
	}
	if !bytes.Equal(builder.Args.OldStateRoot.Bytes(), parentBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual old state root does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.OldStateRoot.Bytes(), parentBlock1.Root().Bytes())
	}
	if !bytes.Equal(builder.Args.NewStateRoot.Bytes(), testBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual new state root does not equal expected.\nactual:%+v\nexpected: %x", builder.Args.NewStateRoot.Bytes(), testBlock1.Root().Bytes())
	}
	if !bytes.Equal(expectedStateDiffPayloadRlp, stateDiffPayloadRlp) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual state diff payload does not equal expected.\nactual:%+v\nexpected: %+v", expectedStateDiffPayload, stateDiffPayload)
	}
}

func TestWaitForSync(t *testing.T) {
	testWaitForSync(t)
	testGetSyncStatus(t)
}

// This function will create a backend and service object which includes a generic Backend
func createServiceWithMockBackend(curBlock uint64, highestBlock uint64) (*mocks.Backend, *statediff.Service) {
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	backend := mocks.Backend{
		StartingBlock:       1,
		CurrBlock:           curBlock,
		HighestBlock:        highestBlock,
		SyncedAccounts:      5,
		SyncedAccountBytes:  5,
		SyncedBytecodes:     5,
		SyncedBytecodeBytes: 5,
		SyncedStorage:       5,
		SyncedStorageBytes:  5,
		HealedTrienodes:     5,
		HealedTrienodeBytes: 5,
		HealedBytecodes:     5,
		HealedBytecodeBytes: 5,
		HealingTrienodes:    5,
		HealingBytecode:     5,
	}

	service := &statediff.Service{
		Mutex:             sync.Mutex{},
		Builder:           &builder,
		BlockChain:        &blockChain,
		QuitChan:          make(chan bool),
		Subscriptions:     make(map[common.Hash]map[rpc.ID]statediff.Subscription),
		SubscriptionTypes: make(map[common.Hash]statediff.Params),
		BlockCache:        statediff.NewBlockCache(1),
		BackendAPI:        &backend,
		WaitForSync:       true,
	}
	return &backend, service
}

// This function will test to make sure that the state diff waits
// until the blockchain has caught up to head!
func testWaitForSync(t *testing.T) {
	t.Log("Starting Sync")
	_, service := createServiceWithMockBackend(10, 10)
	err := service.WaitingForSync()
	if err != nil {
		t.Fatal("Sync Failed")
	}
	t.Log("Sync Complete")
}

// This test will run the WaitForSync() at the start of the execusion
// It will then incrementally increase the currentBlock to match the highestBlock
// At each interval it will run the GetSyncStatus to ensure that the return value is not false.
// It will also check to make sure that the WaitForSync() function has not completed!
func testGetSyncStatus(t *testing.T) {
	t.Log("Starting Get Sync Status Test")
	var highestBlock uint64 = 5
	// Create a backend and a service
	// the backend is lagging behind the sync.
	backend, service := createServiceWithMockBackend(0, highestBlock)

	checkSyncComplete := make(chan int, 1)

	go func() {
		// Start the sync function which will wait for the sync
		// Once the sync is complete add a value to the checkSyncComplet channel
		t.Log("Starting Sync")
		err := service.WaitingForSync()
		if err != nil {
			t.Error("Sync Failed")
			checkSyncComplete <- 1
		}
		t.Log("We have finally synced!")
		checkSyncComplete <- 0
	}()

	tables := []struct {
		currentBlock uint64
		highestBlock uint64
	}{
		{1, highestBlock},
		{2, highestBlock},
		{3, highestBlock},
		{4, highestBlock},
		{5, highestBlock},
	}

	time.Sleep(2 * time.Second)
	for _, table := range tables {
		// Iterate over each block
		// Once the highest block reaches the current block the sync should complete

		// Update the backend current block value
		t.Log("Updating Current Block to: ", table.currentBlock)
		backend.CurrBlock = table.currentBlock
		pubEthAPI := ethapi.NewPublicEthereumAPI(service.BackendAPI)
		syncStatus, err := service.GetSyncStatus(pubEthAPI)

		if err != nil {
			t.Fatal("Sync Failed")
		}

		time.Sleep(2 * time.Second)

		// Make sure if syncStatus is false that WaitForSync has completed!
		if !syncStatus && len(checkSyncComplete) == 0 {
			t.Error("Sync is complete but WaitForSync is not")
		}

		if syncStatus && len(checkSyncComplete) == 1 {
			t.Error("Sync is not complete but WaitForSync is")
		}

		// Make sure sync hasn't completed and that the checkSyncComplete channel is empty
		if syncStatus && len(checkSyncComplete) == 0 {
			continue
		}

		// This code will only be run if the sync is complete and the WaitForSync function is complete

		// If syncstatus is complete, make sure that the blocks match
		if !syncStatus && table.currentBlock != table.highestBlock {
			t.Errorf("syncStatus indicated sync was complete even when current block, %d, and highest block %d aren't equal",
				table.currentBlock, table.highestBlock)
		}

		// Make sure that WaitForSync completed once the current block caught up to head!
		checkSyncCompleteVal := <-checkSyncComplete
		if checkSyncCompleteVal != 0 {
			t.Errorf("syncStatus indicated sync was complete but the checkSyncComplete has a value of %d",
				checkSyncCompleteVal)
		} else {
			t.Log("Test Passed!")
		}

	}

}
