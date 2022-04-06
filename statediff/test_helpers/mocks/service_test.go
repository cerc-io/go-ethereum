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
	"fmt"
	"math/big"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/test_helpers"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var (
	emptyStorage   = make([]sdtypes.StorageNode, 0)
	block0, block1 *types.Block
	minerLeafKey   = test_helpers.AddressToLeafKey(common.HexToAddress("0x0"))
	account1, _    = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    uint64(0),
		Balance:  big.NewInt(10000),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").Bytes(),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
	})
	account1LeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		common.Hex2Bytes("3926db69aaced518e9b9f0f434a473e7174109c943548bb8f23be41ca76d9ad2"),
		account1,
	})
	minerAccount, _ = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    uint64(0),
		Balance:  big.NewInt(2000002625000000000),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").Bytes(),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
	})
	minerAccountLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		common.Hex2Bytes("3380c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312a"),
		minerAccount,
	})
	bankAccount, _ = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    uint64(1),
		Balance:  big.NewInt(1999978999999990000),
		CodeHash: common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").Bytes(),
		Root:     common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
	})
	bankAccountLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		common.Hex2Bytes("30bf49f440a1cd0527e4d06e2765654c0f56452257516d793a9b8d604dcfdf2a"),
		bankAccount,
	})
	mockTotalDifficulty = big.NewInt(1337)
	parameters          = statediff.Params{
		IntermediateStateNodes: false,
		IncludeTD:              true,
		IncludeBlock:           true,
		IncludeReceipts:        true,
	}
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
}

func TestAPI(t *testing.T) {
	testSubscriptionAPI(t)
	testHTTPAPI(t)
	testWatchAddressAPI(t)
}

func testSubscriptionAPI(t *testing.T) {
	blocks, chain := test_helpers.MakeChain(1, test_helpers.Genesis, test_helpers.TestChainGen)
	defer chain.Stop()
	block0 = test_helpers.Genesis
	block1 = blocks[0]
	expectedBlockRlp, _ := rlp.EncodeToBytes(&block1)
	mockReceipt := &types.Receipt{
		BlockNumber: block1.Number(),
		BlockHash:   block1.Hash(),
	}
	expectedReceiptBytes, _ := rlp.EncodeToBytes(&types.Receipts{mockReceipt})
	expectedStateDiff := sdtypes.StateObject{
		BlockNumber: block1.Number(),
		BlockHash:   block1.Hash(),
		Nodes: []sdtypes.StateNode{
			{
				Path:         []byte{'\x05'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      minerLeafKey,
				NodeValue:    minerAccountLeafNode,
				StorageNodes: emptyStorage,
			},
			{
				Path:         []byte{'\x0e'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      test_helpers.Account1LeafKey,
				NodeValue:    account1LeafNode,
				StorageNodes: emptyStorage,
			},
			{
				Path:         []byte{'\x00'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      test_helpers.BankLeafKey,
				NodeValue:    bankAccountLeafNode,
				StorageNodes: emptyStorage,
			},
		},
	}
	expectedStateDiffBytes, _ := rlp.EncodeToBytes(&expectedStateDiff)

	blockChan := make(chan *types.Block)
	parentBlockChain := make(chan *types.Block)
	serviceQuitChan := make(chan bool)
	mockBlockChain := &BlockChain{}
	mockBlockChain.SetReceiptsForHash(block1.Hash(), types.Receipts{mockReceipt})
	mockBlockChain.SetTd(block1.Hash(), block1.NumberU64(), mockTotalDifficulty)
	mockService := MockStateDiffService{
		Mutex:             sync.Mutex{},
		Builder:           statediff.NewBuilder(chain.StateCache()),
		BlockChan:         blockChan,
		BlockChain:        mockBlockChain,
		ParentBlockChan:   parentBlockChain,
		QuitChan:          serviceQuitChan,
		Subscriptions:     make(map[common.Hash]map[rpc.ID]statediff.Subscription),
		SubscriptionTypes: make(map[common.Hash]statediff.Params),
	}

	mockService.Start()
	id := rpc.NewID()
	payloadChan := make(chan statediff.Payload)
	quitChan := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sort.Slice(expectedStateDiffBytes, func(i, j int) bool { return expectedStateDiffBytes[i] < expectedStateDiffBytes[j] })
		select {
		case payload := <-payloadChan:
			if !bytes.Equal(payload.BlockRlp, expectedBlockRlp) {
				t.Errorf("payload does not have expected block\r\nactual block rlp: %v\r\nexpected block rlp: %v", payload.BlockRlp, expectedBlockRlp)
			}
			sort.Slice(payload.StateObjectRlp, func(i, j int) bool { return payload.StateObjectRlp[i] < payload.StateObjectRlp[j] })
			if !bytes.Equal(payload.StateObjectRlp, expectedStateDiffBytes) {
				t.Errorf("payload does not have expected state diff\r\nactual state diff rlp: %v\r\nexpected state diff rlp: %v", payload.StateObjectRlp, expectedStateDiffBytes)
			}
			if !bytes.Equal(expectedReceiptBytes, payload.ReceiptsRlp) {
				t.Errorf("payload does not have expected receipts\r\nactual receipt rlp: %v\r\nexpected receipt rlp: %v", payload.ReceiptsRlp, expectedReceiptBytes)
			}
			if !bytes.Equal(payload.TotalDifficulty.Bytes(), mockTotalDifficulty.Bytes()) {
				t.Errorf("payload does not have expected total difficulty\r\nactual td: %d\r\nexpected td: %d", payload.TotalDifficulty.Int64(), mockTotalDifficulty.Int64())
			}
		case <-quitChan:
			t.Errorf("channel quit before delivering payload")
		}
	}()
	time.Sleep(1 * time.Second)
	mockService.Subscribe(id, payloadChan, quitChan, parameters)
	blockChan <- block1
	parentBlockChain <- block0
	wg.Wait()
}

func testHTTPAPI(t *testing.T) {
	blocks, chain := test_helpers.MakeChain(1, test_helpers.Genesis, test_helpers.TestChainGen)
	defer chain.Stop()
	block0 = test_helpers.Genesis
	block1 = blocks[0]
	expectedBlockRlp, _ := rlp.EncodeToBytes(&block1)
	mockReceipt := &types.Receipt{
		BlockNumber: block1.Number(),
		BlockHash:   block1.Hash(),
	}
	expectedReceiptBytes, _ := rlp.EncodeToBytes(&types.Receipts{mockReceipt})
	expectedStateDiff := sdtypes.StateObject{
		BlockNumber: block1.Number(),
		BlockHash:   block1.Hash(),
		Nodes: []sdtypes.StateNode{
			{
				Path:         []byte{'\x05'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      minerLeafKey,
				NodeValue:    minerAccountLeafNode,
				StorageNodes: emptyStorage,
			},
			{
				Path:         []byte{'\x0e'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      test_helpers.Account1LeafKey,
				NodeValue:    account1LeafNode,
				StorageNodes: emptyStorage,
			},
			{
				Path:         []byte{'\x00'},
				NodeType:     sdtypes.Leaf,
				LeafKey:      test_helpers.BankLeafKey,
				NodeValue:    bankAccountLeafNode,
				StorageNodes: emptyStorage,
			},
		},
	}
	expectedStateDiffBytes, _ := rlp.EncodeToBytes(&expectedStateDiff)
	mockBlockChain := &BlockChain{}
	mockBlockChain.SetBlocksForHashes(map[common.Hash]*types.Block{
		block0.Hash(): block0,
		block1.Hash(): block1,
	})
	mockBlockChain.SetBlockForNumber(block1, block1.Number().Uint64())
	mockBlockChain.SetReceiptsForHash(block1.Hash(), types.Receipts{mockReceipt})
	mockBlockChain.SetTd(block1.Hash(), block1.NumberU64(), big.NewInt(1337))
	mockService := MockStateDiffService{
		Mutex:      sync.Mutex{},
		Builder:    statediff.NewBuilder(chain.StateCache()),
		BlockChain: mockBlockChain,
	}
	payload, err := mockService.StateDiffAt(block1.Number().Uint64(), parameters)
	if err != nil {
		t.Error(err)
	}
	sort.Slice(payload.StateObjectRlp, func(i, j int) bool { return payload.StateObjectRlp[i] < payload.StateObjectRlp[j] })
	sort.Slice(expectedStateDiffBytes, func(i, j int) bool { return expectedStateDiffBytes[i] < expectedStateDiffBytes[j] })
	if !bytes.Equal(payload.BlockRlp, expectedBlockRlp) {
		t.Errorf("payload does not have expected block\r\nactual block rlp: %v\r\nexpected block rlp: %v", payload.BlockRlp, expectedBlockRlp)
	}
	if !bytes.Equal(payload.StateObjectRlp, expectedStateDiffBytes) {
		t.Errorf("payload does not have expected state diff\r\nactual state diff rlp: %v\r\nexpected state diff rlp: %v", payload.StateObjectRlp, expectedStateDiffBytes)
	}
	if !bytes.Equal(expectedReceiptBytes, payload.ReceiptsRlp) {
		t.Errorf("payload does not have expected receipts\r\nactual receipt rlp: %v\r\nexpected receipt rlp: %v", payload.ReceiptsRlp, expectedReceiptBytes)
	}
	if !bytes.Equal(payload.TotalDifficulty.Bytes(), mockTotalDifficulty.Bytes()) {
		t.Errorf("paylaod does not have the expected total difficulty\r\nactual td: %d\r\nexpected td: %d", payload.TotalDifficulty.Int64(), mockTotalDifficulty.Int64())
	}
}

func testWatchAddressAPI(t *testing.T) {
	blocks, chain := test_helpers.MakeChain(6, test_helpers.Genesis, test_helpers.TestChainGen)
	defer chain.Stop()
	block6 := blocks[5]

	mockBlockChain := &BlockChain{}
	mockBlockChain.SetCurrentBlock(block6)
	mockIndexer := StateDiffIndexer{}
	mockService := MockStateDiffService{
		BlockChain: mockBlockChain,
		Indexer:    &mockIndexer,
	}

	// test data
	var (
		contract1Address   = "0x5d663F5269090bD2A7DC2390c911dF6083D7b28F"
		contract2Address   = "0x6Eb7e5C66DB8af2E96159AC440cbc8CDB7fbD26B"
		contract3Address   = "0xcfeB164C328CA13EFd3C77E1980d94975aDfedfc"
		contract4Address   = "0x0Edf0c4f393a628DE4828B228C48175b3EA297fc"
		contract1CreatedAt = uint64(1)
		contract2CreatedAt = uint64(2)
		contract3CreatedAt = uint64(3)
		contract4CreatedAt = uint64(4)

		args1 = []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		startingParams1 = statediff.Params{
			WatchedAddresses: []common.Address{},
		}
		expectedParams1 = statediff.Params{
			WatchedAddresses: []common.Address{
				common.HexToAddress(contract1Address),
				common.HexToAddress(contract2Address),
			},
		}

		args2 = []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		startingParams2 = expectedParams1
		expectedParams2 = statediff.Params{
			WatchedAddresses: []common.Address{
				common.HexToAddress(contract1Address),
				common.HexToAddress(contract2Address),
				common.HexToAddress(contract3Address),
			},
		}

		args3 = []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		startingParams3 = expectedParams2
		expectedParams3 = statediff.Params{
			WatchedAddresses: []common.Address{
				common.HexToAddress(contract1Address),
			},
		}

		args4 = []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		startingParams4 = expectedParams3
		expectedParams4 = statediff.Params{
			WatchedAddresses: []common.Address{},
		}

		args5 = []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
		}
		startingParams5 = expectedParams4
		expectedParams5 = statediff.Params{
			WatchedAddresses: []common.Address{
				common.HexToAddress(contract1Address),
				common.HexToAddress(contract2Address),
				common.HexToAddress(contract3Address),
			},
		}

		args6 = []sdtypes.WatchAddressArg{
			{
				Address:   contract4Address,
				CreatedAt: contract4CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
		}
		startingParams6 = expectedParams5
		expectedParams6 = statediff.Params{
			WatchedAddresses: []common.Address{
				common.HexToAddress(contract4Address),
				common.HexToAddress(contract2Address),
				common.HexToAddress(contract3Address),
			},
		}

		args7           = []sdtypes.WatchAddressArg{}
		startingParams7 = expectedParams6
		expectedParams7 = statediff.Params{
			WatchedAddresses: []common.Address{},
		}

		args8           = []sdtypes.WatchAddressArg{}
		startingParams8 = expectedParams6
		expectedParams8 = statediff.Params{
			WatchedAddresses: []common.Address{},
		}

		args9           = []sdtypes.WatchAddressArg{}
		startingParams9 = expectedParams8
		expectedParams9 = statediff.Params{
			WatchedAddresses: []common.Address{},
		}
	)

	tests := []struct {
		name           string
		operation      sdtypes.OperationType
		args           []sdtypes.WatchAddressArg
		startingParams statediff.Params
		expectedParams statediff.Params
		expectedErr    error
	}{
		{
			"testAddAddresses",
			sdtypes.Add,
			args1,
			startingParams1,
			expectedParams1,
			nil,
		},
		{
			"testAddAddressesSomeWatched",
			sdtypes.Add,
			args2,
			startingParams2,
			expectedParams2,
			nil,
		},
		{
			"testRemoveAddresses",
			sdtypes.Remove,
			args3,
			startingParams3,
			expectedParams3,
			nil,
		},
		{
			"testRemoveAddressesSomeWatched",
			sdtypes.Remove,
			args4,
			startingParams4,
			expectedParams4,
			nil,
		},
		{
			"testSetAddresses",
			sdtypes.Set,
			args5,
			startingParams5,
			expectedParams5,
			nil,
		},
		{
			"testSetAddressesSomeWatched",
			sdtypes.Set,
			args6,
			startingParams6,
			expectedParams6,
			nil,
		},
		{
			"testSetAddressesEmtpyArgs",
			sdtypes.Set,
			args7,
			startingParams7,
			expectedParams7,
			nil,
		},
		{
			"testClearAddresses",
			sdtypes.Clear,
			args8,
			startingParams8,
			expectedParams8,
			nil,
		},
		{
			"testClearAddressesEmpty",
			sdtypes.Clear,
			args9,
			startingParams9,
			expectedParams9,
			nil,
		},

		// invalid args
		{
			"testInvalidOperation",
			"WrongOp",
			args9,
			startingParams9,
			statediff.Params{},
			fmt.Errorf("%s WrongOp", unexpectedOperation),
		},
	}

	for _, test := range tests {
		// set indexing params
		mockService.writeLoopParams = statediff.ParamsWithMutex{
			Params: test.startingParams,
		}
		mockService.writeLoopParams.ComputeWatchedAddressesLeafKeys()

		// make the API call to change watched addresses
		err := mockService.WatchAddress(test.operation, test.args)
		if test.expectedErr != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Logf("Test failed: %s", test.name)
				t.Errorf("actual err: %+v\nexpected err: %+v", err, test.expectedErr)
			}

			continue
		}
		if err != nil {
			t.Error(err)
		}

		// check updated indexing params
		test.expectedParams.ComputeWatchedAddressesLeafKeys()
		updatedParams := mockService.writeLoopParams.Params
		if !reflect.DeepEqual(updatedParams, test.expectedParams) {
			t.Logf("Test failed: %s", test.name)
			t.Errorf("actual params: %+v\nexpected params: %+v", updatedParams, test.expectedParams)
		}
	}
}
