// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package mocks

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/test_helpers"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

// Test variables
var (
	// block data
	// TODO: Update this to `MainnetChainConfig` when `LondonBlock` is added
	TestConfig  = params.RopstenChainConfig
	BlockNumber = TestConfig.LondonBlock
	MockHeader  = types.Header{
		Time:        0,
		Number:      new(big.Int).Set(BlockNumber),
		Root:        common.HexToHash("0x0"),
		TxHash:      common.HexToHash("0x0"),
		ReceiptHash: common.HexToHash("0x0"),
		Difficulty:  big.NewInt(5000000),
		Extra:       []byte{},
		BaseFee:     big.NewInt(params.InitialBaseFee),
		Coinbase:    common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476777"),
	}
	MockTransactions, MockReceipts, SenderAddr = createTransactionsAndReceipts(TestConfig, BlockNumber)
	// Create uncles and add them here.
	MockUncles                  = createUncles(BlockNumber, 3)
	MockBlock                   = types.NewBlock(&MockHeader, MockTransactions, MockUncles, MockReceipts, new(trie.Trie))
	MockHeaderRlp, _            = rlp.EncodeToBytes(MockBlock.Header())
	Address                     = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476592")
	AnotherAddress              = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476593")
	ContractAddress             = crypto.CreateAddress(SenderAddr, MockTransactions[2].Nonce())
	ContractAddress2            = crypto.CreateAddress(SenderAddr, MockTransactions[3].Nonce())
	MockContractByteCode        = []byte{0, 1, 2, 3, 4, 5}
	mockTopic11                 = common.HexToHash("0x04")
	mockTopic12                 = common.HexToHash("0x06")
	mockTopic21                 = common.HexToHash("0x05")
	mockTopic22                 = common.HexToHash("0x07")
	ExpectedPostStatus   uint64 = 1
	ExpectedPostState1          = common.Bytes2Hex(common.HexToHash("0x1").Bytes())
	ExpectedPostState2          = common.Bytes2Hex(common.HexToHash("0x2").Bytes())
	ExpectedPostState3          = common.Bytes2Hex(common.HexToHash("0x3").Bytes())
	MockLog1                    = &types.Log{
		Address: Address,
		Topics:  []common.Hash{mockTopic11, mockTopic12},
		Data:    []byte{},
	}
	MockLog2 = &types.Log{
		Address: AnotherAddress,
		Topics:  []common.Hash{mockTopic21, mockTopic22},
		Data:    []byte{},
	}
	MockLog3 = &types.Log{
		Address: Address,
		Topics:  []common.Hash{mockTopic11, mockTopic22},
		Data:    []byte{},
	}
	MockLog4 = &types.Log{
		Address: AnotherAddress,
		Topics:  []common.Hash{mockTopic21, mockTopic12},
		Data:    []byte{},
	}
	ShortLog1 = &types.Log{
		Address: AnotherAddress,
		Topics:  []common.Hash{},
		Data:    []byte{},
	}
	ShortLog2 = &types.Log{
		Address: Address,
		Topics:  []common.Hash{},
		Data:    []byte{},
	}

	// access list entries
	AccessListEntry1 = types.AccessTuple{
		Address: Address,
	}
	AccessListEntry2 = types.AccessTuple{
		Address:     AnotherAddress,
		StorageKeys: []common.Hash{common.BytesToHash(StorageLeafKey), common.BytesToHash(MockStorageLeafKey)},
	}
	AccessListEntry1Model = models.AccessListElementModel{
		BlockNumber: BlockNumber.String(),
		Index:       0,
		Address:     Address.Hex(),
	}
	AccessListEntry2Model = models.AccessListElementModel{
		BlockNumber: BlockNumber.String(),
		Index:       1,
		Address:     AnotherAddress.Hex(),
		StorageKeys: []string{common.BytesToHash(StorageLeafKey).Hex(), common.BytesToHash(MockStorageLeafKey).Hex()},
	}

	// statediff data
	storageLocation     = common.HexToHash("0")
	StorageLeafKey      = crypto.Keccak256Hash(storageLocation[:]).Bytes()
	mockStorageLocation = common.HexToHash("1")
	MockStorageLeafKey  = crypto.Keccak256Hash(mockStorageLocation[:]).Bytes()
	StorageValue        = common.Hex2Bytes("01")
	StoragePartialPath  = common.Hex2Bytes("20290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")
	StorageLeafNode, _  = rlp.EncodeToBytes(&[]interface{}{
		StoragePartialPath,
		StorageValue,
	})

	nonce1             = uint64(1)
	ContractRoot       = "0x821e2556a290c86405f8160a2d662042a431ba456b9db265c79bb837c04be5f0"
	ContractCodeHash   = common.HexToHash("0x753f98a8d4328b15636e46f66f2cb4bc860100aa17967cc145fcd17d1d4710ea")
	ContractLeafKey    = test_helpers.AddressToLeafKey(ContractAddress)
	ContractAccount, _ = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    nonce1,
		Balance:  big.NewInt(0),
		CodeHash: ContractCodeHash.Bytes(),
		Root:     common.HexToHash(ContractRoot),
	})
	ContractPartialPath = common.Hex2Bytes("3114658a74d9cc9f7acf2c5cd696c3494d7c344d78bfec3add0d91ec4e8d1c45")
	ContractLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		ContractPartialPath,
		ContractAccount,
	})

	Contract2LeafKey = test_helpers.AddressToLeafKey(ContractAddress2)
	storage2Location = common.HexToHash("2")
	Storage2LeafKey  = crypto.Keccak256Hash(storage2Location[:]).Bytes()
	storage3Location = common.HexToHash("3")
	Storage3LeafKey  = crypto.Keccak256Hash(storage3Location[:]).Bytes()

	nonce0          = uint64(0)
	AccountRoot     = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
	AccountCodeHash = common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	AccountLeafKey  = test_helpers.Account2LeafKey
	RemovedLeafKey  = test_helpers.Account1LeafKey
	Account, _      = rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    nonce0,
		Balance:  big.NewInt(1000),
		CodeHash: AccountCodeHash.Bytes(),
		Root:     common.HexToHash(AccountRoot),
	})
	AccountPartialPath = common.Hex2Bytes("3957f3e2f04a0764c3a0491b175f69926da61efbcc8f61fa1455fd2d2b4cdd45")
	AccountLeafNode, _ = rlp.EncodeToBytes(&[]interface{}{
		AccountPartialPath,
		Account,
	})

	StateDiffs = []sdtypes.StateNode{
		{
			Path:      []byte{'\x06'},
			NodeType:  sdtypes.Leaf,
			LeafKey:   ContractLeafKey,
			NodeValue: ContractLeafNode,
			StorageNodes: []sdtypes.StorageNode{
				{
					Path:      []byte{},
					NodeType:  sdtypes.Leaf,
					LeafKey:   StorageLeafKey,
					NodeValue: StorageLeafNode,
				},
				{
					Path:      []byte{'\x03'},
					NodeType:  sdtypes.Removed,
					LeafKey:   RemovedLeafKey,
					NodeValue: []byte{},
				},
			},
		},
		{
			Path:         []byte{'\x0c'},
			NodeType:     sdtypes.Leaf,
			LeafKey:      AccountLeafKey,
			NodeValue:    AccountLeafNode,
			StorageNodes: []sdtypes.StorageNode{},
		},
		{
			Path:      []byte{'\x02'},
			NodeType:  sdtypes.Removed,
			LeafKey:   RemovedLeafKey,
			NodeValue: []byte{},
		},
		{
			Path:      []byte{'\x07'},
			NodeType:  sdtypes.Removed,
			LeafKey:   Contract2LeafKey,
			NodeValue: []byte{},
			StorageNodes: []sdtypes.StorageNode{
				{
					Path:      []byte{'\x0e'},
					NodeType:  sdtypes.Removed,
					LeafKey:   Storage2LeafKey,
					NodeValue: []byte{},
				},
				{
					Path:      []byte{'\x0f'},
					NodeType:  sdtypes.Removed,
					LeafKey:   Storage3LeafKey,
					NodeValue: []byte{},
				},
			},
		},
	}
)

type LegacyData struct {
	Config               *params.ChainConfig
	BlockNumber          *big.Int
	MockHeader           types.Header
	MockTransactions     types.Transactions
	MockReceipts         types.Receipts
	SenderAddr           common.Address
	MockBlock            *types.Block
	MockHeaderRlp        []byte
	Address              []byte
	AnotherAddress       []byte
	ContractAddress      common.Address
	MockContractByteCode []byte
	MockLog1             *types.Log
	MockLog2             *types.Log
	StorageLeafKey       []byte
	MockStorageLeafKey   []byte
	StorageLeafNode      []byte
	ContractLeafKey      []byte
	ContractAccount      []byte
	ContractPartialPath  []byte
	ContractLeafNode     []byte
	AccountRoot          string
	AccountLeafNode      []byte
	StateDiffs           []sdtypes.StateNode
}

func NewLegacyData() *LegacyData {
	config := params.MainnetChainConfig
	// Block number before london fork.
	blockNumber := config.EIP155Block

	mockHeader := types.Header{
		Time:        0,
		Number:      new(big.Int).Set(blockNumber),
		Root:        common.HexToHash("0x0"),
		TxHash:      common.HexToHash("0x0"),
		ReceiptHash: common.HexToHash("0x0"),
		Difficulty:  big.NewInt(5000000),
		Extra:       []byte{},
		Coinbase:    common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476888"),
	}

	mockTransactions, mockReceipts, senderAddr := createLegacyTransactionsAndReceipts(config, blockNumber)
	mockBlock := types.NewBlock(&mockHeader, mockTransactions, nil, mockReceipts, new(trie.Trie))
	mockHeaderRlp, _ := rlp.EncodeToBytes(mockBlock.Header())
	contractAddress := crypto.CreateAddress(senderAddr, mockTransactions[2].Nonce())

	return &LegacyData{
		Config:               config,
		BlockNumber:          blockNumber,
		MockHeader:           mockHeader,
		MockTransactions:     mockTransactions,
		MockReceipts:         mockReceipts,
		SenderAddr:           senderAddr,
		MockBlock:            mockBlock,
		MockHeaderRlp:        mockHeaderRlp,
		ContractAddress:      contractAddress,
		MockContractByteCode: MockContractByteCode,
		MockLog1:             MockLog1,
		MockLog2:             MockLog2,
		StorageLeafKey:       StorageLeafKey,
		MockStorageLeafKey:   MockStorageLeafKey,
		StorageLeafNode:      StorageLeafNode,
		ContractLeafKey:      ContractLeafKey,
		ContractAccount:      ContractAccount,
		ContractPartialPath:  ContractPartialPath,
		ContractLeafNode:     ContractLeafNode,
		AccountRoot:          AccountRoot,
		AccountLeafNode:      AccountLeafKey,
		StateDiffs:           StateDiffs,
	}
}

// createLegacyTransactionsAndReceipts is a helper function to generate signed mock legacy transactions and mock receipts with mock logs
func createLegacyTransactionsAndReceipts(config *params.ChainConfig, blockNumber *big.Int) (types.Transactions, types.Receipts, common.Address) {
	// make transactions
	trx1 := types.NewTransaction(0, Address, big.NewInt(1000), 50, big.NewInt(100), []byte{})
	trx2 := types.NewTransaction(1, AnotherAddress, big.NewInt(2000), 100, big.NewInt(200), []byte{})
	trx3 := types.NewContractCreation(2, big.NewInt(1500), 75, big.NewInt(150), MockContractByteCode)

	transactionSigner := types.MakeSigner(config, blockNumber)
	mockCurve := elliptic.P256()
	mockPrvKey, err := ecdsa.GenerateKey(mockCurve, rand.Reader)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx1, err := types.SignTx(trx1, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx2, err := types.SignTx(trx2, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx3, err := types.SignTx(trx3, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}

	senderAddr, err := types.Sender(transactionSigner, signedTrx1) // same for both trx
	if err != nil {
		log.Crit(err.Error())
	}

	// make receipts
	mockReceipt1 := types.NewReceipt(nil, false, 50)
	mockReceipt1.Logs = []*types.Log{MockLog1}
	mockReceipt1.TxHash = signedTrx1.Hash()
	mockReceipt2 := types.NewReceipt(common.HexToHash("0x1").Bytes(), false, 100)
	mockReceipt2.Logs = []*types.Log{MockLog2, ShortLog1}
	mockReceipt2.TxHash = signedTrx2.Hash()
	mockReceipt3 := types.NewReceipt(common.HexToHash("0x2").Bytes(), false, 75)
	mockReceipt3.Logs = []*types.Log{}
	mockReceipt3.TxHash = signedTrx3.Hash()

	return types.Transactions{signedTrx1, signedTrx2, signedTrx3}, types.Receipts{mockReceipt1, mockReceipt2, mockReceipt3}, senderAddr
}

// createTransactionsAndReceipts is a helper function to generate signed mock transactions and mock receipts with mock logs
func createTransactionsAndReceipts(config *params.ChainConfig, blockNumber *big.Int) (types.Transactions, types.Receipts, common.Address) {
	// make transactions
	trx1 := types.NewTransaction(0, Address, big.NewInt(1000), 50, big.NewInt(100), []byte{})
	trx2 := types.NewTransaction(1, AnotherAddress, big.NewInt(2000), 100, big.NewInt(200), []byte{})
	trx3 := types.NewContractCreation(2, big.NewInt(1500), 75, big.NewInt(150), MockContractByteCode)
	trx4 := types.NewTx(&types.AccessListTx{
		ChainID:  config.ChainID,
		Nonce:    0,
		GasPrice: big.NewInt(100),
		Gas:      50,
		To:       &AnotherAddress,
		Value:    big.NewInt(999),
		Data:     []byte{},
		AccessList: types.AccessList{
			AccessListEntry1,
			AccessListEntry2,
		},
	})
	trx5 := types.NewTx(&types.DynamicFeeTx{
		ChainID:   config.ChainID,
		Nonce:     0,
		GasTipCap: big.NewInt(100),
		GasFeeCap: big.NewInt(100),
		Gas:       50,
		To:        &AnotherAddress,
		Value:     big.NewInt(1000),
		Data:      []byte{},
		AccessList: types.AccessList{
			AccessListEntry1,
			AccessListEntry2,
		},
	})

	transactionSigner := types.MakeSigner(config, blockNumber)
	mockCurve := elliptic.P256()
	mockPrvKey, err := ecdsa.GenerateKey(mockCurve, rand.Reader)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx1, err := types.SignTx(trx1, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx2, err := types.SignTx(trx2, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx3, err := types.SignTx(trx3, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx4, err := types.SignTx(trx4, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}
	signedTrx5, err := types.SignTx(trx5, transactionSigner, mockPrvKey)
	if err != nil {
		log.Crit(err.Error())
	}

	senderAddr, err := types.Sender(transactionSigner, signedTrx1) // same for both trx
	if err != nil {
		log.Crit(err.Error())
	}

	// make receipts
	mockReceipt1 := types.NewReceipt(nil, false, 50)
	mockReceipt1.Logs = []*types.Log{MockLog1}
	mockReceipt1.TxHash = signedTrx1.Hash()
	mockReceipt2 := types.NewReceipt(common.HexToHash("0x1").Bytes(), false, 100)
	mockReceipt2.Logs = []*types.Log{MockLog2, ShortLog1}
	mockReceipt2.TxHash = signedTrx2.Hash()
	mockReceipt3 := types.NewReceipt(common.HexToHash("0x2").Bytes(), false, 75)
	mockReceipt3.Logs = []*types.Log{}
	mockReceipt3.TxHash = signedTrx3.Hash()
	mockReceipt4 := &types.Receipt{
		Type:              types.AccessListTxType,
		PostState:         common.HexToHash("0x3").Bytes(),
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 175,
		Logs:              []*types.Log{MockLog3, MockLog4, ShortLog2},
		TxHash:            signedTrx4.Hash(),
	}
	mockReceipt5 := &types.Receipt{
		Type:              types.DynamicFeeTxType,
		PostState:         common.HexToHash("0x3").Bytes(),
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 175,
		Logs:              []*types.Log{},
		TxHash:            signedTrx5.Hash(),
	}

	return types.Transactions{signedTrx1, signedTrx2, signedTrx3, signedTrx4, signedTrx5}, types.Receipts{mockReceipt1, mockReceipt2, mockReceipt3, mockReceipt4, mockReceipt5}, senderAddr
}

// This functions will create uncles
func createUncles(blockNumber *big.Int, totalUncles int) []*types.Header {
	var uncles = make([]*types.Header, totalUncles+1)
	for i := 0; i <= totalUncles; i++ {
		uncles[i] = &types.Header{
			Difficulty: math.BigPow(11, 11),
			Number:     blockNumber,
			Time:       0,
			Extra:      []byte("test uncle"),
		}
	}
	return uncles
}
