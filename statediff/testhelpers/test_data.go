// Copyright 2015 The go-ethereum Authors
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

package testhelpers

import (
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/statediff"
)

func AddressToLeafKey(address common.Address) common.Hash {
	return common.BytesToHash(crypto.Keccak256(address[:]))
}

var (
	BlockNumber     = rand.Int63()
	BlockHash       = "0xfa40fbe2d98d98b3363a778d52f2bcd29d6790b9b3f3cab2b167fd12d3550f73"
	CodeHash        = common.Hex2Bytes("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	NewNonceValue   = rand.Uint64()
	NewBalanceValue = rand.Int63()
	ContractRoot    = common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	StoragePath     = common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").Bytes()
	StorageKey      = common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001").Bytes()
	StorageValue    = common.Hex2Bytes("0x03")
	storage         = []statediff.StorageDiff{{
		Key:   StorageKey,
		Value: StorageValue,
		Path:  StoragePath,
		Proof: [][]byte{},
	}}
	emptyStorage           = make([]statediff.StorageDiff, 0)
	address                = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476592")
	ContractLeafKey        = AddressToLeafKey(address)
	anotherAddress         = common.HexToAddress("0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476593")
	AnotherContractLeafKey = AddressToLeafKey(anotherAddress)
	testAccount            = state.Account{
		Nonce:    NewNonceValue,
		Balance:  big.NewInt(NewBalanceValue),
		Root:     ContractRoot,
		CodeHash: CodeHash,
	}
	valueBytes, err     = rlp.EncodeToBytes(testAccount)
	CreatedAccountDiffs = statediff.AccountDiffsMap{
		ContractLeafKey: {
			Key:     ContractLeafKey.Bytes(),
			Value:   valueBytes,
			Storage: storage,
		},
		AnotherContractLeafKey: {
			Key:     AnotherContractLeafKey.Bytes(),
			Value:   valueBytes,
			Storage: emptyStorage,
		},
	}

	UpdatedAccountDiffs = statediff.AccountDiffsMap{ContractLeafKey: {
		Key:     ContractLeafKey.Bytes(),
		Value:   valueBytes,
		Storage: storage,
	}}

	DeletedAccountDiffs = statediff.AccountDiffsMap{ContractLeafKey: {
		Key:     ContractLeafKey.Bytes(),
		Value:   valueBytes,
		Storage: storage,
	}}

	TestStateDiff = statediff.StateDiff{
		BlockNumber:     BlockNumber,
		BlockHash:       common.HexToHash(BlockHash),
		CreatedAccounts: CreatedAccountDiffs,
		DeletedAccounts: DeletedAccountDiffs,
		UpdatedAccounts: UpdatedAccountDiffs,
	}
)
