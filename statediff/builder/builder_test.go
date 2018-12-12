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

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package builder_test

import (
	"math/big"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/crypto"
	statediff "github.com/ethereum/go-ethereum/statediff/builder"
)


var (
	testdb = ethdb.NewMemDatabase()

	testBankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testBankAddress = crypto.PubkeyToAddress(testBankKey.PublicKey) //0x71562b71999873DB5b286dF957af199Ec94617F7
	testBankFunds   = big.NewInt(100000000)
	genesis      = core.GenesisBlockForTesting(testdb, testBankAddress, testBankFunds)

	account1Key, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	account2Key, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	account1Addr   = crypto.PubkeyToAddress(account1Key.PublicKey) //0x703c4b2bD70c169f5717101CaeE543299Fc946C7
	account2Addr   = crypto.PubkeyToAddress(account2Key.PublicKey) //0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e

	contractCode = common.Hex2Bytes("606060405260cc8060106000396000f360606040526000357c01000000000000000000000000000000000000000000000000000000009004806360cd2685146041578063c16431b914606b57603f565b005b6055600480803590602001909190505060a9565b6040518082815260200191505060405180910390f35b60886004808035906020019091908035906020019091905050608a565b005b80600060005083606481101560025790900160005b50819055505b5050565b6000600060005082606481101560025790900160005b5054905060c7565b91905056")
	contractAddr common.Address

	emptyAccountDiffEventualMap = make(map[common.Address]statediff.AccountDiffEventual)
	emptyAccountDiffIncrementalMap = make(map[common.Address]statediff.AccountDiffIncremental)
)
/*
contract test {

    uint256[100] data;

    function Put(uint256 addr, uint256 value) {
        data[addr] = value;
    }

    function Get(uint256 addr) constant returns (uint256 value) {
        return data[addr];
    }
}
*/


// makeChain creates a chain of n blocks starting at and including parent.
// the returned hash chain is ordered head->parent. In addition, every 3rd block
// contains a transaction and every 5th an uncle to allow testing correct block
// reassembly.
func makeChain(n int, seed byte, parent *types.Block) ([]common.Hash, map[common.Hash]*types.Block) {
	blocks, _ := core.GenerateChain(params.TestChainConfig, parent, ethash.NewFaker(), testdb, n, testChainGen)
	hashes := make([]common.Hash, n+1)
	hashes[len(hashes)-1] = parent.Hash()
	blockm := make(map[common.Hash]*types.Block, n+1)
	blockm[parent.Hash()] = parent
	for i, b := range blocks {
		hashes[len(hashes)-i-2] = b.Hash()
		blockm[b.Hash()] = b
	}
	return hashes, blockm
}


func testChainGen(i int, block *core.BlockGen) {
	signer := types.HomesteadSigner{}
	switch i {
	case 0:
		// In block 1, the test bank sends account #1 some ether.
		tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBankAddress), account1Addr, big.NewInt(10000), params.TxGas, nil, nil), signer, testBankKey)
		block.AddTx(tx)
	case 1:
		// In block 2, the test bank sends some more ether to account #1.
		// account1Addr passes it on to account #2.
		// account1Addr creates a test contract.
		tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBankAddress), account1Addr, big.NewInt(1000), params.TxGas, nil, nil), signer, testBankKey)
		nonce := block.TxNonce(account1Addr)
		tx2, _ := types.SignTx(types.NewTransaction(nonce, account2Addr, big.NewInt(1000), params.TxGas, nil, nil), signer, account1Key)
		nonce++
		tx3, _ := types.SignTx(types.NewContractCreation(nonce, big.NewInt(0), 1000000, big.NewInt(0), contractCode), signer, account1Key)
		contractAddr = crypto.CreateAddress(account1Addr, nonce) //0xaE9BEa628c4Ce503DcFD7E305CaB4e29E7476592
		block.AddTx(tx1)
		block.AddTx(tx2)
		block.AddTx(tx3)
	case 2:
		// Block 3 is empty but was mined by account #2.
		block.SetCoinbase(account2Addr)
		//get function: 60cd2685
		//put function: c16431b9
		data := common.Hex2Bytes("C16431B900000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000003")
		tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBankAddress), contractAddr, big.NewInt(0), 100000, nil, data), signer, testBankKey)
		block.AddTx(tx)
	}
}
var _ = ginkgo.FDescribe("", func() {
	var (
		block0Hash, block1Hash, block2Hash, block3Hash common.Hash
		block0, block1, block2, block3 *types.Block
		builder statediff.Builder
		miningReward = int64(3000000000000000000)
		burnAddress = common.HexToAddress("0x0")
		diff *statediff.StateDiff
		err error
	)

	ginkgo.BeforeEach(func() {
		_, blocks := makeChain(3, 0, genesis)
		block0Hash = common.HexToHash("0xd1721cfd0b29c36fd7a68f25c128e86413fb666a6e1d68e89b875bd299262661")
		block1Hash = common.HexToHash("0x47c398dd688eaa4dd11b006888156783fe32df83d59b197c0fcd303408103d39")
		block2Hash = common.HexToHash("0x351b2f531838683ba457e8ca4d3a844cc48147dceafbcb589dc6e3227856ee75")
		block3Hash = common.HexToHash("0xfa40fbe2d98d98b3363a778d52f2bcd29d6790b9b3f3cab2b167fd12d3550f73")

		block0 = blocks[block0Hash]
		block1 = blocks[block1Hash]
		block2 = blocks[block2Hash]
		block3 = blocks[block3Hash]
		builder = statediff.NewBuilder(testdb)
	})

	ginkgo.It("returns empty account diff collections when the state root hasn't changed", func() {
		expectedDiff := statediff.StateDiff{
			BlockNumber:     block0.Number().Int64(),
			BlockHash:       block0Hash,
			CreatedAccounts: emptyAccountDiffEventualMap,
			DeletedAccounts: emptyAccountDiffEventualMap,
			UpdatedAccounts: emptyAccountDiffIncrementalMap,
		}

		diff, err := builder.BuildStateDiff(block0.Root(), block0.Root(), block0.Number().Int64(), block0Hash)

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(diff).To(gomega.Equal(&expectedDiff))
	})

	ginkgo.Context("Block 1", func() {
		//10000 transferred from testBankAddress to account1Addr
		var balanceChange = int64(10000)

		ginkgo.BeforeEach(func() {
			diff, err = builder.BuildStateDiff(block0.Root(), block1.Root(), block1.Number().Int64(), block1Hash)

			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("includes the block number and hash", func() {
			gomega.Expect(diff.BlockNumber).To(gomega.Equal(block1.Number().Int64()))
			gomega.Expect(diff.BlockHash).To(gomega.Equal(block1Hash))
		})

		ginkgo.It("returns an empty collection for deleted accounts", func() {
			gomega.Expect(diff.DeletedAccounts).To(gomega.Equal(emptyAccountDiffEventualMap))
		})

		ginkgo.It("returns balance diffs for updated accounts", func() {
			expectedBankBalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(testBankFunds.Int64() - balanceChange),
				OldValue: testBankFunds,
			}

			gomega.Expect(len(diff.UpdatedAccounts)).To(gomega.Equal(1))
			gomega.Expect(diff.UpdatedAccounts[testBankAddress].Balance).To(gomega.Equal(expectedBankBalanceDiff))
		})

		ginkgo.It("returns balance diffs for new accounts", func() {
			expectedAccount1BalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(balanceChange),
				OldValue: nil,
			}

			expectedBurnAddrBalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(miningReward),
				OldValue: nil,
			}

			gomega.Expect(len(diff.CreatedAccounts)).To(gomega.Equal(2))
			gomega.Expect(diff.CreatedAccounts[account1Addr].Balance).To(gomega.Equal(expectedAccount1BalanceDiff))
			gomega.Expect(diff.CreatedAccounts[burnAddress].Balance).To(gomega.Equal(expectedBurnAddrBalanceDiff))
		})
	})

	ginkgo.Context("Block 2", func() {
		//1000 transferred from testBankAddress to account1Addr
		//1000 transferred from account1Addr to account2Addr
		var (
			balanceChange = int64(1000)
			block1BankBalance = int64(99990000)
			block1Account1Balance = int64(10000)
		)

		ginkgo.BeforeEach(func() {
			diff, err = builder.BuildStateDiff(block1.Root(), block2.Root(), block2.Number().Int64(), block2Hash)

			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("includes the block number and hash", func() {
			gomega.Expect(diff.BlockNumber).To(gomega.Equal(block2.Number().Int64()))
			gomega.Expect(diff.BlockHash).To(gomega.Equal(block2Hash))
		})

		ginkgo.It("returns an empty collection for deleted accounts", func() {
			gomega.Expect(diff.DeletedAccounts).To(gomega.Equal(emptyAccountDiffEventualMap))
		})

		ginkgo.It("returns balance diffs for updated accounts", func() {
			expectedBankBalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(block1BankBalance - balanceChange),
				OldValue: big.NewInt(block1BankBalance),
			}

			expectedAccount1BalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(block1Account1Balance - balanceChange + balanceChange),
				OldValue: big.NewInt(block1Account1Balance),
			}

			expectedBurnBalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(miningReward + miningReward),
				OldValue: big.NewInt(miningReward),
			}

			gomega.Expect(len(diff.UpdatedAccounts)).To(gomega.Equal(3))
			gomega.Expect(diff.UpdatedAccounts[testBankAddress].Balance).To(gomega.Equal(expectedBankBalanceDiff))
			gomega.Expect(diff.UpdatedAccounts[account1Addr].Balance).To(gomega.Equal(expectedAccount1BalanceDiff))
			gomega.Expect(diff.UpdatedAccounts[burnAddress].Balance).To(gomega.Equal(expectedBurnBalanceDiff))
		})

		ginkgo.It("returns balance diffs for new accounts", func() {
			expectedAccount2BalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(balanceChange),
				OldValue: nil,
			}

			expectedContractBalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(0),
				OldValue: nil,
			}

			gomega.Expect(len(diff.CreatedAccounts)).To(gomega.Equal(2))
			gomega.Expect(diff.CreatedAccounts[account2Addr].Balance).To(gomega.Equal(expectedAccount2BalanceDiff))
			gomega.Expect(diff.CreatedAccounts[contractAddr].Balance).To(gomega.Equal(expectedContractBalanceDiff))
		})
	})

	ginkgo.Context("Block 3", func() {
		//the contract's storage is changed
		//and the block is mined by account 2
		ginkgo.BeforeEach(func() {
			diff, err = builder.BuildStateDiff(block2.Root(), block3.Root(), block3.Number().Int64(), block3Hash)

			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("includes the block number and hash", func() {
			gomega.Expect(diff.BlockNumber).To(gomega.Equal(block3.Number().Int64()))
			gomega.Expect(diff.BlockHash).To(gomega.Equal(block3Hash))
		})

		ginkgo.It("returns an empty collection for deleted accounts", func() {
			gomega.Expect(diff.DeletedAccounts).To(gomega.Equal(emptyAccountDiffEventualMap))
		})

		ginkgo.It("returns an empty collection for created accounts", func() {
			gomega.Expect(diff.CreatedAccounts).To(gomega.Equal(emptyAccountDiffEventualMap))
		})

		ginkgo.It("returns balance, storage and nonce diffs for updated accounts", func() {
			block2Account2Balance := int64(1000)
			expectedAcct2BalanceDiff := statediff.DiffBigInt{
				NewValue: big.NewInt(block2Account2Balance + miningReward),
				OldValue: big.NewInt(block2Account2Balance),
			}

			expectedContractStorageDiff := make(map[string]statediff.DiffString)
			newVal := "0x03"
			oldVal := "0x0"
			path := "0x405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace"
			expectedContractStorageDiff[path] = statediff.DiffString{
				NewValue: &newVal,
				OldValue: &oldVal,
			}

			oldNonce := uint64(2)
			newNonce := uint64(3)
			expectedBankNonceDiff := statediff.DiffUint64{
				NewValue: &newNonce,
				OldValue: &oldNonce,
			}

			gomega.Expect(len(diff.UpdatedAccounts)).To(gomega.Equal(3))
			gomega.Expect(diff.UpdatedAccounts[account2Addr].Balance).To(gomega.Equal(expectedAcct2BalanceDiff))
			gomega.Expect(diff.UpdatedAccounts[contractAddr].Storage).To(gomega.Equal(expectedContractStorageDiff))
			gomega.Expect(diff.UpdatedAccounts[testBankAddress].Nonce).To(gomega.Equal(expectedBankNonceDiff))
		})
	})
})