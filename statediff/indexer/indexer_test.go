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

package indexer_test

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var (
	db        *postgres.DB
	err       error
	ind       *indexer.StateDiffIndexer
	ipfsPgGet = `SELECT data FROM public.blocks
					WHERE key = $1`
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5                          []byte
	mockBlock                                                                      *types.Block
	headerCID, trx1CID, trx2CID, trx3CID, trx4CID, trx5CID                         cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                                    cid.Cid
	rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5                               []byte
	state1CID, state2CID, storageCID                                               cid.Cid
	contract1Address, contract2Address, contract3Address, contract4Address         string
	contract1CreatedAt, contract2CreatedAt, contract3CreatedAt, contract4CreatedAt uint64
	slot1StorageKeyHex, slot2StorageKeyHex, slot3StorageKeyHex, slot4StorageKeyHex string
	slot1CreatedAt, slot2CreatedAt, slot3CreatedAt, slot4CreatedAt                 uint64
	lastFilledAt, watchedAt1, watchedAt2, watchedAt3                               uint64
)

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}

	mockBlock = mocks.MockBlock
	txs, rcts := mocks.MockBlock.Transactions(), mocks.MockReceipts

	buf := new(bytes.Buffer)
	txs.EncodeIndex(0, buf)
	tx1 = make([]byte, buf.Len())
	copy(tx1, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(1, buf)
	tx2 = make([]byte, buf.Len())
	copy(tx2, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(2, buf)
	tx3 = make([]byte, buf.Len())
	copy(tx3, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(3, buf)
	tx4 = make([]byte, buf.Len())
	copy(tx4, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(4, buf)
	tx5 = make([]byte, buf.Len())
	copy(tx5, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(0, buf)
	rct1 = make([]byte, buf.Len())
	copy(rct1, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(1, buf)
	rct2 = make([]byte, buf.Len())
	copy(rct2, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(2, buf)
	rct3 = make([]byte, buf.Len())
	copy(rct3, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(3, buf)
	rct4 = make([]byte, buf.Len())
	copy(rct4, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(4, buf)
	rct5 = make([]byte, buf.Len())
	copy(rct5, buf.Bytes())
	buf.Reset()

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
	trx1CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx1, multihash.KECCAK_256)
	trx2CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx2, multihash.KECCAK_256)
	trx3CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx3, multihash.KECCAK_256)
	trx4CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx4, multihash.KECCAK_256)
	trx5CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)

	receiptTrie := ipld.NewRctTrie()

	receiptTrie.Add(0, rct1)
	receiptTrie.Add(1, rct2)
	receiptTrie.Add(2, rct3)
	receiptTrie.Add(3, rct4)
	receiptTrie.Add(4, rct5)

	rctLeafNodes, keys, _ := receiptTrie.GetLeafNodes()

	rctleafNodeCids := make([]cid.Cid, len(rctLeafNodes))
	orderedRctLeafNodes := make([][]byte, len(rctLeafNodes))
	for i, rln := range rctLeafNodes {
		var idx uint

		r := bytes.NewReader(keys[i].TrieKey)
		rlp.Decode(r, &idx)
		rctleafNodeCids[idx] = rln.Cid()
		orderedRctLeafNodes[idx] = rln.RawData()
	}

	rct1CID = rctleafNodeCids[0]
	rct2CID = rctleafNodeCids[1]
	rct3CID = rctleafNodeCids[2]
	rct4CID = rctleafNodeCids[3]
	rct5CID = rctleafNodeCids[4]

	rctLeaf1 = orderedRctLeafNodes[0]
	rctLeaf2 = orderedRctLeafNodes[1]
	rctLeaf3 = orderedRctLeafNodes[2]
	rctLeaf4 = orderedRctLeafNodes[3]
	rctLeaf5 = orderedRctLeafNodes[4]

	contract1Address = "0x5d663F5269090bD2A7DC2390c911dF6083D7b28F"
	contract2Address = "0x6Eb7e5C66DB8af2E96159AC440cbc8CDB7fbD26B"
	contract3Address = "0xcfeB164C328CA13EFd3C77E1980d94975aDfedfc"
	contract4Address = "0x0Edf0c4f393a628DE4828B228C48175b3EA297fc"
	contract1CreatedAt = uint64(1)
	contract2CreatedAt = uint64(2)
	contract3CreatedAt = uint64(3)
	contract4CreatedAt = uint64(4)

	slot1StorageKeyHex = crypto.Keccak256Hash(common.HexToHash("1").Bytes()).Hex()
	slot2StorageKeyHex = crypto.Keccak256Hash(common.HexToHash("2").Bytes()).Hex()
	slot3StorageKeyHex = crypto.Keccak256Hash(common.HexToHash("3").Bytes()).Hex()
	slot4StorageKeyHex = crypto.Keccak256Hash(common.HexToHash("4").Bytes()).Hex()
	slot1CreatedAt = uint64(1)
	slot2CreatedAt = uint64(2)
	slot3CreatedAt = uint64(3)
	slot4CreatedAt = uint64(4)

	lastFilledAt = uint64(0)
	watchedAt1 = uint64(10)
	watchedAt2 = uint64(15)
	watchedAt3 = uint64(20)
}

func setupIndexer(t *testing.T) {
	db, err = shared.SetupDB()
	if err != nil {
		t.Fatal(err)
	}
	ind, err = indexer.NewStateDiffIndexer(mocks.TestConfig, db)
	require.NoError(t, err)
}

func setup(t *testing.T) {
	setupIndexer(t)
	var tx *indexer.BlockTx
	tx, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close(err)
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node)
		if err != nil {
			t.Fatal(err)
		}
	}

	shared.ExpectEqual(t, tx.BlockNumber, mocks.BlockNumber.Uint64())
}

func tearDown(t *testing.T) {
	indexer.TearDownDB(t, db)
}

func TestPublishAndIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		pgStr := `SELECT cid, td, reward, id, base_fee
				FROM eth.header_cids
				WHERE block_number = $1`
		// check header was properly indexed
		type res struct {
			CID     string
			TD      string
			Reward  string
			ID      int
			BaseFee *int64 `db:"base_fee"`
		}
		header := new(res)
		err = db.QueryRowx(pgStr, mocks.BlockNumber.Uint64()).StructScan(header)
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, header.CID, headerCID.String())
		shared.ExpectEqual(t, header.TD, mocks.MockBlock.Difficulty().String())
		shared.ExpectEqual(t, header.Reward, "2000000000000021250")
		shared.ExpectEqual(t, *header.BaseFee, mocks.MockHeader.BaseFee.Int64())
		dc, err := cid.Decode(header.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		var data []byte
		err = db.Get(&data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, data, mocks.MockHeaderRlp)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		// check that txs were properly indexed and published
		trxs := make([]string, 0)
		pgStr := `SELECT transaction_cids.cid FROM eth.transaction_cids INNER JOIN eth.header_cids ON (transaction_cids.header_id = header_cids.id)
				WHERE header_cids.block_number = $1`
		err = db.Select(&trxs, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(trxs), 5)
		expectTrue(t, shared.ListContainsString(trxs, trx1CID.String()))
		expectTrue(t, shared.ListContainsString(trxs, trx2CID.String()))
		expectTrue(t, shared.ListContainsString(trxs, trx3CID.String()))
		expectTrue(t, shared.ListContainsString(trxs, trx4CID.String()))
		expectTrue(t, shared.ListContainsString(trxs, trx5CID.String()))

		for _, c := range trxs {
			dc, err := cid.Decode(c)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = db.Get(&data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}
			switch c {
			case trx1CID.String():
				shared.ExpectEqual(t, data, tx1)
				var txType *uint8
				pgStr = `SELECT tx_type FROM eth.transaction_cids WHERE cid = $1`
				err = db.Get(&txType, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if txType != nil {
					t.Fatalf("expected nil tx_type, got %d", *txType)
				}
			case trx2CID.String():
				shared.ExpectEqual(t, data, tx2)
				var txType *uint8
				pgStr = `SELECT tx_type FROM eth.transaction_cids WHERE cid = $1`
				err = db.Get(&txType, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if txType != nil {
					t.Fatalf("expected nil tx_type, got %d", *txType)
				}
			case trx3CID.String():
				shared.ExpectEqual(t, data, tx3)
				var txType *uint8
				pgStr = `SELECT tx_type FROM eth.transaction_cids WHERE cid = $1`
				err = db.Get(&txType, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if txType != nil {
					t.Fatalf("expected nil tx_type, got %d", *txType)
				}
			case trx4CID.String():
				shared.ExpectEqual(t, data, tx4)
				var txType *uint8
				pgStr = `SELECT tx_type FROM eth.transaction_cids WHERE cid = $1`
				err = db.Get(&txType, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if *txType != types.AccessListTxType {
					t.Fatalf("expected AccessListTxType (1), got %d", *txType)
				}
				accessListElementModels := make([]models.AccessListElementModel, 0)
				pgStr = `SELECT access_list_element.* FROM eth.access_list_element INNER JOIN eth.transaction_cids ON (tx_id = transaction_cids.id) WHERE cid = $1 ORDER BY access_list_element.index ASC`
				err = db.Select(&accessListElementModels, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if len(accessListElementModels) != 2 {
					t.Fatalf("expected two access list entries, got %d", len(accessListElementModels))
				}
				model1 := models.AccessListElementModel{
					Index:   accessListElementModels[0].Index,
					Address: accessListElementModels[0].Address,
				}
				model2 := models.AccessListElementModel{
					Index:       accessListElementModels[1].Index,
					Address:     accessListElementModels[1].Address,
					StorageKeys: accessListElementModels[1].StorageKeys,
				}
				shared.ExpectEqual(t, model1, mocks.AccessListEntry1Model)
				shared.ExpectEqual(t, model2, mocks.AccessListEntry2Model)
			case trx5CID.String():
				shared.ExpectEqual(t, data, tx5)
				var txType *uint8
				pgStr = `SELECT tx_type FROM eth.transaction_cids WHERE cid = $1`
				err = db.Get(&txType, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if *txType != types.DynamicFeeTxType {
					t.Fatalf("expected DynamicFeeTxType (2), got %d", *txType)
				}
			}
		}
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setup(t)
		defer tearDown(t)

		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.id
				AND transaction_cids.header_id = header_cids.id
				AND header_cids.block_number = $1
				ORDER BY transaction_cids.index`
		err = db.Select(&rcts, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		type logIPLD struct {
			Index   int    `db:"index"`
			Address string `db:"address"`
			Data    []byte `db:"data"`
			Topic0  string `db:"topic0"`
			Topic1  string `db:"topic1"`
		}
		for i := range rcts {
			results := make([]logIPLD, 0)
			pgStr = `SELECT log_cids.index, log_cids.address, log_cids.Topic0, log_cids.Topic1, data FROM eth.log_cids
    				INNER JOIN eth.receipt_cids ON (log_cids.receipt_id = receipt_cids.id)
					INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
					WHERE receipt_cids.leaf_cid = $1 ORDER BY eth.log_cids.index ASC`
			err = db.Select(&results, pgStr, rcts[i])
			require.NoError(t, err)

			// expecting MockLog1 and MockLog2 for mockReceipt4
			expectedLogs := mocks.MockReceipts[i].Logs
			shared.ExpectEqual(t, len(results), len(expectedLogs))

			var nodeElements []interface{}
			for idx, r := range results {
				// Decode the log leaf node.
				err = rlp.DecodeBytes(r.Data, &nodeElements)
				require.NoError(t, err)

				logRaw, err := rlp.EncodeToBytes(expectedLogs[idx])
				require.NoError(t, err)

				// 2nd element of the leaf node contains the encoded log data.
				shared.ExpectEqual(t, logRaw, nodeElements[1].([]byte))
			}
		}
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)

		// check receipts were properly indexed and published
		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.id
				AND transaction_cids.header_id = header_cids.id
				AND header_cids.block_number = $1 order by transaction_cids.id`
		err = db.Select(&rcts, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(rcts), 5)
		expectTrue(t, shared.ListContainsString(rcts, rct1CID.String()))
		expectTrue(t, shared.ListContainsString(rcts, rct2CID.String()))
		expectTrue(t, shared.ListContainsString(rcts, rct3CID.String()))
		expectTrue(t, shared.ListContainsString(rcts, rct4CID.String()))
		expectTrue(t, shared.ListContainsString(rcts, rct5CID.String()))

		for idx, c := range rcts {
			result := make([]ipfs.BlockModel, 0)
			pgStr = `SELECT data
					FROM eth.receipt_cids
					INNER JOIN public.blocks ON (receipt_cids.leaf_mh_key = public.blocks.key)
					WHERE receipt_cids.leaf_cid = $1`
			err = db.Select(&result, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}

			// Decode the log leaf node.
			var nodeElements []interface{}
			err = rlp.DecodeBytes(result[0].Data, &nodeElements)
			require.NoError(t, err)

			expectedRct, err := mocks.MockReceipts[idx].MarshalBinary()
			require.NoError(t, err)

			shared.ExpectEqual(t, expectedRct, nodeElements[1].([]byte))

			dc, err := cid.Decode(c)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = db.Get(&data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}

			switch c {
			case rct1CID.String():
				shared.ExpectEqual(t, data, rctLeaf1)
				var postStatus uint64
				pgStr = `SELECT post_status FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(&postStatus, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postStatus, mocks.ExpectedPostStatus)
			case rct2CID.String():
				shared.ExpectEqual(t, data, rctLeaf2)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState1)
			case rct3CID.String():
				shared.ExpectEqual(t, data, rctLeaf3)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState2)
			case rct4CID.String():
				shared.ExpectEqual(t, data, rctLeaf4)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState3)
			case rct5CID.String():
				shared.ExpectEqual(t, data, rctLeaf5)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState3)
			}
		}
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		// check that state nodes were properly indexed and published
		stateNodes := make([]models.StateNodeModel, 0)
		pgStr := `SELECT state_cids.id, state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
				WHERE header_cids.block_number = $1 AND node_type != 3`
		err = db.Select(&stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(stateNodes), 2)
		for _, stateNode := range stateNodes {
			var data []byte
			dc, err := cid.Decode(stateNode.CID)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			err = db.Get(&data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}
			pgStr = `SELECT * from eth.state_accounts WHERE state_id = $1`
			var account models.StateAccountModel
			err = db.Get(&account, pgStr, stateNode.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stateNode.CID == state1CID.String() {
				shared.ExpectEqual(t, stateNode.NodeType, 2)
				shared.ExpectEqual(t, stateNode.StateKey, common.BytesToHash(mocks.ContractLeafKey).Hex())
				shared.ExpectEqual(t, stateNode.Path, []byte{'\x06'})
				shared.ExpectEqual(t, data, mocks.ContractLeafNode)
				shared.ExpectEqual(t, account, models.StateAccountModel{
					ID:          account.ID,
					StateID:     stateNode.ID,
					Balance:     "0",
					CodeHash:    mocks.ContractCodeHash.Bytes(),
					StorageRoot: mocks.ContractRoot,
					Nonce:       1,
				})
			}
			if stateNode.CID == state2CID.String() {
				shared.ExpectEqual(t, stateNode.NodeType, 2)
				shared.ExpectEqual(t, stateNode.StateKey, common.BytesToHash(mocks.AccountLeafKey).Hex())
				shared.ExpectEqual(t, stateNode.Path, []byte{'\x0c'})
				shared.ExpectEqual(t, data, mocks.AccountLeafNode)
				shared.ExpectEqual(t, account, models.StateAccountModel{
					ID:          account.ID,
					StateID:     stateNode.ID,
					Balance:     "1000",
					CodeHash:    mocks.AccountCodeHash.Bytes(),
					StorageRoot: mocks.AccountRoot,
					Nonce:       0,
				})
			}
		}

		// check that Removed state nodes were properly indexed and published
		stateNodes = make([]models.StateNodeModel, 0)
		pgStr = `SELECT state_cids.id, state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
				WHERE header_cids.block_number = $1 AND node_type = 3`
		err = db.Select(&stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(stateNodes), 1)
		stateNode := stateNodes[0]
		var data []byte
		dc, err := cid.Decode(stateNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		shared.ExpectEqual(t, prefixedKey, indexer.RemovedNodeMhKey)
		err = db.Get(&data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, stateNode.CID, indexer.RemovedNodeStateCID)
		shared.ExpectEqual(t, stateNode.Path, []byte{'\x02'})
		shared.ExpectEqual(t, data, []byte{})
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		// check that storage nodes were properly indexed
		storageNodes := make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr := `SELECT storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE storage_cids.state_id = state_cids.id
				AND state_cids.header_id = header_cids.id
				AND header_cids.block_number = $1
				AND storage_cids.node_type != 3`
		err = db.Select(&storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(storageNodes), 1)
		shared.ExpectEqual(t, storageNodes[0], models.StorageNodeWithStateKeyModel{
			CID:        storageCID.String(),
			NodeType:   2,
			StorageKey: common.BytesToHash(mocks.StorageLeafKey).Hex(),
			StateKey:   common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:       []byte{},
		})
		var data []byte
		dc, err := cid.Decode(storageNodes[0].CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		err = db.Get(&data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, data, mocks.StorageLeafNode)

		// check that Removed storage nodes were properly indexed
		storageNodes = make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr = `SELECT storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE storage_cids.state_id = state_cids.id
				AND state_cids.header_id = header_cids.id
				AND header_cids.block_number = $1
				AND storage_cids.node_type = 3`
		err = db.Select(&storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, len(storageNodes), 1)
		shared.ExpectEqual(t, storageNodes[0], models.StorageNodeWithStateKeyModel{
			CID:        indexer.RemovedNodeStorageCID,
			NodeType:   3,
			StorageKey: common.BytesToHash(mocks.RemovedLeafKey).Hex(),
			StateKey:   common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:       []byte{'\x03'},
		})
		dc, err = cid.Decode(storageNodes[0].CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey = dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey = blockstore.BlockPrefix.String() + mhKey.String()
		shared.ExpectEqual(t, prefixedKey, indexer.RemovedNodeMhKey)
		err = db.Get(&data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		shared.ExpectEqual(t, data, []byte{})
	})
}

func TestWatchAddressMethods(t *testing.T) {
	setupIndexer(t)
	defer tearDown(t)

	type res struct {
		Address      string `db:"address"`
		Kind         int    `db:"kind"`
		CreatedAt    uint64 `db:"created_at"`
		WatchedAt    uint64 `db:"watched_at"`
		LastFilledAt uint64 `db:"last_filled_at"`
	}
	pgStr := "SELECT * FROM eth.watched_addresses"

	// Watched addresses
	t.Run("Insert watched addresses", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt1)), sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt2)), sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Remove watched addresses", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.RemoveWatchedAddresses(args, sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{}

		ind.RemoveWatchedAddresses(args, sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Set watched addresses", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
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
		expectedData := []res{
			{
				Address:      contract1Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt2)), sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
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
		expectedData := []res{
			{
				Address:      contract4Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract4CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				Kind:         sdtypes.WatchedAddress.Int(),
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt3)), sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Clear watched addresses", func(t *testing.T) {
		expectedData := []res{}

		ind.ClearWatchedAddresses(sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
		expectedData := []res{}

		ind.ClearWatchedAddresses(sdtypes.WatchedAddress)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	// Watched storage slots
	// Reset the db.
	tearDown(t)

	t.Run("Insert watched storage slots", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot1StorageKeyHex,
				CreatedAt: slot1CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      slot1StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot2StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt1)), sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Insert watched storage slots (some already watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot3StorageKeyHex,
				CreatedAt: slot3CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      slot1StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot2StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot3StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt2)), sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Remove watched storage slots", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot3StorageKeyHex,
				CreatedAt: slot3CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      slot1StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.RemoveWatchedAddresses(args, sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Remove watched storage slots (some non-watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot1StorageKeyHex,
				CreatedAt: slot1CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
		}
		expectedData := []res{}

		ind.RemoveWatchedAddresses(args, sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Set watched storage slots", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot1StorageKeyHex,
				CreatedAt: slot1CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
			{
				Address:   slot3StorageKeyHex,
				CreatedAt: slot3CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      slot1StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot1CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot2StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot2CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot3StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt2)), sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Set watched storage slots (some already watched)", func(t *testing.T) {
		args := []sdtypes.WatchAddressArg{
			{
				Address:   slot4StorageKeyHex,
				CreatedAt: slot4CreatedAt,
			},
			{
				Address:   slot2StorageKeyHex,
				CreatedAt: slot2CreatedAt,
			},
			{
				Address:   slot3StorageKeyHex,
				CreatedAt: slot3CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      slot4StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot4CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot2StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot2CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      slot3StorageKeyHex,
				Kind:         sdtypes.WatchedStorageSlot.Int(),
				CreatedAt:    slot3CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
		}

		ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt3)), sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Clear watched storage slots", func(t *testing.T) {
		expectedData := []res{}

		ind.ClearWatchedAddresses(sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})

	t.Run("Clear watched storage slots (empty table)", func(t *testing.T) {
		expectedData := []res{}

		ind.ClearWatchedAddresses(sdtypes.WatchedStorageSlot)

		rows := []res{}
		err = db.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			shared.ExpectEqual(t, row, expectedData[idx])
		}
	})
}
