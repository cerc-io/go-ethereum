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
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
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
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5  []byte
	mockBlock                                              *types.Block
	headerCID, trx1CID, trx2CID, trx3CID, trx4CID, trx5CID cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID            cid.Cid
	state1CID, state2CID, storageCID                       cid.Cid
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
	rct1CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct1, multihash.KECCAK_256)
	rct2CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct2, multihash.KECCAK_256)
	rct3CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct3, multihash.KECCAK_256)
	rct4CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct4, multihash.KECCAK_256)
	rct5CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)
}

func setup(t *testing.T) {
	db, err = shared.SetupDB()
	if err != nil {
		t.Fatal(err)
	}
	ind = indexer.NewStateDiffIndexer(mocks.TestConfig, db)
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
		// check that txs were properly indexed
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
		// and published
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

	t.Run("Publish and index log IPLDs for single receipt", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		type logIPLD struct {
			Index   int    `db:"index"`
			Address string `db:"address"`
			Data    []byte `db:"data"`
			Topic0  string `db:"topic0"`
			Topic1  string `db:"topic1"`
		}

		results := make([]logIPLD, 0)
		pgStr := `SELECT log_cids.index, log_cids.address, log_cids.Topic0, log_cids.Topic1, data FROM eth.log_cids
    				INNER JOIN eth.receipt_cids ON (log_cids.receipt_id = receipt_cids.id)
					INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
					WHERE receipt_cids.cid = $1 ORDER BY eth.log_cids.index ASC`
		err = db.Select(&results, pgStr, rct4CID.String())
		require.NoError(t, err)

		// expecting MockLog1 and MockLog2 for mockReceipt4
		expectedLogs := mocks.MockReceipts[3].Logs
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
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		defer tearDown(t)
		// check receipts were properly indexed
		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.id
				AND transaction_cids.header_id = header_cids.id
				AND header_cids.block_number = $1`
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
		// and published
		for _, c := range rcts {
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
				shared.ExpectEqual(t, data, rct1)
				var postStatus uint64
				pgStr = `SELECT post_status FROM eth.receipt_cids WHERE cid = $1`
				err = db.Get(&postStatus, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postStatus, mocks.ExpectedPostStatus)
			case rct2CID.String():
				shared.ExpectEqual(t, data, rct2)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState1)
			case rct3CID.String():
				shared.ExpectEqual(t, data, rct3)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState2)
			case rct4CID.String():
				shared.ExpectEqual(t, data, rct4)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE cid = $1`
				err = db.Get(&postState, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				shared.ExpectEqual(t, postState, mocks.ExpectedPostState3)
			case rct5CID.String():
				shared.ExpectEqual(t, data, rct5)
				var postState string
				pgStr = `SELECT post_state FROM eth.receipt_cids WHERE cid = $1`
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
				WHERE header_cids.block_number = $1`
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
				AND header_cids.block_number = $1`
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
	})
}
