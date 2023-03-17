// VulcanizeDB
// Copyright © 2022 Vulcanize

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

package test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

// SetupTestData indexes a single mock block along with it's state nodes
func SetupTestData(t *testing.T, ind interfaces.StateDiffIndexer) {
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	if batchTx, ok := tx.(*sql.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), batchTx.BlockNumber)
	} else if batchTx, ok := tx.(*file.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), batchTx.BlockNumber)
	}
}

func TestPublishAndIndexHeaderIPLDs(t *testing.T, db sql.Database) {
	pgStr := `SELECT cid, cast(td AS TEXT), cast(reward AS TEXT), block_hash, coinbase
	FROM eth.header_cids
	WHERE block_number = $1`
	// check header was properly indexed
	type res struct {
		CID       string
		TD        string
		Reward    string
		BlockHash string `db:"block_hash"`
		Coinbase  string `db:"coinbase"`
	}
	header := new(res)
	err = db.QueryRow(context.Background(), pgStr, mocks.BlockNumber.Uint64()).Scan(
		&header.CID,
		&header.TD,
		&header.Reward,
		&header.BlockHash,
		&header.Coinbase)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, headerCID.String(), header.CID)
	require.Equal(t, mocks.MockBlock.Difficulty().String(), header.TD)
	require.Equal(t, "2000000000000021250", header.Reward)
	require.Equal(t, mocks.MockHeader.Coinbase.String(), header.Coinbase)
	dc, err := cid.Decode(header.CID)
	if err != nil {
		t.Fatal(err)
	}
	mhKey := dshelp.MultihashToDsKey(dc.Hash())
	prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
	var data []byte
	err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, mocks.MockHeaderRlp, data)
}

func TestPublishAndIndexTransactionIPLDs(t *testing.T, db sql.Database) {
	// check that txs were properly indexed and published
	trxs := make([]string, 0)
	pgStr := `SELECT transaction_cids.cid FROM eth.transaction_cids INNER JOIN eth.header_cids ON (transaction_cids.header_id = header_cids.block_hash)
					WHERE header_cids.block_number = $1`
	err = db.Select(context.Background(), &trxs, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 5, len(trxs))
	expectTrue(t, test_helpers.ListContainsString(trxs, trx1CID.String()))
	expectTrue(t, test_helpers.ListContainsString(trxs, trx2CID.String()))
	expectTrue(t, test_helpers.ListContainsString(trxs, trx3CID.String()))
	expectTrue(t, test_helpers.ListContainsString(trxs, trx4CID.String()))
	expectTrue(t, test_helpers.ListContainsString(trxs, trx5CID.String()))

	transactions := mocks.MockBlock.Transactions()
	type txResult struct {
		TxType uint8 `db:"tx_type"`
		Value  string
	}
	for _, c := range trxs {
		dc, err := cid.Decode(c)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		var data []byte
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		txTypeAndValueStr := `SELECT tx_type, CAST(value as TEXT) FROM eth.transaction_cids WHERE cid = $1`
		switch c {
		case trx1CID.String():
			require.Equal(t, tx1, data)
			txRes := new(txResult)
			err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
			if err != nil {
				t.Fatal(err)
			}
			if txRes.TxType != 0 {
				t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
			}
			if txRes.Value != transactions[0].Value().String() {
				t.Fatalf("expected tx value %s got %s", transactions[0].Value().String(), txRes.Value)
			}
		case trx2CID.String():
			require.Equal(t, tx2, data)
			txRes := new(txResult)
			err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
			if err != nil {
				t.Fatal(err)
			}
			if txRes.TxType != 0 {
				t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
			}
			if txRes.Value != transactions[1].Value().String() {
				t.Fatalf("expected tx value %s got %s", transactions[1].Value().String(), txRes.Value)
			}
		case trx3CID.String():
			require.Equal(t, tx3, data)
			txRes := new(txResult)
			err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
			if err != nil {
				t.Fatal(err)
			}
			if txRes.TxType != 0 {
				t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
			}
			if txRes.Value != transactions[2].Value().String() {
				t.Fatalf("expected tx value %s got %s", transactions[2].Value().String(), txRes.Value)
			}
		case trx4CID.String():
			require.Equal(t, tx4, data)
			txRes := new(txResult)
			err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
			if err != nil {
				t.Fatal(err)
			}
			if txRes.TxType != types.AccessListTxType {
				t.Fatalf("expected AccessListTxType (1), got %d", txRes.TxType)
			}
			if txRes.Value != transactions[3].Value().String() {
				t.Fatalf("expected tx value %s got %s", transactions[3].Value().String(), txRes.Value)
			}
			accessListElementModels := make([]models.AccessListElementModel, 0)
			pgStr = "SELECT cast(access_list_elements.block_number AS TEXT), access_list_elements.index, access_list_elements.tx_id, " +
				"access_list_elements.address, access_list_elements.storage_keys FROM eth.access_list_elements " +
				"INNER JOIN eth.transaction_cids ON (tx_id = transaction_cids.tx_hash) WHERE cid = $1 ORDER BY access_list_elements.index ASC"
			err = db.Select(context.Background(), &accessListElementModels, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			if len(accessListElementModels) != 2 {
				t.Fatalf("expected two access list entries, got %d", len(accessListElementModels))
			}
			model1 := models.AccessListElementModel{
				BlockNumber: mocks.BlockNumber.String(),
				Index:       accessListElementModels[0].Index,
				Address:     accessListElementModels[0].Address,
			}
			model2 := models.AccessListElementModel{
				BlockNumber: mocks.BlockNumber.String(),
				Index:       accessListElementModels[1].Index,
				Address:     accessListElementModels[1].Address,
				StorageKeys: accessListElementModels[1].StorageKeys,
			}
			require.Equal(t, mocks.AccessListEntry1Model, model1)
			require.Equal(t, mocks.AccessListEntry2Model, model2)
		case trx5CID.String():
			require.Equal(t, tx5, data)
			txRes := new(txResult)
			err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
			if err != nil {
				t.Fatal(err)
			}
			if txRes.TxType != types.DynamicFeeTxType {
				t.Fatalf("expected DynamicFeeTxType (2), got %d", txRes.TxType)
			}
			if txRes.Value != transactions[4].Value().String() {
				t.Fatalf("expected tx value %s got %s", transactions[4].Value().String(), txRes.Value)
			}
		}
	}
}

func TestPublishAndIndexLogIPLDs(t *testing.T, db sql.Database) {
	rcts := make([]string, 0)
	rctsPgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
			WHERE receipt_cids.tx_id = transaction_cids.tx_hash
			AND transaction_cids.header_id = header_cids.block_hash
			AND header_cids.block_number = $1
			ORDER BY transaction_cids.index`
	logsPgStr := `SELECT log_cids.index, log_cids.address, log_cids.topic0, log_cids.topic1, data FROM eth.log_cids
				INNER JOIN eth.receipt_cids ON (log_cids.rct_id = receipt_cids.tx_id)
				INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
				WHERE receipt_cids.leaf_cid = $1 ORDER BY eth.log_cids.index ASC`
	err = db.Select(context.Background(), &rcts, rctsPgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	if len(rcts) != len(mocks.MockReceipts) {
		t.Fatalf("expected %d receipts, got %d", len(mocks.MockReceipts), len(rcts))
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
		err = db.Select(context.Background(), &results, logsPgStr, rcts[i])
		require.NoError(t, err)

		expectedLogs := mocks.MockReceipts[i].Logs
		require.Equal(t, len(expectedLogs), len(results))

		var nodeElements []interface{}
		for idx, r := range results {
			// Attempt to decode the log leaf node.
			err = rlp.DecodeBytes(r.Data, &nodeElements)
			require.NoError(t, err)
			if len(nodeElements) == 2 {
				logRaw, err := rlp.EncodeToBytes(&expectedLogs[idx])
				require.NoError(t, err)
				// 2nd element of the leaf node contains the encoded log data.
				require.Equal(t, nodeElements[1].([]byte), logRaw)
			} else {
				logRaw, err := rlp.EncodeToBytes(&expectedLogs[idx])
				require.NoError(t, err)
				// raw log was IPLDized
				require.Equal(t, r.Data, logRaw)
			}
		}
	}
}

func TestPublishAndIndexReceiptIPLDs(t *testing.T, db sql.Database) {
	// check receipts were properly indexed and published
	rcts := make([]string, 0)
	pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.tx_hash
				AND transaction_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1 order by transaction_cids.index`
	err = db.Select(context.Background(), &rcts, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 5, len(rcts))
	expectTrue(t, test_helpers.ListContainsString(rcts, rct1CID.String()))
	expectTrue(t, test_helpers.ListContainsString(rcts, rct2CID.String()))
	expectTrue(t, test_helpers.ListContainsString(rcts, rct3CID.String()))
	expectTrue(t, test_helpers.ListContainsString(rcts, rct4CID.String()))
	expectTrue(t, test_helpers.ListContainsString(rcts, rct5CID.String()))

	for idx, c := range rcts {
		result := make([]models.IPLDModel, 0)
		pgStr = `SELECT data
					FROM eth.receipt_cids
					INNER JOIN public.blocks ON (receipt_cids.leaf_mh_key = public.blocks.key)
					WHERE receipt_cids.leaf_cid = $1`
		err = db.Select(context.Background(), &result, pgStr, c)
		if err != nil {
			t.Fatal(err)
		}

		// Decode the receipt leaf node.
		var nodeElements []interface{}
		err = rlp.DecodeBytes(result[0].Data, &nodeElements)
		require.NoError(t, err)

		expectedRct, err := mocks.MockReceipts[idx].MarshalBinary()
		require.NoError(t, err)

		require.Equal(t, nodeElements[1].([]byte), expectedRct)

		dc, err := cid.Decode(c)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		var data []byte
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		postStatePgStr := `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
		switch c {
		case rct1CID.String():
			require.Equal(t, rctLeaf1, data)
			var postStatus uint64
			pgStr = `SELECT post_status FROM eth.receipt_cids WHERE leaf_cid = $1`
			err = db.Get(context.Background(), &postStatus, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostStatus, postStatus)
		case rct2CID.String():
			require.Equal(t, rctLeaf2, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState1, postState)
		case rct3CID.String():
			require.Equal(t, rctLeaf3, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState2, postState)
		case rct4CID.String():
			require.Equal(t, rctLeaf4, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState3, postState)
		case rct5CID.String():
			require.Equal(t, rctLeaf5, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState3, postState)
		}
	}
}

func TestPublishAndIndexStateIPLDs(t *testing.T, db sql.Database) {
	// check that state nodes were properly indexed and published
	stateNodes := make([]models.StateNodeModel, 0)
	pgStr := `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type != 3`
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 2, len(stateNodes))
	for _, stateNode := range stateNodes {
		var data []byte
		dc, err := cid.Decode(stateNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		pgStr = `SELECT cast(block_number AS TEXT), header_id, state_path, cast(balance AS TEXT), nonce, code_hash, storage_root from eth.state_accounts WHERE header_id = $1 AND state_path = $2`
		var account models.StateAccountModel
		err = db.Get(context.Background(), &account, pgStr, stateNode.HeaderID, stateNode.Path)
		if err != nil {
			t.Fatal(err)
		}
		if stateNode.CID == state1CID.String() {
			require.Equal(t, 2, stateNode.NodeType)
			require.Equal(t, common.BytesToHash(mocks.ContractLeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, []byte{'\x06'}, stateNode.Path)
			require.Equal(t, mocks.ContractLeafNode, data)
			require.Equal(t, models.StateAccountModel{
				BlockNumber: mocks.BlockNumber.String(),
				HeaderID:    account.HeaderID,
				StatePath:   stateNode.Path,
				Balance:     "0",
				CodeHash:    mocks.ContractCodeHash.Bytes(),
				StorageRoot: mocks.ContractRoot,
				Nonce:       1,
			}, account)
		}
		if stateNode.CID == state2CID.String() {
			require.Equal(t, 2, stateNode.NodeType)
			require.Equal(t, common.BytesToHash(mocks.AccountLeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, []byte{'\x0c'}, stateNode.Path)
			require.Equal(t, mocks.AccountLeafNode, data)
			require.Equal(t, models.StateAccountModel{
				BlockNumber: mocks.BlockNumber.String(),
				HeaderID:    account.HeaderID,
				StatePath:   stateNode.Path,
				Balance:     mocks.Balance.String(),
				CodeHash:    mocks.AccountCodeHash.Bytes(),
				StorageRoot: mocks.AccountRoot,
				Nonce:       0,
			}, account)
		}
	}

	// check that Removed state nodes were properly indexed and published
	stateNodes = make([]models.StateNodeModel, 0)
	pgStr = `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type = 3
				ORDER BY state_path`
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 2, len(stateNodes))
	for idx, stateNode := range stateNodes {
		var data []byte
		dc, err := cid.Decode(stateNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		require.Equal(t, shared.RemovedNodeMhKey, prefixedKey)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		if idx == 0 {
			require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
			require.Equal(t, common.BytesToHash(mocks.RemovedLeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, []byte{'\x02'}, stateNode.Path)
			require.Equal(t, []byte{}, data)
		}
		if idx == 1 {
			require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
			require.Equal(t, common.BytesToHash(mocks.Contract2LeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, []byte{'\x07'}, stateNode.Path)
			require.Equal(t, []byte{}, data)
		}
	}
}

func TestPublishAndIndexStorageIPLDs(t *testing.T, db sql.Database) {
	// check that storage nodes were properly indexed
	storageNodes := make([]models.StorageNodeWithStateKeyModel, 0)
	pgStr := `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type != 3
				ORDER BY storage_path`
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 1, len(storageNodes))
	require.Equal(t, models.StorageNodeWithStateKeyModel{
		BlockNumber: mocks.BlockNumber.String(),
		CID:         storageCID.String(),
		NodeType:    2,
		StorageKey:  common.BytesToHash(mocks.StorageLeafKey).Hex(),
		StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
		Path:        []byte{},
	}, storageNodes[0])
	var data []byte
	dc, err := cid.Decode(storageNodes[0].CID)
	if err != nil {
		t.Fatal(err)
	}
	mhKey := dshelp.MultihashToDsKey(dc.Hash())
	prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
	err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, mocks.StorageLeafNode, data)

	// check that Removed storage nodes were properly indexed
	storageNodes = make([]models.StorageNodeWithStateKeyModel, 0)
	pgStr = `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type = 3
				ORDER BY storage_path`
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 3, len(storageNodes))
	expectedStorageNodes := []models.StorageNodeWithStateKeyModel{
		{
			BlockNumber: mocks.BlockNumber.String(),
			CID:         shared.RemovedNodeStorageCID,
			NodeType:    3,
			StorageKey:  common.BytesToHash(mocks.RemovedLeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:        []byte{'\x03'},
		},
		{
			BlockNumber: mocks.BlockNumber.String(),
			CID:         shared.RemovedNodeStorageCID,
			NodeType:    3,
			StorageKey:  common.BytesToHash(mocks.Storage2LeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
			Path:        []byte{'\x0e'},
		},
		{
			BlockNumber: mocks.BlockNumber.String(),
			CID:         shared.RemovedNodeStorageCID,
			NodeType:    3,
			StorageKey:  common.BytesToHash(mocks.Storage3LeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
			Path:        []byte{'\x0f'},
		},
	}
	for idx, storageNode := range storageNodes {
		require.Equal(t, expectedStorageNodes[idx], storageNode)
		dc, err = cid.Decode(storageNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey = dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey = blockstore.BlockPrefix.String() + mhKey.String()
		require.Equal(t, shared.RemovedNodeMhKey, prefixedKey)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, []byte{}, data)
	}
}

// SetupTestDataNonCanonical indexes a mock block and a non-canonical mock block at London height
// and a non-canonical block at London height + 1
// along with their state nodes
func SetupTestDataNonCanonical(t *testing.T, ind interfaces.StateDiffIndexer) {
	// index a canonical block at London height
	var tx1 interfaces.Batch
	tx1, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx1, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	if batchTx, ok := tx1.(*sql.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), batchTx.BlockNumber)
	} else if batchTx, ok := tx1.(*file.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), batchTx.BlockNumber)
	}

	if err := tx1.Submit(err); err != nil {
		t.Fatal(err)
	}

	// index a non-canonical block at London height
	// has transactions overlapping with that of the canonical block
	var tx2 interfaces.Batch
	tx2, err = ind.PushBlock(
		mockNonCanonicalBlock,
		mocks.MockNonCanonicalBlockReceipts,
		mockNonCanonicalBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx2, node, mockNonCanonicalBlock.Hash().String())
		require.NoError(t, err)
	}

	if tx, ok := tx2.(*sql.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), tx.BlockNumber)
	} else if tx, ok := tx2.(*sql.BatchTx); ok {
		require.Equal(t, mocks.BlockNumber.String(), tx.BlockNumber)
	}

	if err := tx2.Submit(err); err != nil {
		t.Fatal(err)
	}

	// index a non-canonical block at London height + 1
	// has transactions overlapping with that of the canonical block
	var tx3 interfaces.Batch
	tx3, err = ind.PushBlock(
		mockNonCanonicalBlock2,
		mocks.MockNonCanonicalBlock2Receipts,
		mockNonCanonicalBlock2.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs[:2] {
		err = ind.PushStateNode(tx3, node, mockNonCanonicalBlock2.Hash().String())
		require.NoError(t, err)
	}

	if batchTx, ok := tx3.(*sql.BatchTx); ok {
		require.Equal(t, mocks.Block2Number.String(), batchTx.BlockNumber)
	} else if batchTx, ok := tx3.(*file.BatchTx); ok {
		require.Equal(t, mocks.Block2Number.String(), batchTx.BlockNumber)
	}

	if err := tx3.Submit(err); err != nil {
		t.Fatal(err)
	}
}

func TestPublishAndIndexHeaderNonCanonical(t *testing.T, db sql.Database) {
	// check indexed headers
	pgStr := `SELECT CAST(block_number as TEXT), block_hash, cid, cast(td AS TEXT), cast(reward AS TEXT),
			tx_root, receipt_root, uncle_root, coinbase
			FROM eth.header_cids
			ORDER BY block_number`
	headerRes := make([]models.HeaderModel, 0)
	err = db.Select(context.Background(), &headerRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	// expect three blocks to be indexed
	// a canonical and a non-canonical block at London height,
	// non-canonical block at London height + 1
	expectedRes := []models.HeaderModel{
		{
			BlockNumber:     mockBlock.Number().String(),
			BlockHash:       mockBlock.Hash().String(),
			CID:             headerCID.String(),
			TotalDifficulty: mockBlock.Difficulty().String(),
			TxRoot:          mockBlock.TxHash().String(),
			RctRoot:         mockBlock.ReceiptHash().String(),
			UncleRoot:       mockBlock.UncleHash().String(),
			Coinbase:        mocks.MockHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock.Number().String(),
			BlockHash:       mockNonCanonicalBlock.Hash().String(),
			CID:             mockNonCanonicalHeaderCID.String(),
			TotalDifficulty: mockNonCanonicalBlock.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock.ReceiptHash().String(),
			UncleRoot:       mockNonCanonicalBlock.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock2.Number().String(),
			BlockHash:       mockNonCanonicalBlock2.Hash().String(),
			CID:             mockNonCanonicalHeader2CID.String(),
			TotalDifficulty: mockNonCanonicalBlock2.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock2.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock2.ReceiptHash().String(),
			UncleRoot:       mockNonCanonicalBlock2.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader2.Coinbase.String(),
		},
	}
	expectedRes[0].Reward = shared.CalcEthBlockReward(mockBlock.Header(), mockBlock.Uncles(), mockBlock.Transactions(), mocks.MockReceipts).String()
	expectedRes[1].Reward = shared.CalcEthBlockReward(mockNonCanonicalBlock.Header(), mockNonCanonicalBlock.Uncles(), mockNonCanonicalBlock.Transactions(), mocks.MockNonCanonicalBlockReceipts).String()
	expectedRes[2].Reward = shared.CalcEthBlockReward(mockNonCanonicalBlock2.Header(), mockNonCanonicalBlock2.Uncles(), mockNonCanonicalBlock2.Transactions(), mocks.MockNonCanonicalBlock2Receipts).String()

	require.Equal(t, len(expectedRes), len(headerRes))
	require.ElementsMatch(t,
		[]string{mockBlock.Hash().String(), mockNonCanonicalBlock.Hash().String(), mockNonCanonicalBlock2.Hash().String()},
		[]string{headerRes[0].BlockHash, headerRes[1].BlockHash, headerRes[2].BlockHash},
	)

	if headerRes[0].BlockHash == mockBlock.Hash().String() {
		require.Equal(t, expectedRes[0], headerRes[0])
		require.Equal(t, expectedRes[1], headerRes[1])
		require.Equal(t, expectedRes[2], headerRes[2])
	} else {
		require.Equal(t, expectedRes[1], headerRes[0])
		require.Equal(t, expectedRes[0], headerRes[1])
		require.Equal(t, expectedRes[2], headerRes[2])
	}

	// check indexed IPLD blocks
	headerCIDs := []cid.Cid{headerCID, mockNonCanonicalHeaderCID, mockNonCanonicalHeader2CID}
	blockNumbers := []uint64{mocks.BlockNumber.Uint64(), mocks.BlockNumber.Uint64(), mocks.Block2Number.Uint64()}
	headerRLPs := [][]byte{mocks.MockHeaderRlp, mocks.MockNonCanonicalHeaderRlp, mocks.MockNonCanonicalHeader2Rlp}
	for i := range expectedRes {
		var data []byte
		prefixedKey := shared.MultihashKeyFromCID(headerCIDs[i])
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, blockNumbers[i])
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, headerRLPs[i], data)
	}
}

func TestPublishAndIndexTransactionsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed transactions
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_hash, cid, dst, src, index,
		tx_data, tx_type, CAST(value as TEXT)
		FROM eth.transaction_cids
		ORDER BY block_number, index`
	txRes := make([]models.TxModel, 0)
	err = db.Select(context.Background(), &txRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	// expected transactions in the canonical block
	mockBlockTxs := mocks.MockBlock.Transactions()
	expectedBlockTxs := []models.TxModel{
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[0].Hash().String(),
			CID:         trx1CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[0].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockBlockTxs[0].Data(),
			Type:        mockBlockTxs[0].Type(),
			Value:       mockBlockTxs[0].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[1].Hash().String(),
			CID:         trx2CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[1].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockBlockTxs[1].Data(),
			Type:        mockBlockTxs[1].Type(),
			Value:       mockBlockTxs[1].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[2].Hash().String(),
			CID:         trx3CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[2].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       2,
			Data:        mockBlockTxs[2].Data(),
			Type:        mockBlockTxs[2].Type(),
			Value:       mockBlockTxs[2].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[3].Hash().String(),
			CID:         trx4CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[3].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       3,
			Data:        mockBlockTxs[3].Data(),
			Type:        mockBlockTxs[3].Type(),
			Value:       mockBlockTxs[3].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[4].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[4].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       4,
			Data:        mockBlockTxs[4].Data(),
			Type:        mockBlockTxs[4].Type(),
			Value:       mockBlockTxs[4].Value().String(),
		},
	}

	// expected transactions in the non-canonical block at London height
	mockNonCanonicalBlockTxs := mockNonCanonicalBlock.Transactions()
	expectedNonCanonicalBlockTxs := []models.TxModel{
		{
			BlockNumber: mockNonCanonicalBlock.Number().String(),
			HeaderID:    mockNonCanonicalBlock.Hash().String(),
			TxHash:      mockNonCanonicalBlockTxs[0].Hash().String(),
			CID:         trx2CID.String(),
			Dst:         mockNonCanonicalBlockTxs[0].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockNonCanonicalBlockTxs[0].Data(),
			Type:        mockNonCanonicalBlockTxs[0].Type(),
			Value:       mockNonCanonicalBlockTxs[0].Value().String(),
		},
		{
			BlockNumber: mockNonCanonicalBlock.Number().String(),
			HeaderID:    mockNonCanonicalBlock.Hash().String(),
			TxHash:      mockNonCanonicalBlockTxs[1].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         mockNonCanonicalBlockTxs[1].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockNonCanonicalBlockTxs[1].Data(),
			Type:        mockNonCanonicalBlockTxs[1].Type(),
			Value:       mockNonCanonicalBlockTxs[1].Value().String(),
		},
	}

	// expected transactions in the non-canonical block at London height + 1
	mockNonCanonicalBlock2Txs := mockNonCanonicalBlock2.Transactions()
	expectedNonCanonicalBlock2Txs := []models.TxModel{
		{
			BlockNumber: mockNonCanonicalBlock2.Number().String(),
			HeaderID:    mockNonCanonicalBlock2.Hash().String(),
			TxHash:      mockNonCanonicalBlock2Txs[0].Hash().String(),
			CID:         trx3CID.String(),
			Dst:         "",
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockNonCanonicalBlock2Txs[0].Data(),
			Type:        mockNonCanonicalBlock2Txs[0].Type(),
			Value:       mockNonCanonicalBlock2Txs[0].Value().String(),
		},
		{
			BlockNumber: mockNonCanonicalBlock2.Number().String(),
			HeaderID:    mockNonCanonicalBlock2.Hash().String(),
			TxHash:      mockNonCanonicalBlock2Txs[1].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         mockNonCanonicalBlock2Txs[1].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockNonCanonicalBlock2Txs[1].Data(),
			Type:        mockNonCanonicalBlock2Txs[1].Type(),
			Value:       mockNonCanonicalBlock2Txs[1].Value().String(),
		},
	}

	require.Equal(t, len(expectedBlockTxs)+len(expectedNonCanonicalBlockTxs)+len(expectedNonCanonicalBlock2Txs), len(txRes))

	// sort results such that non-canonical block transactions come after canonical block ones
	sort.SliceStable(txRes, func(i, j int) bool {
		if txRes[i].BlockNumber < txRes[j].BlockNumber {
			return true
		} else if txRes[i].HeaderID == txRes[j].HeaderID {
			return txRes[i].Index < txRes[j].Index
		} else if txRes[i].HeaderID == mockBlock.Hash().String() {
			return true
		} else {
			return false
		}
	})

	for i, expectedTx := range expectedBlockTxs {
		require.Equal(t, expectedTx, txRes[i])
	}
	for i, expectedTx := range expectedNonCanonicalBlockTxs {
		require.Equal(t, expectedTx, txRes[len(expectedBlockTxs)+i])
	}
	for i, expectedTx := range expectedNonCanonicalBlock2Txs {
		require.Equal(t, expectedTx, txRes[len(expectedBlockTxs)+len(expectedNonCanonicalBlockTxs)+i])
	}

	// check indexed IPLD blocks
	var data []byte
	var prefixedKey string

	txCIDs := []cid.Cid{trx1CID, trx2CID, trx3CID, trx4CID, trx5CID}
	txRLPs := [][]byte{tx1, tx2, tx3, tx4, tx5}
	for i, txCID := range txCIDs {
		prefixedKey = shared.MultihashKeyFromCID(txCID)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, txRLPs[i], data)
	}
}

func TestPublishAndIndexReceiptsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed receipts
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_id, leaf_cid, leaf_mh_key, post_status, post_state, contract, contract_hash, log_root
		FROM eth.receipt_cids
		ORDER BY block_number`
	rctRes := make([]models.ReceiptModel, 0)
	err = db.Select(context.Background(), &rctRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	// expected receipts in the canonical block
	rctCids := []cid.Cid{rct1CID, rct2CID, rct3CID, rct4CID, rct5CID}
	expectedBlockRctsMap := make(map[string]models.ReceiptModel, len(mocks.MockReceipts))
	for i, mockBlockRct := range mocks.MockReceipts {
		rctModel := createRctModel(mockBlockRct, rctCids[i], mockBlock.Number().String())
		expectedBlockRctsMap[rctCids[i].String()] = rctModel
	}

	// expected receipts in the non-canonical block at London height
	nonCanonicalBlockRctCids := []cid.Cid{nonCanonicalBlockRct1CID, nonCanonicalBlockRct2CID}
	expectedNonCanonicalBlockRctsMap := make(map[string]models.ReceiptModel, len(mocks.MockNonCanonicalBlockReceipts))
	for i, mockNonCanonicalBlockRct := range mocks.MockNonCanonicalBlockReceipts {
		rctModel := createRctModel(mockNonCanonicalBlockRct, nonCanonicalBlockRctCids[i], mockNonCanonicalBlock.Number().String())
		expectedNonCanonicalBlockRctsMap[nonCanonicalBlockRctCids[i].String()] = rctModel
	}

	// expected receipts in the non-canonical block at London height + 1
	nonCanonicalBlock2RctCids := []cid.Cid{nonCanonicalBlock2Rct1CID, nonCanonicalBlock2Rct2CID}
	expectedNonCanonicalBlock2RctsMap := make(map[string]models.ReceiptModel, len(mocks.MockNonCanonicalBlock2Receipts))
	for i, mockNonCanonicalBlock2Rct := range mocks.MockNonCanonicalBlock2Receipts {
		rctModel := createRctModel(mockNonCanonicalBlock2Rct, nonCanonicalBlock2RctCids[i], mockNonCanonicalBlock2.Number().String())
		expectedNonCanonicalBlock2RctsMap[nonCanonicalBlock2RctCids[i].String()] = rctModel
	}

	require.Equal(t, len(expectedBlockRctsMap)+len(expectedNonCanonicalBlockRctsMap)+len(expectedNonCanonicalBlock2RctsMap), len(rctRes))

	// sort results such that non-canonical block reciepts come after canonical block ones
	sort.SliceStable(rctRes, func(i, j int) bool {
		if rctRes[i].BlockNumber < rctRes[j].BlockNumber {
			return true
		} else if rctRes[i].HeaderID == rctRes[j].HeaderID {
			return false
		} else if rctRes[i].HeaderID == mockBlock.Hash().String() {
			return true
		} else {
			return false
		}
	})

	for i := 0; i < len(expectedBlockRctsMap); i++ {
		rct := rctRes[i]
		require.Contains(t, expectedBlockRctsMap, rct.LeafCID)
		require.Equal(t, expectedBlockRctsMap[rct.LeafCID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlockRctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlockRctsMap, rct.LeafCID)
		require.Equal(t, expectedNonCanonicalBlockRctsMap[rct.LeafCID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlock2RctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+len(expectedNonCanonicalBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlock2RctsMap, rct.LeafCID)
		require.Equal(t, expectedNonCanonicalBlock2RctsMap[rct.LeafCID], rct)
	}

	// check indexed rct IPLD blocks
	var data []byte
	var prefixedKey string

	rctRLPs := [][]byte{
		rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5,
		nonCanonicalBlockRctLeaf1, nonCanonicalBlockRctLeaf2,
	}
	for i, rctCid := range append(rctCids, nonCanonicalBlockRctCids...) {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, rctRLPs[i], data)
	}

	nonCanonicalBlock2RctRLPs := [][]byte{nonCanonicalBlock2RctLeaf1, nonCanonicalBlock2RctLeaf2}
	for i, rctCid := range nonCanonicalBlock2RctCids {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.Block2Number.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, nonCanonicalBlock2RctRLPs[i], data)
	}
}

func TestPublishAndIndexLogsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed logs
	pgStr := `SELECT address, log_data, topic0, topic1, topic2, topic3, data
		FROM eth.log_cids
		INNER JOIN public.blocks ON (log_cids.block_number = blocks.block_number AND log_cids.leaf_mh_key = blocks.key)
		WHERE log_cids.block_number = $1 AND header_id = $2 AND rct_id = $3
		ORDER BY log_cids.index ASC`

	type rctWithBlockHash struct {
		rct         *types.Receipt
		blockHash   string
		blockNumber uint64
	}
	mockRcts := make([]rctWithBlockHash, 0)

	// logs in the canonical block
	for _, mockBlockRct := range mocks.MockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockBlock.Hash().String(),
			mockBlock.NumberU64(),
		})
	}

	// logs in the non-canonical block at London height
	for _, mockBlockRct := range mocks.MockNonCanonicalBlockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockNonCanonicalBlock.Hash().String(),
			mockNonCanonicalBlock.NumberU64(),
		})
	}

	// logs in the non-canonical block at London height + 1
	for _, mockBlockRct := range mocks.MockNonCanonicalBlock2Receipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockNonCanonicalBlock2.Hash().String(),
			mockNonCanonicalBlock2.NumberU64(),
		})
	}

	for _, mockRct := range mockRcts {
		type logWithIPLD struct {
			models.LogsModel
			IPLDData []byte `db:"data"`
		}
		logRes := make([]logWithIPLD, 0)
		err = db.Select(context.Background(), &logRes, pgStr, mockRct.blockNumber, mockRct.blockHash, mockRct.rct.TxHash.String())
		require.NoError(t, err)
		require.Equal(t, len(mockRct.rct.Logs), len(logRes))

		for i, log := range mockRct.rct.Logs {
			topicSet := make([]string, 4)
			for ti, topic := range log.Topics {
				topicSet[ti] = topic.Hex()
			}

			expectedLog := models.LogsModel{
				Address: log.Address.String(),
				Data:    log.Data,
				Topic0:  topicSet[0],
				Topic1:  topicSet[1],
				Topic2:  topicSet[2],
				Topic3:  topicSet[3],
			}
			require.Equal(t, expectedLog, logRes[i].LogsModel)

			// check indexed log IPLD block
			var nodeElements []interface{}
			err = rlp.DecodeBytes(logRes[i].IPLDData, &nodeElements)
			require.NoError(t, err)

			if len(nodeElements) == 2 {
				logRaw, err := rlp.EncodeToBytes(log)
				require.NoError(t, err)
				// 2nd element of the leaf node contains the encoded log data.
				require.Equal(t, nodeElements[1].([]byte), logRaw)
			} else {
				logRaw, err := rlp.EncodeToBytes(log)
				require.NoError(t, err)
				// raw log was IPLDized
				require.Equal(t, logRes[i].IPLDData, logRaw)
			}
		}
	}
}

func TestPublishAndIndexStateNonCanonical(t *testing.T, db sql.Database) {
	// check indexed state nodes
	pgStr := `SELECT state_path, state_leaf_key, node_type, cid, mh_key, diff
					FROM eth.state_cids
					WHERE block_number = $1
					AND header_id = $2
					ORDER BY state_path`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStateCID)
	stateNodeCIDs := []cid.Cid{state1CID, state2CID, removedNodeCID, removedNodeCID}

	// expected state nodes in the canonical and the non-canonical block at London height
	expectedStateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs {
		expectedStateNodes = append(expectedStateNodes, models.StateNodeModel{
			Path:     stateDiff.Path,
			StateKey: common.BytesToHash(stateDiff.LeafKey).Hex(),
			NodeType: stateDiff.NodeType.Int(),
			CID:      stateNodeCIDs[i].String(),
			MhKey:    shared.MultihashKeyFromCID(stateNodeCIDs[i]),
			Diff:     true,
		})
	}
	sort.Slice(expectedStateNodes, func(i, j int) bool {
		if bytes.Compare(expectedStateNodes[i].Path, expectedStateNodes[j].Path) < 0 {
			return true
		} else {
			return false
		}
	})

	// expected state nodes in the non-canonical block at London height + 1
	expectedNonCanonicalBlock2StateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs[:2] {
		expectedNonCanonicalBlock2StateNodes = append(expectedNonCanonicalBlock2StateNodes, models.StateNodeModel{
			Path:     stateDiff.Path,
			StateKey: common.BytesToHash(stateDiff.LeafKey).Hex(),
			NodeType: stateDiff.NodeType.Int(),
			CID:      stateNodeCIDs[i].String(),
			MhKey:    shared.MultihashKeyFromCID(stateNodeCIDs[i]),
			Diff:     true,
		})
	}

	// check state nodes for canonical block
	stateNodes := make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64(), mockBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedStateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}

	// check state nodes for non-canonical block at London height
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedStateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}

	// check state nodes for non-canonical block at London height + 1
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.Block2Number.Uint64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedNonCanonicalBlock2StateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}
}

func TestPublishAndIndexStorageNonCanonical(t *testing.T, db sql.Database) {
	// check indexed storage nodes
	pgStr := `SELECT state_path, storage_path, storage_leaf_key, node_type, cid, mh_key, diff
					FROM eth.storage_cids
					WHERE block_number = $1
					AND header_id = $2
					ORDER BY state_path, storage_path`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStorageCID)
	storageNodeCIDs := []cid.Cid{storageCID, removedNodeCID, removedNodeCID, removedNodeCID}

	// expected storage nodes in the canonical and the non-canonical block at London height
	expectedStorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex := 0
	for _, stateDiff := range mocks.StateDiffs {
		for _, storageNode := range stateDiff.StorageNodes {
			expectedStorageNodes = append(expectedStorageNodes, models.StorageNodeModel{
				StatePath:  stateDiff.Path,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				NodeType:   storageNode.NodeType.Int(),
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				MhKey:      shared.MultihashKeyFromCID(storageNodeCIDs[storageNodeIndex]),
				Diff:       true,
			})
			storageNodeIndex++
		}
	}
	sort.Slice(expectedStorageNodes, func(i, j int) bool {
		if bytes.Compare(expectedStorageNodes[i].Path, expectedStorageNodes[j].Path) < 0 {
			return true
		} else {
			return false
		}
	})

	// expected storage nodes in the non-canonical block at London height + 1
	expectedNonCanonicalBlock2StorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex = 0
	for _, stateDiff := range mocks.StateDiffs[:2] {
		for _, storageNode := range stateDiff.StorageNodes {
			expectedNonCanonicalBlock2StorageNodes = append(expectedNonCanonicalBlock2StorageNodes, models.StorageNodeModel{
				StatePath:  stateDiff.Path,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				NodeType:   storageNode.NodeType.Int(),
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				MhKey:      shared.MultihashKeyFromCID(storageNodeCIDs[storageNodeIndex]),
				Diff:       true,
			})
			storageNodeIndex++
		}
	}

	// check storage nodes for canonical block
	storageNodes := make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64(), mockBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedStorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}

	// check storage nodes for non-canonical block at London height
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedStorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}

	// check storage nodes for non-canonical block at London height + 1
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mockNonCanonicalBlock2.NumberU64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedNonCanonicalBlock2StorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}
}
