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
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ipfs/go-cid"
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
	for _, node := range mocks.IPLDs {
		err = ind.PushIPLD(tx, node)
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
	var data []byte
	err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
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
		var data []byte
		err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
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
	rctsPgStr := `SELECT receipt_cids.cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
			WHERE receipt_cids.tx_id = transaction_cids.tx_hash
			AND transaction_cids.header_id = header_cids.block_hash
			AND header_cids.block_number = $1
			ORDER BY transaction_cids.index`
	logsPgStr := `SELECT log_cids.index, log_cids.address, blocks.data, log_cids.topic0, log_cids.topic1 FROM eth.log_cids
				INNER JOIN eth.receipt_cids ON (log_cids.rct_id = receipt_cids.tx_id)
				INNER JOIN ipld.blocks ON (log_cids.cid = blocks.key)
				WHERE receipt_cids.cid = $1 ORDER BY eth.log_cids.index ASC`
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

		for idx, r := range results {
			logRaw, err := rlp.EncodeToBytes(&expectedLogs[idx])
			require.NoError(t, err)
			require.Equal(t, r.Data, logRaw)
		}
	}
}

func TestPublishAndIndexReceiptIPLDs(t *testing.T, db sql.Database) {
	// check receipts were properly indexed and published
	rcts := make([]string, 0)
	pgStr := `SELECT receipt_cids.cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
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
					FROM ipld.blocks
					WHERE ipld.blocks.key = $1`
		err = db.Select(context.Background(), &result, pgStr, c)
		if err != nil {
			t.Fatal(err)
		}

		expectedRct, err := mocks.MockReceipts[idx].MarshalBinary()
		require.NoError(t, err)

		require.Equal(t, result[0].Data, expectedRct)

		dc, err := cid.Decode(c)
		if err != nil {
			t.Fatal(err)
		}
		var data []byte
		err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		postStatePgStr := `SELECT post_state FROM eth.receipt_cids WHERE cid = $1`
		switch c {
		case rct1CID.String():
			require.Equal(t, rct1, data)
			var postStatus uint64
			pgStr = `SELECT post_status FROM eth.receipt_cids WHERE cid = $1`
			err = db.Get(context.Background(), &postStatus, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostStatus, postStatus)
		case rct2CID.String():
			require.Equal(t, rct2, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState1, postState)
		case rct3CID.String():
			require.Equal(t, rct3, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState2, postState)
		case rct4CID.String():
			require.Equal(t, rct4, data)
			var postState string
			err = db.Get(context.Background(), &postState, postStatePgStr, c)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, mocks.ExpectedPostState3, postState)
		case rct5CID.String():
			require.Equal(t, rct5, data)
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
	pgStr := `SELECT state_cids.cid, CAST(state_cids.block_number as TEXT), state_cids.state_leaf_key, state_cids.removed,
				state_cids.header_id, CAST(state_cids.balance as TEXT), state_cids.nonce, state_cids.code_hash, state_cids.storage_root
				FROM eth.state_cids
				WHERE block_number = $1
				AND removed = false`
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
		err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		if stateNode.CID == state1CID.String() {
			require.Equal(t, false, stateNode.Removed)
			require.Equal(t, common.BytesToHash(mocks.ContractLeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, mocks.ContractLeafNode, data)
			require.Equal(t, mocks.BlockNumber.String(), stateNode.BlockNumber)
			require.Equal(t, "0", stateNode.Balance)
			require.Equal(t, mocks.ContractCodeHash.String(), stateNode.CodeHash)
			require.Equal(t, mocks.ContractRoot, stateNode.StorageRoot)
			require.Equal(t, uint64(1), stateNode.Nonce)
			require.Equal(t, mockBlock.Hash().String(), stateNode.HeaderID)
		}
		if stateNode.CID == state2CID.String() {
			require.Equal(t, false, stateNode.Removed)
			require.Equal(t, common.BytesToHash(mocks.AccountLeafKey).Hex(), stateNode.StateKey)
			require.Equal(t, mocks.AccountLeafNode, data)
			require.Equal(t, mocks.BlockNumber.String(), stateNode.BlockNumber)
			require.Equal(t, mocks.Balance.String(), stateNode.Balance)
			require.Equal(t, mocks.AccountCodeHash.String(), stateNode.CodeHash)
			require.Equal(t, mocks.AccountRoot, stateNode.StorageRoot)
			require.Equal(t, uint64(0), stateNode.Nonce)
			require.Equal(t, mockBlock.Hash().String(), stateNode.HeaderID)
		}
	}

	// check that Removed state nodes were properly indexed and published
	stateNodes = make([]models.StateNodeModel, 0)
	pgStr = `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.removed, state_cids.header_id,
				state_cids.nonce, CAST(state_cids.balance as TEXT), state_cids.code_hash, state_cids.storage_root
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND removed = true
				ORDER BY state_leaf_key`
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
		require.Equal(t, shared.RemovedNodeStateCID, dc.String())
		err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		if common.BytesToHash(mocks.RemovedLeafKey).Hex() == stateNode.StateKey {
			require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
			require.Equal(t, true, stateNode.Removed)
			require.Equal(t, []byte{}, data)
		} else if common.BytesToHash(mocks.Contract2LeafKey).Hex() == stateNode.StateKey {
			require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
			require.Equal(t, true, stateNode.Removed)
			require.Equal(t, []byte{}, data)
		} else {
			t.Fatalf("unexpected stateNode.StateKey value: %s", stateNode.StateKey)
		}
	}
}

/*
type StorageNodeModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StateKey    []byte `db:"state_leaf_key"`
	StorageKey  string `db:"storage_leaf_key"`
	Removed     bool   `db:"removed"`
	CID         string `db:"cid"`
	Diff        bool   `db:"diff"`
	Value       []byte `db:"val"`
}
*/

func TestPublishAndIndexStorageIPLDs(t *testing.T, db sql.Database) {
	// check that storage nodes were properly indexed
	storageNodes := make([]models.StorageNodeModel, 0)
	pgStr := `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.header_id, storage_cids.cid,
				storage_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.removed, storage_cids.val
				FROM eth.storage_cids
				WHERE storage_cids.block_number = $1
				AND storage_cids.removed = false
				ORDER BY storage_leaf_key`
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 1, len(storageNodes))
	require.Equal(t, models.StorageNodeModel{
		BlockNumber: mocks.BlockNumber.String(),
		HeaderID:    mockBlock.Header().Hash().Hex(),
		CID:         storageCID.String(),
		Removed:     false,
		StorageKey:  common.BytesToHash(mocks.StorageLeafKey).Hex(),
		StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
		Value:       mocks.StorageValue,
	}, storageNodes[0])
	var data []byte
	dc, err := cid.Decode(storageNodes[0].CID)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, mocks.StorageLeafNode, data)

	// check that Removed storage nodes were properly indexed
	storageNodes = make([]models.StorageNodeModel, 0)
	pgStr = `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.header_id, storage_cids.cid,
				storage_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.removed, storage_cids.val
				FROM eth.storage_cids
				WHERE storage_cids.block_number = $1
				AND storage_cids.removed = true
				ORDER BY storage_leaf_key`
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 3, len(storageNodes))
	expectedStorageNodes := []models.StorageNodeModel{ // TODO: ordering is non-deterministic
		{
			BlockNumber: mocks.BlockNumber.String(),
			HeaderID:    mockBlock.Header().Hash().Hex(),
			CID:         shared.RemovedNodeStorageCID,
			Removed:     true,
			StorageKey:  common.BytesToHash(mocks.Storage2LeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
			Value:       []byte{},
		},
		{
			BlockNumber: mocks.BlockNumber.String(),
			HeaderID:    mockBlock.Header().Hash().Hex(),
			CID:         shared.RemovedNodeStorageCID,
			Removed:     true,
			StorageKey:  common.BytesToHash(mocks.Storage3LeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
			Value:       []byte{},
		},
		{
			BlockNumber: mocks.BlockNumber.String(),
			HeaderID:    mockBlock.Header().Hash().Hex(),
			CID:         shared.RemovedNodeStorageCID,
			Removed:     true,
			StorageKey:  common.BytesToHash(mocks.RemovedLeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Value:       []byte{},
		},
	}
	for idx, storageNode := range storageNodes {
		require.Equal(t, expectedStorageNodes[idx], storageNode)
		dc, err = cid.Decode(storageNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, shared.RemovedNodeStorageCID, dc.String())
		err = db.Get(context.Background(), &data, ipfsPgGet, dc.String(), mocks.BlockNumber.Uint64())
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
			tx_root, receipt_root, uncles_hash, coinbase
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
			UnclesHash:      mockBlock.UncleHash().String(),
			Coinbase:        mocks.MockHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock.Number().String(),
			BlockHash:       mockNonCanonicalBlock.Hash().String(),
			CID:             mockNonCanonicalHeaderCID.String(),
			TotalDifficulty: mockNonCanonicalBlock.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock.ReceiptHash().String(),
			UnclesHash:      mockNonCanonicalBlock.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock2.Number().String(),
			BlockHash:       mockNonCanonicalBlock2.Hash().String(),
			CID:             mockNonCanonicalHeader2CID.String(),
			TotalDifficulty: mockNonCanonicalBlock2.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock2.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock2.ReceiptHash().String(),
			UnclesHash:      mockNonCanonicalBlock2.UncleHash().String(),
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
		err = db.Get(context.Background(), &data, ipfsPgGet, headerCIDs[i].String(), blockNumbers[i])
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, headerRLPs[i], data)
	}
}

func TestPublishAndIndexTransactionsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed transactions
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_hash, cid, dst, src, index,
		tx_type, CAST(value as TEXT)
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

	txCIDs := []cid.Cid{trx1CID, trx2CID, trx3CID, trx4CID, trx5CID}
	txRLPs := [][]byte{tx1, tx2, tx3, tx4, tx5}
	for i, txCID := range txCIDs {
		err = db.Get(context.Background(), &data, ipfsPgGet, txCID.String(), mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, txRLPs[i], data)
	}
}

func TestPublishAndIndexReceiptsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed receipts
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_id, cid, post_status, post_state, contract
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
		require.Contains(t, expectedBlockRctsMap, rct.CID)
		require.Equal(t, expectedBlockRctsMap[rct.CID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlockRctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlockRctsMap, rct.CID)
		require.Equal(t, expectedNonCanonicalBlockRctsMap[rct.CID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlock2RctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+len(expectedNonCanonicalBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlock2RctsMap, rct.CID)
		require.Equal(t, expectedNonCanonicalBlock2RctsMap[rct.CID], rct)
	}

	// check indexed rct IPLD blocks
	var data []byte

	rctRLPs := [][]byte{
		rct1, rct2, rct3, rct4, rct5, nonCanonicalBlockRct1, nonCanonicalBlockRct2,
	}
	for i, rctCid := range append(rctCids, nonCanonicalBlockRctCids...) {
		err = db.Get(context.Background(), &data, ipfsPgGet, rctCid.String(), mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, rctRLPs[i], data)
	}

	nonCanonicalBlock2RctRLPs := [][]byte{nonCanonicalBlock2Rct1, nonCanonicalBlock2Rct2}
	for i, rctCid := range nonCanonicalBlock2RctCids {
		err = db.Get(context.Background(), &data, ipfsPgGet, rctCid.String(), mocks.Block2Number.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, nonCanonicalBlock2RctRLPs[i], data)
	}
}

func TestPublishAndIndexLogsNonCanonical(t *testing.T, db sql.Database) {
	// check indexed logs
	pgStr := `SELECT address, topic0, topic1, topic2, topic3, data
		FROM eth.log_cids
		INNER JOIN ipld.blocks ON (log_cids.block_number = blocks.block_number AND log_cids.cid = blocks.key)
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
				Topic0:  topicSet[0],
				Topic1:  topicSet[1],
				Topic2:  topicSet[2],
				Topic3:  topicSet[3],
			}
			require.Equal(t, expectedLog, logRes[i].LogsModel)

			logRaw, err := rlp.EncodeToBytes(log)
			require.NoError(t, err)
			require.Equal(t, logRaw, logRes[i].IPLDData)
		}
	}
}

func TestPublishAndIndexStateNonCanonical(t *testing.T, db sql.Database) {
	// check indexed state nodes
	pgStr := `SELECT state_leaf_key, removed, cid, diff
					FROM eth.state_cids
					WHERE block_number = $1
					AND header_id = $2`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStateCID)
	stateNodeCIDs := []cid.Cid{state1CID, state2CID, removedNodeCID, removedNodeCID}

	// expected state nodes in the canonical and the non-canonical block at London height
	expectedStateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs {
		expectedStateNodes = append(expectedStateNodes, models.StateNodeModel{
			StateKey: common.BytesToHash(stateDiff.AccountWrapper.LeafKey).Hex(),
			Removed:  stateDiff.Removed,
			CID:      stateNodeCIDs[i].String(),
			Diff:     true,
		})
	}

	// expected state nodes in the non-canonical block at London height + 1
	expectedNonCanonicalBlock2StateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs[:2] {
		expectedNonCanonicalBlock2StateNodes = append(expectedNonCanonicalBlock2StateNodes, models.StateNodeModel{
			StateKey: common.BytesToHash(stateDiff.AccountWrapper.LeafKey).Hex(),
			Removed:  stateDiff.Removed,
			CID:      stateNodeCIDs[i].String(),
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
	assert.ElementsMatch(t, expectedStateNodes, stateNodes)

	// check state nodes for non-canonical block at London height
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStateNodes), len(stateNodes))
	assert.ElementsMatch(t, expectedStateNodes, stateNodes)

	// check state nodes for non-canonical block at London height + 1
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.Block2Number.Uint64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StateNodes), len(stateNodes))
	assert.ElementsMatch(t, expectedNonCanonicalBlock2StateNodes, stateNodes)
}

func TestPublishAndIndexStorageNonCanonical(t *testing.T, db sql.Database) {
	// check indexed storage nodes
	pgStr := `SELECT storage_leaf_key, state_leaf_key, removed, cid, diff, val
					FROM eth.storage_cids
					WHERE block_number = $1
					AND header_id = $2`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStorageCID)
	storageNodeCIDs := []cid.Cid{storageCID, removedNodeCID, removedNodeCID, removedNodeCID}

	// expected storage nodes in the canonical and the non-canonical block at London height
	expectedStorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex := 0
	for _, stateDiff := range mocks.StateDiffs {
		for _, storageNode := range stateDiff.StorageDiff {
			expectedStorageNodes = append(expectedStorageNodes, models.StorageNodeModel{
				StateKey:   common.BytesToHash(stateDiff.AccountWrapper.LeafKey).Hex(),
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				Removed:    storageNode.Removed,
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				Diff:       true,
				Value:      storageNode.Value,
			})
			storageNodeIndex++
		}
	}

	// expected storage nodes in the non-canonical block at London height + 1
	expectedNonCanonicalBlock2StorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex = 0
	for _, stateDiff := range mocks.StateDiffs[:2] {
		for _, storageNode := range stateDiff.StorageDiff {
			expectedNonCanonicalBlock2StorageNodes = append(expectedNonCanonicalBlock2StorageNodes, models.StorageNodeModel{
				StateKey:   common.BytesToHash(stateDiff.AccountWrapper.LeafKey).Hex(),
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				Removed:    storageNode.Removed,
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				Diff:       true,
				Value:      storageNode.Value,
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
	assert.ElementsMatch(t, expectedStorageNodes, storageNodes)

	// check storage nodes for non-canonical block at London height
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedStorageNodes), len(storageNodes))
	assert.ElementsMatch(t, expectedStorageNodes, storageNodes)

	// check storage nodes for non-canonical block at London height + 1
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mockNonCanonicalBlock2.NumberU64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StorageNodes), len(storageNodes))
	assert.ElementsMatch(t, expectedNonCanonicalBlock2StorageNodes, storageNodes)
}
