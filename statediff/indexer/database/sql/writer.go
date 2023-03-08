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

package sql

import (
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

var (
	nullHash = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
)

// Writer handles processing and writing of indexed IPLD objects to Postgres
type Writer struct {
	db Database
}

// NewWriter creates a new pointer to a Writer
func NewWriter(db Database) *Writer {
	return &Writer{
		db: db,
	}
}

// Close satisfies io.Closer
func (w *Writer) Close() error {
	return w.db.Close()
}

/*
INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (block_hash, block_number) DO UPDATE SET (block_number, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) = ($1, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, eth.header_cids.times_validated + 1, $16)
*/
func (w *Writer) upsertHeaderCID(tx Tx, header models.HeaderModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertHeaderStm(),
		header.BlockNumber, header.BlockHash, header.ParentHash, header.CID, header.TotalDifficulty, w.db.NodeID(),
		header.Reward, header.StateRoot, header.TxRoot, header.RctRoot, header.UncleRoot, header.Bloom,
		header.Timestamp, header.MhKey, 1, header.Coinbase)
	if err != nil {
		return insertError{"eth.header_cids", err, w.db.InsertHeaderStm(), header}
	}
	metrics.IndexerMetrics.BlocksCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.uncle_cids (block_number, block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (block_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertUncleCID(tx Tx, uncle models.UncleModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertUncleStm(),
		uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.MhKey)
	if err != nil {
		return insertError{"eth.uncle_cids", err, w.db.InsertUncleStm(), uncle}
	}
	return nil
}

/*
INSERT INTO eth.transaction_cids (block_number, header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type, value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tx_hash, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertTransactionCID(tx Tx, transaction models.TxModel) error {
	if w.useCopyForTx(tx) {
		var row []interface{}
		blockNum, err := strconv.ParseInt(transaction.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}

		value, err := strconv.ParseFloat(transaction.Value, 64)
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}

		row = append(row, blockNum, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src,
			transaction.Index, transaction.MhKey, transaction.Data, int(transaction.Type), value)

		var rows [][]interface{}
		rows = append(rows, row)

		_, err = tx.CopyFrom(w.db.Context(), w.db.TxTableName(), w.db.TxColumnNames(), rows)
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertTxStm(),
			transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src,
			transaction.Index, transaction.MhKey, transaction.Data, transaction.Type, transaction.Value)
		if err != nil {
			return insertError{"eth.transaction_cids", err, w.db.InsertTxStm(), transaction}
		}
	}
	metrics.IndexerMetrics.TransactionsCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.access_list_elements (block_number, tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tx_id, index, block_number) DO NOTHING
*/
func (w *Writer) upsertAccessListElement(tx Tx, accessListElement models.AccessListElementModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertAccessListElementStm(),
		accessListElement.BlockNumber, accessListElement.TxID, accessListElement.Index, accessListElement.Address,
		accessListElement.StorageKeys)
	if err != nil {
		return insertError{"eth.access_list_elements", err, w.db.InsertAccessListElementStm(), accessListElement}
	}
	metrics.IndexerMetrics.AccessListEntriesCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.receipt_cids (block_number, header_id, tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tx_id, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertReceiptCID(tx Tx, rct *models.ReceiptModel) error {
	if w.useCopyForTx(tx) {
		var row []interface{}
		blockNum, err := strconv.ParseInt(rct.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.receipt_cids", err, "COPY", rct}
		}

		row = append(row, blockNum, rct.HeaderID, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash,
			rct.LeafMhKey, rct.PostState, int(rct.PostStatus), rct.LogRoot)

		var rows [][]interface{}
		rows = append(rows, row)

		_, err = tx.CopyFrom(w.db.Context(), w.db.RctTableName(), w.db.RctColumnNames(), rows)
		if err != nil {
			return insertError{"eth.receipt_cids", err, "COPY", rct}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertRctStm(),
			rct.BlockNumber, rct.HeaderID, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState,
			rct.PostStatus, rct.LogRoot)
		if err != nil {
			return insertError{"eth.receipt_cids", err, w.db.InsertRctStm(), *rct}
		}
	}
	metrics.IndexerMetrics.ReceiptsCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.log_cids (block_number, header_id, leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (rct_id, index, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertLogCID(tx Tx, logs []*models.LogsModel) error {
	if w.useCopyForTx(tx) {
		var rows [][]interface{}
		for _, log := range logs {
			var row []interface{}
			blockNum, err := strconv.ParseInt(log.BlockNumber, 10, 64)
			if err != nil {
				return insertError{"eth.log_cids", err, "COPY", log}
			}

			row = append(row, blockNum, log.HeaderID, log.LeafCID, log.LeafMhKey, log.ReceiptID, log.Address,
				log.Index, log.Topic0, log.Topic1, log.Topic2, log.Topic3, log.Data)

			rows = append(rows, row)
		}
		if nil != rows && len(rows) >= 0 {
			_, err := tx.CopyFrom(w.db.Context(), w.db.LogTableName(), w.db.LogColumnNames(), rows)
			if err != nil {
				return insertError{"eth.log_cids", err, "COPY", rows}
			}
			metrics.IndexerMetrics.LogsCounter.Inc(int64(len(rows)))
		}
	} else {
		for _, log := range logs {
			_, err := tx.Exec(w.db.Context(), w.db.InsertLogStm(),
				log.BlockNumber, log.HeaderID, log.LeafCID, log.LeafMhKey, log.ReceiptID, log.Address, log.Index, log.Topic0, log.Topic1,
				log.Topic2, log.Topic3, log.Data)
			if err != nil {
				return insertError{"eth.log_cids", err, w.db.InsertLogStm(), *log}
			}
			metrics.IndexerMetrics.LogsCounter.Inc(1)
		}
	}
	return nil
}

/*
INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (header_id, state_path, block_number) DO UPDATE SET (block_number, state_leaf_key, cid, node_type, diff, mh_key) = ($1 $3, $4, $6, $7, $8)
*/
func (w *Writer) upsertStateCID(tx Tx, stateNode models.StateNodeModel) error {
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	if w.useCopyForTx(tx) {
		var row []interface{}
		blockNum, err := strconv.ParseInt(stateNode.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.state_cids", err, "COPY", stateNode}
		}

		row = append(row, blockNum, stateNode.HeaderID, stateKey, stateNode.CID,
			stateNode.Path, stateNode.NodeType, true, stateNode.MhKey)

		var rows [][]interface{}
		rows = append(rows, row)

		_, err = tx.CopyFrom(w.db.Context(), w.db.StateTableName(), w.db.StateColumnNames(), rows)
		if err != nil {
			return insertError{"eth.state_cids", err, "COPY", stateNode}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertStateStm(),
			stateNode.BlockNumber, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true,
			stateNode.MhKey)
		if err != nil {
			return insertError{"eth.state_cids", err, w.db.InsertStateStm(), stateNode}
		}
	}
	return nil
}

/*
INSERT INTO eth.state_accounts (block_number, header_id, state_path, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (header_id, state_path, block_number) DO NOTHING
*/
func (w *Writer) upsertStateAccount(tx Tx, stateAccount models.StateAccountModel) error {
	if w.useCopyForTx(tx) {
		var row []interface{}
		blockNum, err := strconv.ParseInt(stateAccount.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.state_accounts", err, "COPY", stateAccount}
		}
		balance, err := strconv.ParseFloat(stateAccount.Balance, 64)
		if err != nil {
			return insertError{"eth.state_accounts", err, "COPY", stateAccount}
		}

		row = append(row, blockNum, stateAccount.HeaderID, stateAccount.StatePath, balance,
			stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)

		var rows [][]interface{}
		rows = append(rows, row)

		_, err = tx.CopyFrom(w.db.Context(), w.db.AccountTableName(), w.db.AccountColumnNames(), rows)
		if err != nil {
			return insertError{"eth.state_accounts", err, "COPY", stateAccount}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertAccountStm(),
			stateAccount.BlockNumber, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
			stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)
		if err != nil {
			return insertError{"eth.state_accounts", err, w.db.InsertAccountStm(), stateAccount}
		}
	}
	return nil
}

/*
INSERT INTO eth.storage_cids (block_number, header_id, state_path, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (header_id, state_path, storage_path, block_number) DO UPDATE SET (block_number, storage_leaf_key, cid, node_type, diff, mh_key) = ($1, $4, $5, $7, $8, $9)
*/
func (w *Writer) upsertStorageCID(tx Tx, storageCID models.StorageNodeModel) error {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	if w.useCopyForTx(tx) {
		var row []interface{}
		blockNum, err := strconv.ParseInt(storageCID.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.storage_cids", err, "COPY", storageCID}
		}

		row = append(row, blockNum, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID,
			storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)

		var rows [][]interface{}
		rows = append(rows, row)

		_, err = tx.CopyFrom(w.db.Context(), w.db.StorageTableName(), w.db.StorageColumnNames(), rows)
		if err != nil {
			return insertError{"eth.storage_cids", err, "COPY", storageCID}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertStorageStm(),
			storageCID.BlockNumber, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID, storageCID.Path,
			storageCID.NodeType, true, storageCID.MhKey)
		if err != nil {
			return insertError{"eth.storage_cids", err, w.db.InsertStorageStm(), storageCID}
		}
	}
	return nil
}

func (w *Writer) useCopyForTx(tx Tx) bool {
	// Using COPY instead of INSERT only makes much sense if also using a DelayedTx, so that operations
	// can be collected over time and then all submitted within in a single TX.
	if _, ok := tx.(*DelayedTx); ok {
		return w.db.UseCopyFrom()
	}
	return false
}

type insertError struct {
	table     string
	err       error
	stmt      string
	arguments interface{}
}

var _ error = insertError{}

func (dbe insertError) Error() string {
	return fmt.Sprintf("error inserting %s entry: %v\r\nstatement: %s\r\narguments: %+v",
		dbe.table, dbe.err, dbe.stmt, dbe.arguments)
}
