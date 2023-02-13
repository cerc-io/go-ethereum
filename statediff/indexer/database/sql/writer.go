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

	"github.com/ethereum/go-ethereum/statediff/indexer/models"
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
INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncles_hash, bloom, timestamp, mh_key, times_validated, coinbase)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (block_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertHeaderCID(tx Tx, header models.HeaderModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertHeaderStm(),
		header.BlockNumber, header.BlockHash, header.ParentHash, header.CID, header.TotalDifficulty, w.db.NodeID(),
		header.Reward, header.StateRoot, header.TxRoot, header.RctRoot, header.UnclesHash, header.Bloom,
		header.Timestamp, header.Coinbase)
	if err != nil {
		return insertError{"eth.header_cids", err, w.db.InsertHeaderStm(), header}
	}
	indexerMetrics.blocks.Inc(1)
	return nil
}

/*
INSERT INTO eth.uncle_cids (block_number, block_hash, header_id, parent_hash, cid, reward, index) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (block_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertUncleCID(tx Tx, uncle models.UncleModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertUncleStm(),
		uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.Index)
	if err != nil {
		return insertError{"eth.uncle_cids", err, w.db.InsertUncleStm(), uncle}
	}
	return nil
}

/*
INSERT INTO eth.transaction_cids (block_number, header_id, tx_hash, cid, dst, src, index, tx_type, value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tx_hash, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertTransactionCID(tx Tx, transaction models.TxModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertTxStm(),
		transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src,
		transaction.Index, transaction.Type, transaction.Value)
	if err != nil {
		return insertError{"eth.transaction_cids", err, w.db.InsertTxStm(), transaction}
	}
	indexerMetrics.transactions.Inc(1)
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
	indexerMetrics.accessListEntries.Inc(1)
	return nil
}

/*
INSERT INTO eth.receipt_cids (block_number, header_id, tx_id, cid, contract, contract_hash, post_state, post_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tx_id, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertReceiptCID(tx Tx, rct *models.ReceiptModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertRctStm(),
		rct.BlockNumber, rct.HeaderID, rct.TxID, rct.CID, rct.Contract, rct.ContractHash, rct.PostState,
		rct.PostStatus)
	if err != nil {
		return insertError{"eth.receipt_cids", err, w.db.InsertRctStm(), *rct}
	}
	indexerMetrics.receipts.Inc(1)
	return nil
}

/*
INSERT INTO eth.log_cids (block_number, header_id, cid, rct_id, address, index, topic0, topic1, topic2, topic3) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (rct_id, index, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertLogCID(tx Tx, logs []*models.LogsModel) error {
	for _, log := range logs {
		_, err := tx.Exec(w.db.Context(), w.db.InsertLogStm(),
			log.BlockNumber, log.HeaderID, log.CID, log.ReceiptID, log.Address, log.Index, log.Topic0, log.Topic1,
			log.Topic2, log.Topic3)
		if err != nil {
			return insertError{"eth.log_cids", err, w.db.InsertLogStm(), *log}
		}
		indexerMetrics.logs.Inc(1)
	}
	return nil
}

/*
INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, partial_path, removed, diff, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (header_id, state_leaf_key, block_number) DO NOTHING
*/
func (w *Writer) upsertStateCID(tx Tx, stateNode models.StateNodeModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertStateStm(),
		stateNode.BlockNumber, stateNode.HeaderID, stateNode.StateKey, stateNode.CID, stateNode.Path, stateNode.Removed, true,
		stateNode.Balance, stateNode.Nonce, stateNode.CodeHash, stateNode.StorageRoot)
	if err != nil {
		return insertError{"eth.state_cids", err, w.db.InsertStateStm(), stateNode}
	}
	return nil
}

/*
INSERT INTO eth.storage_cids (block_number, header_id, state_leaf_key, storage_leaf_key, cid, storage_path, removed, diff, val) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (header_id, state_leaf_key, storage_leaf_key, block_number) DO NOTHING
*/
func (w *Writer) upsertStorageCID(tx Tx, storageCID models.StorageNodeModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertStorageStm(),
		storageCID.BlockNumber, storageCID.HeaderID, storageCID.StateKey, storageCID.StorageKey, storageCID.CID, storageCID.Path,
		storageCID.Removed, true, storageCID.Value)
	if err != nil {
		return insertError{"eth.storage_cids", err, w.db.InsertStorageStm(), storageCID}
	}
	return nil
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
