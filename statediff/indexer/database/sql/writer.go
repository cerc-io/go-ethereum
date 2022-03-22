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
	"context"
	"fmt"

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
		return fmt.Errorf("error upserting header_cids entry: %v", err)
	}
	indexerMetrics.blocks.Inc(1)
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
		return fmt.Errorf("error upserting uncle_cids entry: %v", err)
	}
	return nil
}

/*
INSERT INTO eth.transaction_cids (block_number, header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type, value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tx_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertTransactionCID(tx Tx, transaction models.TxModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertTxStm(),
		transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src,
		transaction.Index, transaction.MhKey, transaction.Data, transaction.Type, transaction.Value)
	if err != nil {
		return fmt.Errorf("error upserting transaction_cids entry: %v", err)
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
		return fmt.Errorf("error upserting access_list_element entry: %v", err)
	}
	indexerMetrics.accessListEntries.Inc(1)
	return nil
}

/*
INSERT INTO eth.receipt_cids (block_number, tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tx_id, block_number) DO NOTHING
*/
func (w *Writer) upsertReceiptCID(tx Tx, rct *models.ReceiptModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertRctStm(),
		rct.BlockNumber, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState,
		rct.PostStatus, rct.LogRoot)
	if err != nil {
		return fmt.Errorf("error upserting receipt_cids entry: %w", err)
	}
	indexerMetrics.receipts.Inc(1)
	return nil
}

/*
INSERT INTO eth.log_cids (block_number, leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (rct_id, index, block_number) DO NOTHING
*/
func (w *Writer) upsertLogCID(tx Tx, logs []*models.LogsModel) error {
	for _, log := range logs {
		_, err := tx.Exec(w.db.Context(), w.db.InsertLogStm(),
			log.BlockNumber, log.LeafCID, log.LeafMhKey, log.ReceiptID, log.Address, log.Index, log.Topic0, log.Topic1,
			log.Topic2, log.Topic3, log.Data)
		if err != nil {
			return fmt.Errorf("error upserting logs entry: %w", err)
		}
		indexerMetrics.logs.Inc(1)
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
	_, err := tx.Exec(w.db.Context(), w.db.InsertStateStm(),
		stateNode.BlockNumber, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true,
		stateNode.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting state_cids entry: %v", err)
	}
	return nil
}

/*
INSERT INTO eth.state_accounts (block_number, header_id, state_path, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (header_id, state_path, block_number) DO NOTHING
*/
func (w *Writer) upsertStateAccount(tx Tx, stateAccount models.StateAccountModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertAccountStm(),
		stateAccount.BlockNumber, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
		stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)
	if err != nil {
		return fmt.Errorf("error upserting state_accounts entry: %v", err)
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
	_, err := tx.Exec(w.db.Context(), w.db.InsertStorageStm(),
		storageCID.BlockNumber, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID, storageCID.Path,
		storageCID.NodeType, true, storageCID.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting storage_cids entry: %v", err)
	}
	return nil
}

// Upserts known gaps to the DB.
// INSERT INTO eth.known_gaps (starting_block_number, ending_block_number, checked_out, processing_key) VALUES ($1, $2, $3, $4)
func (w *Writer) upsertKnownGaps(knownGaps models.KnownGapsModel) error {
	_, err := w.db.Exec(context.Background(), w.db.InsertKnownGapsStm(),
		knownGaps.StartingBlockNumber, knownGaps.EndingBlockNumber, knownGaps.CheckedOut, knownGaps.ProcessingKey)
	if err != nil {
		return fmt.Errorf("error upserting known_gaps entry: %v", err)
	}

	return nil
}
