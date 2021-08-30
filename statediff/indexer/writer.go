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

package indexer

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
)

var (
	nullHash = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
)

// Handles processing and writing of indexed IPLD objects to Postgres
type PostgresCIDWriter struct {
	db *postgres.DB
}

// NewPostgresCIDWriter creates a new pointer to a Indexer which satisfies the PostgresCIDWriter interface
func NewPostgresCIDWriter(db *postgres.DB) *PostgresCIDWriter {
	return &PostgresCIDWriter{
		db: db,
	}
}

func (in *PostgresCIDWriter) upsertHeaderCID(tx *sqlx.Tx, header models.HeaderModel) (int64, error) {
	var headerID int64
	err := tx.QueryRowx(`INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
								ON CONFLICT (block_number, block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, eth.header_cids.times_validated + 1, $16)
								RETURNING id`,
		header.BlockNumber, header.BlockHash, header.ParentHash, header.CID, header.TotalDifficulty, in.db.NodeID, header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, 1, header.BaseFee).Scan(&headerID)
	if err != nil {
		return 0, fmt.Errorf("error upserting header_cids entry: %v", err)
	}
	indexerMetrics.blocks.Inc(1)
	return headerID, nil
}

func (in *PostgresCIDWriter) upsertUncleCID(tx *sqlx.Tx, uncle models.UncleModel, headerID int64) error {
	_, err := tx.Exec(`INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES ($1, $2, $3, $4, $5, $6)
								ON CONFLICT (header_id, block_hash) DO UPDATE SET (parent_hash, cid, reward, mh_key) = ($3, $4, $5, $6)`,
		uncle.BlockHash, headerID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting uncle_cids entry: %v", err)
	}
	return nil
}

func (in *PostgresCIDWriter) upsertTransactionCID(tx *sqlx.Tx, transaction models.TxModel, headerID int64) (int64, error) {
	var txID int64
	err := tx.QueryRowx(`INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
									ON CONFLICT (header_id, tx_hash) DO UPDATE SET (cid, dst, src, index, mh_key, tx_data, tx_type) = ($3, $4, $5, $6, $7, $8, $9)
									RETURNING id`,
		headerID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type).Scan(&txID)
	if err != nil {
		return 0, fmt.Errorf("error upserting transaction_cids entry: %v", err)
	}
	indexerMetrics.transactions.Inc(1)
	return txID, nil
}

func (in *PostgresCIDWriter) upsertAccessListElement(tx *sqlx.Tx, accessListElement models.AccessListElementModel, txID int64) error {
	_, err := tx.Exec(`INSERT INTO eth.access_list_element (tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4)
								ON CONFLICT (tx_id, index) DO UPDATE SET (address, storage_keys) = ($3, $4)`,
		txID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	if err != nil {
		return fmt.Errorf("error upserting access_list_element entry: %v", err)
	}
	indexerMetrics.accessListEntries.Inc(1)
	return nil
}

func (in *PostgresCIDWriter) upsertReceiptCID(tx *sqlx.Tx, rct *models.ReceiptModel, txID int64) (int64, error) {
	var receiptID int64
	err := tx.QueryRowx(`INSERT INTO eth.receipt_cids (tx_id, cid, contract, contract_hash, mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
 							  ON CONFLICT (tx_id) DO UPDATE SET (cid, contract, contract_hash, mh_key, post_state, post_status, log_root) = ($2, $3, $4, $5, $6, $7, $8)
 							  RETURNING id`,
		txID, rct.CID, rct.Contract, rct.ContractHash, rct.MhKey, rct.PostState, rct.PostStatus, rct.LogRoot).Scan(&receiptID)
	if err != nil {
		return 0, fmt.Errorf("error upserting receipt_cids entry: %w", err)
	}
	indexerMetrics.receipts.Inc(1)
	return receiptID, nil
}

func (in *PostgresCIDWriter) upsertLogCID(tx *sqlx.Tx, logs []*models.LogsModel, receiptID int64) error {
	for _, log := range logs {
		_, err := tx.Exec(`INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, receipt_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
					ON CONFLICT (receipt_id, index) DO UPDATE SET (leaf_cid, leaf_mh_key ,address, topic0, topic1, topic2, topic3,log_data ) = ($1, $2, $4, $6, $7, $8, $9, $10)`,
			log.LeafCID, log.LeafMhKey, receiptID, log.Address, log.Index, log.Topic0, log.Topic1, log.Topic2, log.Topic3, log.Data)
		if err != nil {
			return fmt.Errorf("error upserting logs entry: %w", err)
		}
	}
	// TODO: Add metrics for logs.
	return nil
}

func (in *PostgresCIDWriter) upsertStateCID(tx *sqlx.Tx, stateNode models.StateNodeModel, headerID int64) (int64, error) {
	var stateID int64
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	err := tx.QueryRowx(`INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
									ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)
									RETURNING id`,
		headerID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true, stateNode.MhKey).Scan(&stateID)
	if err != nil {
		return 0, fmt.Errorf("error upserting state_cids entry: %v", err)
	}
	return stateID, nil
}

func (in *PostgresCIDWriter) upsertStateAccount(tx *sqlx.Tx, stateAccount models.StateAccountModel, stateID int64) error {
	_, err := tx.Exec(`INSERT INTO eth.state_accounts (state_id, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5)
							  ON CONFLICT (state_id) DO UPDATE SET (balance, nonce, code_hash, storage_root) = ($2, $3, $4, $5)`,
		stateID, stateAccount.Balance, stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)
	if err != nil {
		return fmt.Errorf("error upserting state_accounts entry: %v", err)
	}
	return nil
}

func (in *PostgresCIDWriter) upsertStorageCID(tx *sqlx.Tx, storageCID models.StorageNodeModel, stateID int64) error {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	_, err := tx.Exec(`INSERT INTO eth.storage_cids (state_id, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
							  ON CONFLICT (state_id, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)`,
		stateID, storageKey, storageCID.CID, storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting storage_cids entry: %v", err)
	}
	return nil
}
