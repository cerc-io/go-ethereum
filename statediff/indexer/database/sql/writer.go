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

	"github.com/jackc/pgtype"
	shopspring "github.com/jackc/pgtype/ext/shopspring-numeric"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
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
INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_ids, reward, state_root, tx_root, receipt_root, uncles_hash, bloom, timestamp, coinbase)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (block_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertHeaderCID(tx Tx, header models.HeaderModel) error {
	nodeIDs := pq.StringArray([]string{w.db.NodeID()})
	_, err := tx.Exec(w.db.Context(), w.db.InsertHeaderStm(),
		header.BlockNumber,
		header.BlockHash,
		header.ParentHash,
		header.CID,
		header.TotalDifficulty,
		nodeIDs,
		header.Reward,
		header.StateRoot,
		header.TxRoot,
		header.RctRoot,
		header.UnclesHash,
		header.Bloom,
		header.Timestamp,
		header.Coinbase)
	if err != nil {
		return insertError{"eth.header_cids", err, w.db.InsertHeaderStm(), header}
	}
	metrics.IndexerMetrics.BlocksCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.uncle_cids (block_number, block_hash, header_id, parent_hash, cid, reward, index) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (block_hash, block_number) DO NOTHING
*/
func (w *Writer) upsertUncleCID(tx Tx, uncle models.UncleModel) error {
	_, err := tx.Exec(w.db.Context(), w.db.InsertUncleStm(),
		uncle.BlockNumber,
		uncle.BlockHash,
		uncle.HeaderID,
		uncle.ParentHash,
		uncle.CID,
		uncle.Reward,
		uncle.Index)
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
	val := transaction.Value
	if val == "" {
		val = "0"
	}
	if w.useCopyForTx(tx) {
		blockNum, err := strconv.ParseInt(transaction.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}

		value, err := toNumeric(val)
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}

		_, err = tx.CopyFrom(w.db.Context(), w.db.TxTableName(), w.db.TxColumnNames(),
			toRows(toRow(blockNum, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst,
				transaction.Src, transaction.Index, int(transaction.Type), value)))
		if err != nil {
			return insertError{"eth.transaction_cids", err, "COPY", transaction}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertTxStm(),
			transaction.BlockNumber,
			transaction.HeaderID,
			transaction.TxHash,
			transaction.CID,
			transaction.Dst,
			transaction.Src,
			transaction.Index,
			transaction.Type,
			transaction.Value)
		if err != nil {
			return insertError{"eth.transaction_cids", err, w.db.InsertTxStm(), transaction}
		}
	}
	metrics.IndexerMetrics.TransactionsCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.receipt_cids (block_number, header_id, tx_id, cid, contract, post_state, post_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tx_id, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertReceiptCID(tx Tx, rct *models.ReceiptModel) error {
	if w.useCopyForTx(tx) {
		blockNum, err := strconv.ParseUint(rct.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.receipt_cids", err, "COPY", rct}
		}

		_, err = tx.CopyFrom(w.db.Context(), w.db.RctTableName(), w.db.RctColumnNames(),
			toRows(toRow(blockNum, rct.HeaderID, rct.TxID, rct.CID, rct.Contract,
				rct.PostState, int(rct.PostStatus))))
		if err != nil {
			return insertError{"eth.receipt_cids", err, "COPY", rct}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertRctStm(),
			rct.BlockNumber,
			rct.HeaderID,
			rct.TxID,
			rct.CID,
			rct.Contract,
			rct.PostState,
			rct.PostStatus)
		if err != nil {
			return insertError{"eth.receipt_cids", err, w.db.InsertRctStm(), *rct}
		}
	}
	metrics.IndexerMetrics.ReceiptsCounter.Inc(1)
	return nil
}

/*
INSERT INTO eth.log_cids (block_number, header_id, cid, rct_id, address, index, topic0, topic1, topic2, topic3) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (rct_id, index, header_id, block_number) DO NOTHING
*/
func (w *Writer) upsertLogCID(tx Tx, logs []*models.LogsModel) error {
	if w.useCopyForTx(tx) {
		var rows [][]interface{}
		for _, log := range logs {
			blockNum, err := strconv.ParseUint(log.BlockNumber, 10, 64)
			if err != nil {
				return insertError{"eth.log_cids", err, "COPY", log}
			}

			rows = append(rows, toRow(blockNum, log.HeaderID, log.CID, log.ReceiptID,
				log.Address, log.Index, log.Topic0, log.Topic1, log.Topic2, log.Topic3))
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
				log.BlockNumber,
				log.HeaderID,
				log.CID,
				log.ReceiptID,
				log.Address,
				log.Index,
				log.Topic0,
				log.Topic1,
				log.Topic2,
				log.Topic3)
			if err != nil {
				return insertError{"eth.log_cids", err, w.db.InsertLogStm(), *log}
			}
			metrics.IndexerMetrics.LogsCounter.Inc(1)
		}
	}
	return nil
}

/*
INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, removed, diff, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (header_id, state_leaf_key, block_number) DO NOTHING
*/
func (w *Writer) upsertStateCID(tx Tx, stateNode models.StateNodeModel) error {
	bal := stateNode.Balance
	if stateNode.Removed {
		bal = "0"
	}

	if w.useCopyForTx(tx) {
		blockNum, err := strconv.ParseUint(stateNode.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.state_cids", err, "COPY", stateNode}
		}

		balance, err := toNumeric(bal)
		if err != nil {
			return insertError{"eth.state_cids", err, "COPY", stateNode}
		}

		_, err = tx.CopyFrom(w.db.Context(), w.db.StateTableName(), w.db.StateColumnNames(),
			toRows(toRow(blockNum, stateNode.HeaderID, stateNode.StateKey, stateNode.CID,
				true, balance, stateNode.Nonce, stateNode.CodeHash, stateNode.StorageRoot, stateNode.Removed)))
		if err != nil {
			return insertError{"eth.state_cids", err, "COPY", stateNode}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertStateStm(),
			stateNode.BlockNumber,
			stateNode.HeaderID,
			stateNode.StateKey,
			stateNode.CID,
			true,
			bal,
			stateNode.Nonce,
			stateNode.CodeHash,
			stateNode.StorageRoot,
			stateNode.Removed,
		)
		if err != nil {
			return insertError{"eth.state_cids", err, w.db.InsertStateStm(), stateNode}
		}
	}
	return nil
}

/*
INSERT INTO eth.storage_cids (block_number, header_id, state_leaf_key, storage_leaf_key, cid, removed, diff, val) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (header_id, state_leaf_key, storage_leaf_key, block_number) DO NOTHING
*/
func (w *Writer) upsertStorageCID(tx Tx, storageCID models.StorageNodeModel) error {
	if w.useCopyForTx(tx) {
		blockNum, err := strconv.ParseUint(storageCID.BlockNumber, 10, 64)
		if err != nil {
			return insertError{"eth.storage_cids", err, "COPY", storageCID}
		}

		_, err = tx.CopyFrom(w.db.Context(), w.db.StorageTableName(), w.db.StorageColumnNames(),
			toRows(toRow(blockNum, storageCID.HeaderID, storageCID.StateKey, storageCID.StorageKey, storageCID.CID,
				true, storageCID.Value, storageCID.Removed)))
		if err != nil {
			return insertError{"eth.storage_cids", err, "COPY", storageCID}
		}
	} else {
		_, err := tx.Exec(w.db.Context(), w.db.InsertStorageStm(),
			storageCID.BlockNumber,
			storageCID.HeaderID,
			storageCID.StateKey,
			storageCID.StorageKey,
			storageCID.CID,
			true,
			storageCID.Value,
			storageCID.Removed,
		)
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

// combine args into a row
func toRow(args ...interface{}) []interface{} {
	var row []interface{}
	row = append(row, args...)
	return row
}

func toNumeric(value string) (*shopspring.Numeric, error) {
	decimalValue, err := decimal.NewFromString(value)
	if nil != err {
		return nil, err
	}

	return &shopspring.Numeric{Decimal: decimalValue, Status: pgtype.Present}, nil
}

// combine row (or rows) into a slice of rows for CopyFrom
func toRows(rows ...[]interface{}) [][]interface{} {
	return rows
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
