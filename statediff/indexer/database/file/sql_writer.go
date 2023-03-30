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

package file

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"

	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/thoas/go-funk"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/types"
)

var (
	pipeSize        = 65336 // min(linuxPipeSize, macOSPipeSize)
	writeBufferSize = pipeSize * 16 * 96
)

// SQLWriter writes sql statements to a file
type SQLWriter struct {
	wc             io.WriteCloser
	stmts          chan []byte
	collatedStmt   []byte
	collationIndex int

	flushChan     chan struct{}
	flushFinished chan struct{}
	quitChan      chan struct{}
	doneChan      chan struct{}

	watchedAddressesFilePath string
}

// NewSQLWriter creates a new pointer to a Writer
func NewSQLWriter(wc io.WriteCloser, watchedAddressesFilePath string) *SQLWriter {
	return &SQLWriter{
		wc:                       wc,
		stmts:                    make(chan []byte),
		collatedStmt:             make([]byte, writeBufferSize),
		flushChan:                make(chan struct{}),
		flushFinished:            make(chan struct{}),
		quitChan:                 make(chan struct{}),
		doneChan:                 make(chan struct{}),
		watchedAddressesFilePath: watchedAddressesFilePath,
	}
}

// Loop enables concurrent writes to the underlying os.File
// since os.File does not buffer, it utilizes an internal buffer that is the size of a unix pipe
// by using copy() and tracking the index/size of the buffer, we require only the initial memory allocation
func (sqw *SQLWriter) Loop() {
	sqw.collationIndex = 0
	go func() {
		defer close(sqw.doneChan)
		var l int
		for {
			select {
			case stmt := <-sqw.stmts:
				l = len(stmt)
				if sqw.collationIndex+l > writeBufferSize {
					if err := sqw.flush(); err != nil {
						panic(fmt.Sprintf("error writing sql stmts buffer to file: %v", err))
					}
					if l > writeBufferSize {
						if _, err := sqw.wc.Write(stmt); err != nil {
							panic(fmt.Sprintf("error writing large sql stmt to file: %v", err))
						}
						continue
					}
				}
				copy(sqw.collatedStmt[sqw.collationIndex:sqw.collationIndex+l], stmt)
				sqw.collationIndex += l
			case <-sqw.quitChan:
				if err := sqw.flush(); err != nil {
					panic(fmt.Sprintf("error writing sql stmts buffer to file: %v", err))
				}
				return
			case <-sqw.flushChan:
				if err := sqw.flush(); err != nil {
					panic(fmt.Sprintf("error writing sql stmts buffer to file: %v", err))
				}
				sqw.flushFinished <- struct{}{}
			}
		}
	}()
}

// Close satisfies io.Closer
func (sqw *SQLWriter) Close() error {
	close(sqw.quitChan)
	<-sqw.doneChan
	close(sqw.stmts)
	close(sqw.flushChan)
	close(sqw.flushFinished)
	return sqw.wc.Close()
}

// Flush sends a flush signal to the looping process
func (sqw *SQLWriter) Flush() {
	sqw.flushChan <- struct{}{}
	<-sqw.flushFinished
}

func (sqw *SQLWriter) flush() error {
	if _, err := sqw.wc.Write(sqw.collatedStmt[0:sqw.collationIndex]); err != nil {
		return err
	}
	sqw.collationIndex = 0
	return nil
}

const (
	nodeInsert = "INSERT INTO nodes (genesis_block, network_id, node_id, client_name, chain_id) VALUES " +
		"('%s', '%s', '%s', '%s', %d);\n"

	ipldInsert = "INSERT INTO ipld.blocks (block_number, key, data) VALUES ('%s', '%s', '\\x%x');\n"

	headerInsert = "INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_ids, reward, " +
		"state_root, tx_root, receipt_root, uncles_hash, bloom, timestamp, coinbase) VALUES " +
		"('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '\\x%x', %d, '%s');\n"

	uncleInsert = "INSERT INTO eth.uncle_cids (block_number, block_hash, header_id, parent_hash, cid, reward, index) VALUES " +
		"('%s', '%s', '%s', '%s', '%s', '%s', %d);\n"

	txInsert = "INSERT INTO eth.transaction_cids (block_number, header_id, tx_hash, cid, dst, src, index, tx_type, " +
		"value) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s');\n"

	rctInsert = "INSERT INTO eth.receipt_cids (block_number, header_id, tx_id, cid, contract, post_state, " +
		"post_status) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d);\n"

	logInsert = "INSERT INTO eth.log_cids (block_number, header_id, cid, rct_id, address, index, topic0, topic1, topic2, " +
		"topic3) VALUES ('%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s');\n"

	stateInsert = "INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, removed, diff, " +
		"balance, nonce, code_hash, storage_root) VALUES ('%s', '%s', '%s', '%s', %t, %t, '%s', %d, '%s', '%s');\n"

	storageInsert = "INSERT INTO eth.storage_cids (block_number, header_id, state_leaf_key, storage_leaf_key, cid, " +
		"removed, diff, val) VALUES ('%s', '%s', '%s', '%s', '%s', %t, %t, '\\x%x');\n"
)

func (sqw *SQLWriter) upsertNode(node nodeinfo.Info) {
	sqw.stmts <- []byte(fmt.Sprintf(nodeInsert, node.GenesisBlock, node.NetworkID, node.ID, node.ClientName, node.ChainID))
}

func (sqw *SQLWriter) upsertIPLD(ipld models.IPLDModel) {
	sqw.stmts <- []byte(fmt.Sprintf(ipldInsert, ipld.BlockNumber, ipld.Key, ipld.Data))
}

func (sqw *SQLWriter) upsertIPLDDirect(blockNumber, key string, value []byte) {
	sqw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         key,
		Data:        value,
	})
}

func (sqw *SQLWriter) upsertIPLDNode(blockNumber string, i ipld.IPLD) {
	sqw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         i.Cid().String(),
		Data:        i.RawData(),
	})
}

func (sqw *SQLWriter) upsertHeaderCID(header models.HeaderModel) {
	stmt := fmt.Sprintf(headerInsert, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
		header.TotalDifficulty, formatPostgresStringArray(header.NodeIDs), header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UnclesHash, header.Bloom, header.Timestamp, header.Coinbase)
	sqw.stmts <- []byte(stmt)
	metrics.IndexerMetrics.BlocksCounter.Inc(1)
}

func (sqw *SQLWriter) upsertUncleCID(uncle models.UncleModel) {
	sqw.stmts <- []byte(fmt.Sprintf(uncleInsert, uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID,
		uncle.Reward, uncle.Index))
}

func (sqw *SQLWriter) upsertTransactionCID(transaction models.TxModel) {
	sqw.stmts <- []byte(fmt.Sprintf(txInsert, transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst,
		transaction.Src, transaction.Index, transaction.Type, transaction.Value))
	metrics.IndexerMetrics.TransactionsCounter.Inc(1)
}

func (sqw *SQLWriter) upsertReceiptCID(rct *models.ReceiptModel) {
	sqw.stmts <- []byte(fmt.Sprintf(rctInsert, rct.BlockNumber, rct.HeaderID, rct.TxID, rct.CID, rct.Contract,
		rct.PostState, rct.PostStatus))
	metrics.IndexerMetrics.ReceiptsCounter.Inc(1)
}

func (sqw *SQLWriter) upsertLogCID(logs []*models.LogsModel) {
	for _, l := range logs {
		sqw.stmts <- []byte(fmt.Sprintf(logInsert, l.BlockNumber, l.HeaderID, l.CID, l.ReceiptID, l.Address, l.Index, l.Topic0,
			l.Topic1, l.Topic2, l.Topic3))
		metrics.IndexerMetrics.LogsCounter.Inc(1)
	}
}

func (sqw *SQLWriter) upsertStateCID(stateNode models.StateNodeModel) {
	balance := stateNode.Balance
	if stateNode.Removed {
		balance = "0"
	}
	sqw.stmts <- []byte(fmt.Sprintf(stateInsert, stateNode.BlockNumber, stateNode.HeaderID, stateNode.StateKey, stateNode.CID,
		stateNode.Removed, true, balance, stateNode.Nonce, stateNode.CodeHash, stateNode.StorageRoot))
}

func (sqw *SQLWriter) upsertStorageCID(storageCID models.StorageNodeModel) {
	sqw.stmts <- []byte(fmt.Sprintf(storageInsert, storageCID.BlockNumber, storageCID.HeaderID, storageCID.StateKey, storageCID.StorageKey, storageCID.CID,
		storageCID.Removed, true, storageCID.Value))
}

// LoadWatchedAddresses loads watched addresses from a file
func (sqw *SQLWriter) loadWatchedAddresses() ([]common.Address, error) {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sqw.watchedAddressesFilePath)
	if err != nil {
		return nil, err
	}

	// extract addresses from the sql statements
	watchedAddresses := []common.Address{}
	for _, stmt := range stmts {
		addressString, err := parseWatchedAddressStatement(stmt)
		if err != nil {
			return nil, err
		}
		watchedAddresses = append(watchedAddresses, common.HexToAddress(addressString))
	}

	return watchedAddresses, nil
}

// InsertWatchedAddresses inserts the given addresses in a file
func (sqw *SQLWriter) insertWatchedAddresses(args []types.WatchAddressArg, currentBlockNumber *big.Int) error {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sqw.watchedAddressesFilePath)
	if err != nil {
		return err
	}

	// get already watched addresses
	var watchedAddresses []string
	for _, stmt := range stmts {
		addressString, err := parseWatchedAddressStatement(stmt)
		if err != nil {
			return err
		}

		watchedAddresses = append(watchedAddresses, addressString)
	}

	// append statements for new addresses to existing statements
	for _, arg := range args {
		// ignore if already watched
		if funk.Contains(watchedAddresses, arg.Address) {
			continue
		}

		stmt := fmt.Sprintf(watchedAddressesInsert, arg.Address, arg.CreatedAt, currentBlockNumber.Uint64())
		stmts = append(stmts, stmt)
	}

	return dumpWatchedAddressesStatements(sqw.watchedAddressesFilePath, stmts)
}

// RemoveWatchedAddresses removes the given watched addresses from a file
func (sqw *SQLWriter) removeWatchedAddresses(args []types.WatchAddressArg) error {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sqw.watchedAddressesFilePath)
	if err != nil {
		return err
	}

	// get rid of statements having addresses to be removed
	var filteredStmts []string
	for _, stmt := range stmts {
		addressString, err := parseWatchedAddressStatement(stmt)
		if err != nil {
			return err
		}

		toRemove := funk.Contains(args, func(arg types.WatchAddressArg) bool {
			return arg.Address == addressString
		})

		if !toRemove {
			filteredStmts = append(filteredStmts, stmt)
		}
	}

	return dumpWatchedAddressesStatements(sqw.watchedAddressesFilePath, filteredStmts)
}

// SetWatchedAddresses clears and inserts the given addresses in a file
func (sqw *SQLWriter) setWatchedAddresses(args []types.WatchAddressArg, currentBlockNumber *big.Int) error {
	var stmts []string
	for _, arg := range args {
		stmt := fmt.Sprintf(watchedAddressesInsert, arg.Address, arg.CreatedAt, currentBlockNumber.Uint64())
		stmts = append(stmts, stmt)
	}

	return dumpWatchedAddressesStatements(sqw.watchedAddressesFilePath, stmts)
}

// loadWatchedAddressesStatements loads sql statements from the given file in a string slice
func loadWatchedAddressesStatements(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}

		return nil, fmt.Errorf("error opening watched addresses file: %v", err)
	}
	defer file.Close()

	stmts := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		stmts = append(stmts, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error loading watched addresses: %v", err)
	}

	return stmts, nil
}

// dumpWatchedAddressesStatements dumps sql statements to the given file
func dumpWatchedAddressesStatements(filePath string, stmts []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating watched addresses file: %v", err)
	}
	defer file.Close()

	for _, stmt := range stmts {
		_, err := file.Write([]byte(stmt + "\n"))
		if err != nil {
			return fmt.Errorf("error inserting watched_addresses entry: %v", err)
		}
	}

	return nil
}

// parseWatchedAddressStatement parses given sql insert statement to extract the address argument
func parseWatchedAddressStatement(stmt string) (string, error) {
	parseResult, err := pg_query.Parse(stmt)
	if err != nil {
		return "", fmt.Errorf("error parsing sql stmt: %v", err)
	}

	// extract address argument from parse output for a SQL statement of form
	// "INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at)
	// VALUES ('0xabc', '123', '130') ON CONFLICT (address) DO NOTHING;"
	addressString := parseResult.Stmts[0].Stmt.GetInsertStmt().
		SelectStmt.GetSelectStmt().
		ValuesLists[0].GetList().
		Items[0].GetAConst().
		GetVal().
		GetString_().
		Str

	return addressString, nil
}
