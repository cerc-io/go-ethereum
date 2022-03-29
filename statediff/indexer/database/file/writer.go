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
	"fmt"
	"io"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	node "github.com/ipfs/go-ipld-format"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
)

var (
	nullHash        = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
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
}

// NewSQLWriter creates a new pointer to a Writer
func NewSQLWriter(wc io.WriteCloser) *SQLWriter {
	return &SQLWriter{
		wc:            wc,
		stmts:         make(chan []byte),
		collatedStmt:  make([]byte, writeBufferSize),
		flushChan:     make(chan struct{}),
		flushFinished: make(chan struct{}),
		quitChan:      make(chan struct{}),
		doneChan:      make(chan struct{}),
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

	ipldInsert = "INSERT INTO public.blocks (block_number, key, data) VALUES ('%s', '%s', '\\x%x');\n"

	headerInsert = "INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, " +
		"state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) VALUES " +
		"('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '\\x%x', %d, '%s', %d, '%s');\n"

	uncleInsert = "INSERT INTO eth.uncle_cids (block_number, block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES " +
		"('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"

	txInsert = "INSERT INTO eth.transaction_cids (block_number, header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type, " +
		"value) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '\\x%x', %d, '%s');\n"

	alInsert = "INSERT INTO eth.access_list_elements (block_number, tx_id, index, address, storage_keys) VALUES " +
		"('%s', '%s', %d, '%s', '%s');\n"

	rctInsert = "INSERT INTO eth.receipt_cids (block_number, tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, " +
		"post_status, log_root) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s');\n"

	logInsert = "INSERT INTO eth.log_cids (block_number, leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, " +
		"topic3, log_data) VALUES ('%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '\\x%x');\n"

	stateInsert = "INSERT INTO eth.state_cids (block_number, header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) " +
		"VALUES ('%s', '%s', '%s', '%s', '\\x%x', %d, %t, '%s');\n"

	accountInsert = "INSERT INTO eth.state_accounts (block_number, header_id, state_path, balance, nonce, code_hash, storage_root) " +
		"VALUES ('%s', '%s', '\\x%x', '%s', %d, '\\x%x', '%s');\n"

	storageInsert = "INSERT INTO eth.storage_cids (block_number, header_id, state_path, storage_leaf_key, cid, storage_path, " +
		"node_type, diff, mh_key) VALUES ('%s', '%s', '\\x%x', '%s', '%s', '\\x%x', %d, %t, '%s');\n"
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

func (sqw *SQLWriter) upsertIPLDNode(blockNumber string, i node.Node) {
	sqw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(i.Cid().Hash()).String(),
		Data:        i.RawData(),
	})
}

func (sqw *SQLWriter) upsertIPLDRaw(blockNumber string, codec, mh uint64, raw []byte) (string, string, error) {
	c, err := ipld.RawdataToCid(codec, raw, mh)
	if err != nil {
		return "", "", err
	}
	prefixedKey := blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String()
	sqw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         prefixedKey,
		Data:        raw,
	})
	return c.String(), prefixedKey, err
}

func (sqw *SQLWriter) upsertHeaderCID(header models.HeaderModel) {
	stmt := fmt.Sprintf(headerInsert, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
		header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, 1, header.Coinbase)
	sqw.stmts <- []byte(stmt)
	indexerMetrics.blocks.Inc(1)
}

func (sqw *SQLWriter) upsertUncleCID(uncle models.UncleModel) {
	sqw.stmts <- []byte(fmt.Sprintf(uncleInsert, uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID,
		uncle.Reward, uncle.MhKey))
}

func (sqw *SQLWriter) upsertTransactionCID(transaction models.TxModel) {
	sqw.stmts <- []byte(fmt.Sprintf(txInsert, transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst,
		transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type, transaction.Value))
	indexerMetrics.transactions.Inc(1)
}

func (sqw *SQLWriter) upsertAccessListElement(accessListElement models.AccessListElementModel) {
	sqw.stmts <- []byte(fmt.Sprintf(alInsert, accessListElement.BlockNumber, accessListElement.TxID, accessListElement.Index, accessListElement.Address,
		formatPostgresStringArray(accessListElement.StorageKeys)))
	indexerMetrics.accessListEntries.Inc(1)
}

func (sqw *SQLWriter) upsertReceiptCID(rct *models.ReceiptModel) {
	sqw.stmts <- []byte(fmt.Sprintf(rctInsert, rct.BlockNumber, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey,
		rct.PostState, rct.PostStatus, rct.LogRoot))
	indexerMetrics.receipts.Inc(1)
}

func (sqw *SQLWriter) upsertLogCID(logs []*models.LogsModel) {
	for _, l := range logs {
		sqw.stmts <- []byte(fmt.Sprintf(logInsert, l.BlockNumber, l.LeafCID, l.LeafMhKey, l.ReceiptID, l.Address, l.Index, l.Topic0,
			l.Topic1, l.Topic2, l.Topic3, l.Data))
		indexerMetrics.logs.Inc(1)
	}
}

func (sqw *SQLWriter) upsertStateCID(stateNode models.StateNodeModel) {
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	sqw.stmts <- []byte(fmt.Sprintf(stateInsert, stateNode.BlockNumber, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path,
		stateNode.NodeType, true, stateNode.MhKey))
}

func (sqw *SQLWriter) upsertStateAccount(stateAccount models.StateAccountModel) {
	sqw.stmts <- []byte(fmt.Sprintf(accountInsert, stateAccount.BlockNumber, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
		stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot))
}

func (sqw *SQLWriter) upsertStorageCID(storageCID models.StorageNodeModel) {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	sqw.stmts <- []byte(fmt.Sprintf(storageInsert, storageCID.BlockNumber, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID,
		storageCID.Path, storageCID.NodeType, true, storageCID.MhKey))
}
