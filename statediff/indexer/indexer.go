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

// This package provides an interface for pushing and indexing IPLD objects into a Postgres database
// Metrics for reporting processing and connection stats are defined in ./metrics.go
package indexer

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
)

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
	dbMetrics      = RegisterDBMetrics(metrics.DefaultRegistry)
)

// Indexer interface to allow substitution of mocks for testing
type Indexer interface {
	PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (*BlockTx, error)
	PushStateNode(tx *BlockTx, stateNode sdtypes.StateNode) error
	PushCodeAndCodeHash(tx *BlockTx, codeAndCodeHash sdtypes.CodeAndCodeHash) error
	ReportDBMetrics(delay time.Duration, quit <-chan bool)
}

// StateDiffIndexer satisfies the Indexer interface for ethereum statediff objects
type StateDiffIndexer struct {
	chainConfig *params.ChainConfig
	dbWriter    *PostgresCIDWriter
}

// NewStateDiffIndexer creates a pointer to a new PayloadConverter which satisfies the PayloadConverter interface
func NewStateDiffIndexer(chainConfig *params.ChainConfig, db *postgres.DB) *StateDiffIndexer {
	return &StateDiffIndexer{
		chainConfig: chainConfig,
		dbWriter:    NewPostgresCIDWriter(db),
	}
}

type BlockTx struct {
	dbtx        *sqlx.Tx
	BlockNumber uint64
	headerID    int64
	Close       func(err error) error
}

// Reporting function to run as goroutine
func (sdi *StateDiffIndexer) ReportDBMetrics(delay time.Duration, quit <-chan bool) {
	if !metrics.Enabled {
		return
	}
	ticker := time.NewTicker(delay)
	go func() {
		for {
			select {
			case <-ticker.C:
				dbMetrics.Update(sdi.dbWriter.db.Stats())
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// Pushes and indexes block data in database, except state & storage nodes (includes header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (*BlockTx, error) {
	start, t := time.Now(), time.Now()
	blockHash := block.Hash()
	blockHashStr := blockHash.String()
	height := block.NumberU64()
	traceMsg := fmt.Sprintf("indexer stats for statediff at %d with hash %s:\r\n", height, blockHashStr)
	transactions := block.Transactions()
	// Derive any missing fields
	if err := receipts.DeriveFields(sdi.chainConfig, blockHash, height, transactions); err != nil {
		return nil, err
	}

	// Generate the block iplds
	headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, rctTrieNodes, logTrieNodes, logLeafNodeCIDs, rctLeafNodeCIDs, err := ipld.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, fmt.Errorf("error creating IPLD nodes from block and receipts: %v", err)
	}

	if len(txNodes) != len(rctNodes) || len(rctNodes) != len(rctLeafNodeCIDs) {
		return nil, fmt.Errorf("expected number of transactions (%d), receipts (%d), and receipt trie leaf nodes (%d)to be equal", len(txNodes), len(rctNodes), len(rctLeafNodeCIDs))
	}

	// Calculate reward
	var reward *big.Int
	// in PoA networks block reward is 0
	if sdi.chainConfig.Clique != nil {
		reward = big.NewInt(0)
	} else {
		reward = CalcEthBlockReward(block.Header(), block.Uncles(), block.Transactions(), receipts)
	}
	t = time.Now()
	// Begin new db tx for everything
	tx, err := sdi.dbWriter.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			shared.Rollback(tx)
			panic(p)
		} else if err != nil {
			shared.Rollback(tx)
		}
	}()
	blockTx := &BlockTx{
		dbtx: tx,
		// handle transaction commit or rollback for any return case
		Close: func(err error) error {
			if p := recover(); p != nil {
				shared.Rollback(tx)
				panic(p)
			} else if err != nil {
				shared.Rollback(tx)
			} else {
				tDiff := time.Since(t)
				indexerMetrics.tStateStoreCodeProcessing.Update(tDiff)
				traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
				t = time.Now()
				err = tx.Commit()
				tDiff = time.Since(t)
				indexerMetrics.tPostgresCommit.Update(tDiff)
				traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			}
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	tDiff := time.Since(t)
	indexerMetrics.tFreePostgres.Update(tDiff)

	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// Publish and index header, collect headerID
	var headerID int64
	headerID, err = sdi.processHeader(tx, block.Header(), headerNode, reward, totalDifficulty)
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	indexerMetrics.tHeaderProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index uncles
	err = sdi.processUncles(tx, headerID, height, uncleNodes)
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	indexerMetrics.tUncleProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index receipts and txs
	err = sdi.processReceiptsAndTxs(tx, processArgs{
		headerID:        headerID,
		blockNumber:     block.Number(),
		receipts:        receipts,
		txs:             transactions,
		rctNodes:        rctNodes,
		rctTrieNodes:    rctTrieNodes,
		txNodes:         txNodes,
		txTrieNodes:     txTrieNodes,
		logTrieNodes:    logTrieNodes,
		logLeafNodeCIDs: logLeafNodeCIDs,
		rctLeafNodeCIDs: rctLeafNodeCIDs,
	})
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	indexerMetrics.tTxAndRecProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("tx and receipt processing time: %s\r\n", tDiff.String())
	t = time.Now()

	blockTx.BlockNumber = height
	blockTx.headerID = headerID
	return blockTx, err
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(tx *sqlx.Tx, header *types.Header, headerNode node.Node, reward, td *big.Int) (int64, error) {
	// publish header
	if err := shared.PublishIPLD(tx, headerNode); err != nil {
		return 0, fmt.Errorf("error publishing header IPLD: %v", err)
	}

	var baseFee *int64
	if header.BaseFee != nil {
		baseFee = new(int64)
		*baseFee = header.BaseFee.Int64()
	}

	// index header
	return sdi.dbWriter.upsertHeaderCID(tx, models.HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       header.Hash().String(),
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
		BaseFee:         baseFee,
	})
}

func (sdi *StateDiffIndexer) processUncles(tx *sqlx.Tx, headerID int64, blockNumber uint64, uncleNodes []*ipld.EthHeader) error {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		if err := shared.PublishIPLD(tx, uncleNode); err != nil {
			return fmt.Errorf("error publishing uncle IPLD: %v", err)
		}
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = CalcUncleMinerReward(blockNumber, uncleNode.Number.Uint64())
		}
		uncle := models.UncleModel{
			CID:        uncleNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash: uncleNode.ParentHash.String(),
			BlockHash:  uncleNode.Hash().String(),
			Reward:     uncleReward.String(),
		}
		if err := sdi.dbWriter.upsertUncleCID(tx, uncle, headerID); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arguments to processReceiptsAndTxs
type processArgs struct {
	headerID        int64
	blockNumber     *big.Int
	receipts        types.Receipts
	txs             types.Transactions
	rctNodes        []*ipld.EthReceipt
	rctTrieNodes    []*ipld.EthRctTrie
	txNodes         []*ipld.EthTx
	txTrieNodes     []*ipld.EthTxTrie
	logTrieNodes    [][]*ipld.EthLogTrie
	logLeafNodeCIDs [][]cid.Cid
	rctLeafNodeCIDs []cid.Cid
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdi *StateDiffIndexer) processReceiptsAndTxs(tx *sqlx.Tx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		// tx that corresponds with this receipt
		trx := args.txs[i]
		from, err := types.Sender(signer, trx)
		if err != nil {
			return fmt.Errorf("error deriving tx sender: %v", err)
		}

		for _, trie := range args.logTrieNodes[i] {
			if err = shared.PublishIPLD(tx, trie); err != nil {
				return fmt.Errorf("error publishing log trie node IPLD: %w", err)
			}
		}

		// publish the txs and receipts
		txNode := args.txNodes[i]
		if err := shared.PublishIPLD(tx, txNode); err != nil {
			return fmt.Errorf("error publishing tx IPLD: %v", err)
		}

		// Indexing
		// extract topic and contract data from the receipt for indexing
		mappedContracts := make(map[string]bool) // use map to avoid duplicate addresses
		logDataSet := make([]*models.LogsModel, len(receipt.Logs))
		for idx, l := range receipt.Logs {
			topicSet := make([]string, 4)
			for ti, topic := range l.Topics {
				topicSet[ti] = topic.Hex()
			}

			if !args.logLeafNodeCIDs[i][idx].Defined() {
				return fmt.Errorf("invalid log cid")
			}

			mappedContracts[l.Address.String()] = true
			logDataSet[idx] = &models.LogsModel{
				ID:        0,
				Address:   l.Address.String(),
				Index:     int64(l.Index),
				Data:      l.Data,
				LeafCID:   args.logLeafNodeCIDs[i][idx].String(),
				LeafMhKey: shared.MultihashKeyFromCID(args.logLeafNodeCIDs[i][idx]),
				Topic0:    topicSet[0],
				Topic1:    topicSet[1],
				Topic2:    topicSet[2],
				Topic3:    topicSet[3],
			}
		}
		// these are the contracts seen in the logs
		logContracts := make([]string, 0, len(mappedContracts))
		for addr := range mappedContracts {
			logContracts = append(logContracts, addr)
		}
		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		if contract != "" {
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}
		// index tx first so that the receipt can reference it by FK
		txModel := models.TxModel{
			Dst:    shared.HandleZeroAddrPointer(trx.To()),
			Src:    shared.HandleZeroAddr(from),
			TxHash: trx.Hash().String(),
			Index:  int64(i),
			Data:   trx.Data(),
			CID:    txNode.Cid().String(),
			MhKey:  shared.MultihashKeyFromCID(txNode.Cid()),
		}
		txType := trx.Type()
		if txType != types.LegacyTxType {
			txModel.Type = &txType
		}
		txID, err := sdi.dbWriter.upsertTransactionCID(tx, txModel, args.headerID)
		if err != nil {
			return err
		}

		// index access list if this is one
		for j, accessListElement := range trx.AccessList() {
			storageKeys := make([]string, len(accessListElement.StorageKeys))
			for k, storageKey := range accessListElement.StorageKeys {
				storageKeys[k] = storageKey.Hex()
			}
			accessListElementModel := models.AccessListElementModel{
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}
			if err := sdi.dbWriter.upsertAccessListElement(tx, accessListElementModel, txID); err != nil {
				return err
			}
		}

		// index the receipt
		if !args.rctLeafNodeCIDs[i].Defined() {
			return fmt.Errorf("invalid receipt leaf node cid")
		}

		rctModel := &models.ReceiptModel{
			Contract:     contract,
			ContractHash: contractHash,
			LeafCID:      args.rctLeafNodeCIDs[i].String(),
			LeafMhKey:    shared.MultihashKeyFromCID(args.rctLeafNodeCIDs[i]),
			LogRoot:      args.rctNodes[i].LogRoot.String(),
		}
		if len(receipt.PostState) == 0 {
			rctModel.PostStatus = receipt.Status
		} else {
			rctModel.PostState = common.Bytes2Hex(receipt.PostState)
		}

		receiptID, err := sdi.dbWriter.upsertReceiptCID(tx, rctModel, txID)
		if err != nil {
			return err
		}

		if err = sdi.dbWriter.upsertLogCID(tx, logDataSet, receiptID); err != nil {
			return err
		}
	}

	// publish trie nodes, these aren't indexed directly
	for _, n := range args.txTrieNodes {
		if err := shared.PublishIPLD(tx, n); err != nil {
			return fmt.Errorf("error publishing tx trie node IPLD: %w", err)
		}
	}

	for _, n := range args.rctTrieNodes {
		if err := shared.PublishIPLD(tx, n); err != nil {
			return fmt.Errorf("error publishing rct trie node IPLD: %w", err)
		}
	}

	return nil
}

func (sdi *StateDiffIndexer) PushStateNode(tx *BlockTx, stateNode sdtypes.StateNode) error {
	// publish the state node
	stateCIDStr, err := shared.PublishRaw(tx.dbtx, ipld.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
	if err != nil {
		return fmt.Errorf("error publishing state node IPLD: %v", err)
	}
	mhKey, _ := shared.MultihashKeyFromCIDString(stateCIDStr)
	stateModel := models.StateNodeModel{
		Path:     stateNode.Path,
		StateKey: common.BytesToHash(stateNode.LeafKey).String(),
		CID:      stateCIDStr,
		MhKey:    mhKey,
		NodeType: ResolveFromNodeType(stateNode.NodeType),
	}
	// index the state node, collect the stateID to reference by FK
	stateID, err := sdi.dbWriter.upsertStateCID(tx.dbtx, stateModel, tx.headerID)
	if err != nil {
		return err
	}
	// if we have a leaf, decode and index the account data
	if stateNode.NodeType == sdtypes.Leaf {
		var i []interface{}
		if err := rlp.DecodeBytes(stateNode.NodeValue, &i); err != nil {
			return fmt.Errorf("error decoding state leaf node rlp: %s", err.Error())
		}
		if len(i) != 2 {
			return fmt.Errorf("eth IPLDPublisher expected state leaf node rlp to decode into two elements")
		}
		var account state.Account
		if err := rlp.DecodeBytes(i[1].([]byte), &account); err != nil {
			return fmt.Errorf("error decoding state account rlp: %s", err.Error())
		}
		accountModel := models.StateAccountModel{
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}
		if err := sdi.dbWriter.upsertStateAccount(tx.dbtx, accountModel, stateID); err != nil {
			return err
		}
	}
	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		storageCIDStr, err := shared.PublishRaw(tx.dbtx, ipld.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error publishing storage node IPLD: %v", err)
		}
		mhKey, _ := shared.MultihashKeyFromCIDString(storageCIDStr)
		storageModel := models.StorageNodeModel{
			Path:       storageNode.Path,
			StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
			CID:        storageCIDStr,
			MhKey:      mhKey,
			NodeType:   ResolveFromNodeType(storageNode.NodeType),
		}
		if err := sdi.dbWriter.upsertStorageCID(tx.dbtx, storageModel, stateID); err != nil {
			return err
		}
	}

	return nil
}

// Publishes code and codehash pairs to the ipld database
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(tx *BlockTx, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	// codec doesn't matter since db key is multihash-based
	mhKey, err := shared.MultihashKeyFromKeccak256(codeAndCodeHash.Hash)
	if err != nil {
		return fmt.Errorf("error deriving multihash key from codehash: %v", err)
	}
	if err := shared.PublishDirect(tx.dbtx, mhKey, codeAndCodeHash.Code); err != nil {
		return fmt.Errorf("error publishing code IPLD: %v", err)
	}
	return nil
}
