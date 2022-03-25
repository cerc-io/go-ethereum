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

// Package sql provides an interface for pushing and indexing IPLD objects into a sql database
// Metrics for reporting processing and connection stats are defined in ./metrics.go

package sql

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	ipld2 "github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
	dbMetrics      = RegisterDBMetrics(metrics.DefaultRegistry)
)

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of an SQL sql
type StateDiffIndexer struct {
	ctx         context.Context
	chainConfig *params.ChainConfig
	dbWriter    *Writer
	blockNumber string
}

// NewStateDiffIndexer creates a sql implementation of interfaces.StateDiffIndexer
func NewStateDiffIndexer(ctx context.Context, chainConfig *params.ChainConfig, db Database) (*StateDiffIndexer, error) {
	return &StateDiffIndexer{
		ctx:         ctx,
		chainConfig: chainConfig,
		dbWriter:    NewWriter(db),
	}, nil
}

// ReportDBMetrics is a reporting function to run as goroutine
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

// PushBlock pushes and indexes block data in sql, except state & storage nodes (includes header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (interfaces.Batch, error) {
	start, t := time.Now(), time.Now()
	sdi.blockNumber = block.Number().String()
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
	headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, rctTrieNodes, logTrieNodes, logLeafNodeCIDs, rctLeafNodeCIDs, err := ipld2.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, fmt.Errorf("error creating IPLD nodes from block and receipts: %v", err)
	}

	if len(txNodes) != len(rctNodes) || len(rctNodes) != len(rctLeafNodeCIDs) {
		return nil, fmt.Errorf("expected number of transactions (%d), receipts (%d), and receipt trie leaf nodes (%d) to be equal", len(txNodes), len(rctNodes), len(rctLeafNodeCIDs))
	}
	if len(txTrieNodes) != len(rctTrieNodes) {
		return nil, fmt.Errorf("expected number of tx trie (%d) and rct trie (%d) nodes to be equal", len(txTrieNodes), len(rctTrieNodes))
	}

	// Calculate reward
	var reward *big.Int
	// in PoA networks block reward is 0
	if sdi.chainConfig.Clique != nil {
		reward = big.NewInt(0)
	} else {
		reward = shared.CalcEthBlockReward(block.Header(), block.Uncles(), block.Transactions(), receipts)
	}
	t = time.Now()

	// Begin new db tx for everything
	tx, err := sdi.dbWriter.db.Begin(sdi.ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, tx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, tx)
		}
	}()
	blockTx := &BatchTx{
		removedCacheFlag: new(uint32),
		ctx:              sdi.ctx,
		BlockNumber:      block.Number().String(),
		stm:              sdi.dbWriter.db.InsertIPLDsStm(),
		iplds:            make(chan models.IPLDModel),
		quit:             make(chan struct{}),
		ipldCache: models.IPLDBatch{
			BlockNumbers: make([]string, 0, startingCacheCapacity),
			Keys:         make([]string, 0, startingCacheCapacity),
			Values:       make([][]byte, 0, startingCacheCapacity),
		},
		dbtx: tx,
		// handle transaction commit or rollback for any return case
		submit: func(self *BatchTx, err error) error {
			defer func() {
				close(self.quit)
				close(self.iplds)
			}()
			if p := recover(); p != nil {
				log.Info("panic detected before tx submission, rolling back the tx", "panic", p)
				rollback(sdi.ctx, tx)
				panic(p)
			} else if err != nil {
				log.Info("error detected before tx submission, rolling back the tx", "error", err)
				rollback(sdi.ctx, tx)
			} else {
				tDiff := time.Since(t)
				indexerMetrics.tStateStoreCodeProcessing.Update(tDiff)
				traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
				t = time.Now()
				if err := self.flush(); err != nil {
					rollback(sdi.ctx, tx)
					traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
					log.Debug(traceMsg)
					return err
				}
				err = tx.Commit(sdi.ctx)
				tDiff = time.Since(t)
				indexerMetrics.tPostgresCommit.Update(tDiff)
				traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			}
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	go blockTx.cache()

	tDiff := time.Since(t)
	indexerMetrics.tFreePostgres.Update(tDiff)

	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// Publish and index header, collect headerID
	var headerID string
	headerID, err = sdi.processHeader(blockTx, block.Header(), headerNode, reward, totalDifficulty)
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	indexerMetrics.tHeaderProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index uncles
	err = sdi.processUncles(blockTx, headerID, block.NumberU64(), uncleNodes)
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	indexerMetrics.tUncleProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index receipts and txs
	err = sdi.processReceiptsAndTxs(blockTx, processArgs{
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

	return blockTx, err
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(tx *BatchTx, header *types.Header, headerNode node.Node, reward, td *big.Int) (string, error) {
	tx.cacheIPLD(headerNode)

	var baseFee *string
	if header.BaseFee != nil {
		baseFee = new(string)
		*baseFee = header.BaseFee.String()
	}
	headerID := header.Hash().String()
	// index header
	return headerID, sdi.dbWriter.upsertHeaderCID(tx.dbtx, models.HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     sdi.blockNumber,
		BlockHash:       headerID,
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
		Coinbase:        header.Coinbase.String(),
	})
}

// processUncles publishes and indexes uncle IPLDs in Postgres
func (sdi *StateDiffIndexer) processUncles(tx *BatchTx, headerID string, blockNumber uint64, uncleNodes []*ipld2.EthHeader) error {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		tx.cacheIPLD(uncleNode)
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber, uncleNode.Number.Uint64())
		}
		uncle := models.UncleModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    headerID,
			CID:         uncleNode.Cid().String(),
			MhKey:       shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash:  uncleNode.ParentHash.String(),
			BlockHash:   uncleNode.Hash().String(),
			Reward:      uncleReward.String(),
		}
		if err := sdi.dbWriter.upsertUncleCID(tx.dbtx, uncle); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arguments to processReceiptsAndTxs
type processArgs struct {
	headerID        string
	blockNumber     *big.Int
	receipts        types.Receipts
	txs             types.Transactions
	rctNodes        []*ipld2.EthReceipt
	rctTrieNodes    []*ipld2.EthRctTrie
	txNodes         []*ipld2.EthTx
	txTrieNodes     []*ipld2.EthTxTrie
	logTrieNodes    [][]node.Node
	logLeafNodeCIDs [][]cid.Cid
	rctLeafNodeCIDs []cid.Cid
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdi *StateDiffIndexer) processReceiptsAndTxs(tx *BatchTx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		for _, logTrieNode := range args.logTrieNodes[i] {
			tx.cacheIPLD(logTrieNode)
		}
		txNode := args.txNodes[i]
		tx.cacheIPLD(txNode)

		// index tx
		trx := args.txs[i]
		txID := trx.Hash().String()

		var val string
		if trx.Value() != nil {
			val = trx.Value().String()
		}

		// derive sender for the tx that corresponds with this receipt
		from, err := types.Sender(signer, trx)
		if err != nil {
			return fmt.Errorf("error deriving tx sender: %v", err)
		}
		txModel := models.TxModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    args.headerID,
			Dst:         shared.HandleZeroAddrPointer(trx.To()),
			Src:         shared.HandleZeroAddr(from),
			TxHash:      txID,
			Index:       int64(i),
			Data:        trx.Data(),
			CID:         txNode.Cid().String(),
			MhKey:       shared.MultihashKeyFromCID(txNode.Cid()),
			Type:        trx.Type(),
			Value:       val,
		}
		if err := sdi.dbWriter.upsertTransactionCID(tx.dbtx, txModel); err != nil {
			return err
		}

		// index access list if this is one
		for j, accessListElement := range trx.AccessList() {
			storageKeys := make([]string, len(accessListElement.StorageKeys))
			for k, storageKey := range accessListElement.StorageKeys {
				storageKeys[k] = storageKey.Hex()
			}
			accessListElementModel := models.AccessListElementModel{
				BlockNumber: sdi.blockNumber,
				TxID:        txID,
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}
			if err := sdi.dbWriter.upsertAccessListElement(tx.dbtx, accessListElementModel); err != nil {
				return err
			}
		}

		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		if contract != "" {
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}

		// index receipt
		if !args.rctLeafNodeCIDs[i].Defined() {
			return fmt.Errorf("invalid receipt leaf node cid")
		}

		rctModel := &models.ReceiptModel{
			BlockNumber:  sdi.blockNumber,
			TxID:         txID,
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

		if err := sdi.dbWriter.upsertReceiptCID(tx.dbtx, rctModel); err != nil {
			return err
		}

		// index logs
		logDataSet := make([]*models.LogsModel, len(receipt.Logs))
		for idx, l := range receipt.Logs {
			topicSet := make([]string, 4)
			for ti, topic := range l.Topics {
				topicSet[ti] = topic.Hex()
			}

			if !args.logLeafNodeCIDs[i][idx].Defined() {
				return fmt.Errorf("invalid log cid")
			}

			logDataSet[idx] = &models.LogsModel{
				BlockNumber: sdi.blockNumber,
				ReceiptID:   txID,
				Address:     l.Address.String(),
				Index:       int64(l.Index),
				Data:        l.Data,
				LeafCID:     args.logLeafNodeCIDs[i][idx].String(),
				LeafMhKey:   shared.MultihashKeyFromCID(args.logLeafNodeCIDs[i][idx]),
				Topic0:      topicSet[0],
				Topic1:      topicSet[1],
				Topic2:      topicSet[2],
				Topic3:      topicSet[3],
			}
		}

		if err := sdi.dbWriter.upsertLogCID(tx.dbtx, logDataSet); err != nil {
			return err
		}
	}

	// publish trie nodes, these aren't indexed directly
	for i, n := range args.txTrieNodes {
		tx.cacheIPLD(n)
		tx.cacheIPLD(args.rctTrieNodes[i])
	}

	return nil
}

// PushStateNode publishes and indexes a state diff node object (including any child storage nodes) in the IPLD sql
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateNode, headerID string) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	var stateModel models.StateNodeModel
	if stateNode.NodeType == sdtypes.Removed {
		tx.cacheRemoved(shared.RemovedNodeMhKey, []byte{})
		stateModel = models.StateNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			Path:        stateNode.Path,
			StateKey:    common.BytesToHash(stateNode.LeafKey).String(),
			CID:         shared.RemovedNodeStateCID,
			MhKey:       shared.RemovedNodeMhKey,
			NodeType:    stateNode.NodeType.Int(),
		}
	} else {
		stateCIDStr, stateMhKey, err := tx.cacheRaw(ipld2.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error generating and cacheing state node IPLD: %v", err)
		}
		stateModel = models.StateNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			Path:        stateNode.Path,
			StateKey:    common.BytesToHash(stateNode.LeafKey).String(),
			CID:         stateCIDStr,
			MhKey:       stateMhKey,
			NodeType:    stateNode.NodeType.Int(),
		}
	}

	// index the state node
	if err := sdi.dbWriter.upsertStateCID(tx.dbtx, stateModel); err != nil {
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
		var account types.StateAccount
		if err := rlp.DecodeBytes(i[1].([]byte), &account); err != nil {
			return fmt.Errorf("error decoding state account rlp: %s", err.Error())
		}
		accountModel := models.StateAccountModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			StatePath:   stateNode.Path,
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}
		if err := sdi.dbWriter.upsertStateAccount(tx.dbtx, accountModel); err != nil {
			return err
		}
	}

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		if storageNode.NodeType == sdtypes.Removed {
			tx.cacheRemoved(shared.RemovedNodeMhKey, []byte{})
			storageModel := models.StorageNodeModel{
				BlockNumber: tx.BlockNumber,
				HeaderID:    headerID,
				StatePath:   stateNode.Path,
				Path:        storageNode.Path,
				StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
				CID:         shared.RemovedNodeStorageCID,
				MhKey:       shared.RemovedNodeMhKey,
				NodeType:    storageNode.NodeType.Int(),
			}
			if err := sdi.dbWriter.upsertStorageCID(tx.dbtx, storageModel); err != nil {
				return err
			}
			continue
		}
		storageCIDStr, storageMhKey, err := tx.cacheRaw(ipld2.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error generating and cacheing storage node IPLD: %v", err)
		}
		storageModel := models.StorageNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			StatePath:   stateNode.Path,
			Path:        storageNode.Path,
			StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
			CID:         storageCIDStr,
			MhKey:       storageMhKey,
			NodeType:    storageNode.NodeType.Int(),
		}
		if err := sdi.dbWriter.upsertStorageCID(tx.dbtx, storageModel); err != nil {
			return err
		}
	}

	return nil
}

// PushCodeAndCodeHash publishes code and codehash pairs to the ipld sql
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(batch interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// codec doesn't matter since db key is multihash-based
	mhKey, err := shared.MultihashKeyFromKeccak256(codeAndCodeHash.Hash)
	if err != nil {
		return fmt.Errorf("error deriving multihash key from codehash: %v", err)
	}
	tx.cacheDirect(mhKey, codeAndCodeHash.Code)
	return nil
}

// Close satisfies io.Closer
func (sdi *StateDiffIndexer) Close() error {
	return sdi.dbWriter.Close()
}

// Update the known gaps table with the gap information.

// LoadWatchedAddresses reads watched addresses from the database
func (sdi *StateDiffIndexer) LoadWatchedAddresses() ([]common.Address, error) {
	addressStrings := make([]string, 0)
	pgStr := "SELECT address FROM eth_meta.watched_addresses"
	err := sdi.dbWriter.db.Select(sdi.ctx, &addressStrings, pgStr)
	if err != nil {
		return nil, fmt.Errorf("error loading watched addresses: %v", err)
	}

	watchedAddresses := []common.Address{}
	for _, addressString := range addressStrings {
		watchedAddresses = append(watchedAddresses, common.HexToAddress(addressString))
	}

	return watchedAddresses, nil
}

// InsertWatchedAddresses inserts the given addresses in the database
func (sdi *StateDiffIndexer) InsertWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	tx, err := sdi.dbWriter.db.Begin(sdi.ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, tx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, tx)
		} else {
			err = tx.Commit(sdi.ctx)
		}
	}()

	for _, arg := range args {
		_, err = tx.Exec(sdi.ctx, `INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at) VALUES ($1, $2, $3) ON CONFLICT (address) DO NOTHING`,
			arg.Address, arg.CreatedAt, currentBlockNumber.Uint64())
		if err != nil {
			return fmt.Errorf("error inserting watched_addresses entry: %v", err)
		}
	}

	return err
}

// RemoveWatchedAddresses removes the given watched addresses from the database
func (sdi *StateDiffIndexer) RemoveWatchedAddresses(args []sdtypes.WatchAddressArg) error {
	tx, err := sdi.dbWriter.db.Begin(sdi.ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, tx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, tx)
		} else {
			err = tx.Commit(sdi.ctx)
		}
	}()

	for _, arg := range args {
		_, err = tx.Exec(sdi.ctx, `DELETE FROM eth_meta.watched_addresses WHERE address = $1`, arg.Address)
		if err != nil {
			return fmt.Errorf("error removing watched_addresses entry: %v", err)
		}
	}

	return err
}

// SetWatchedAddresses clears and inserts the given addresses in the database
func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	tx, err := sdi.dbWriter.db.Begin(sdi.ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, tx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, tx)
		} else {
			err = tx.Commit(sdi.ctx)
		}
	}()

	_, err = tx.Exec(sdi.ctx, `DELETE FROM eth_meta.watched_addresses`)
	if err != nil {
		return fmt.Errorf("error setting watched_addresses table: %v", err)
	}

	for _, arg := range args {
		_, err = tx.Exec(sdi.ctx, `INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at) VALUES ($1, $2, $3) ON CONFLICT (address) DO NOTHING`,
			arg.Address, arg.CreatedAt, currentBlockNumber.Uint64())
		if err != nil {
			return fmt.Errorf("error setting watched_addresses table: %v", err)
		}
	}

	return err
}

// ClearWatchedAddresses clears all the watched addresses from the database
func (sdi *StateDiffIndexer) ClearWatchedAddresses() error {
	_, err := sdi.dbWriter.db.Exec(sdi.ctx, `DELETE FROM eth_meta.watched_addresses`)
	if err != nil {
		return fmt.Errorf("error clearing watched_addresses table: %v", err)
	}

	return nil
}

// This is a simple wrapper function which will run QueryRow on the DB
func (sdi *StateDiffIndexer) QueryDb(queryString string) (string, error) {
	var ret string
	err := sdi.dbWriter.db.QueryRow(context.Background(), queryString).Scan(&ret)
	if err != nil {
		log.Error(fmt.Sprint("Can't properly query the DB for query: ", queryString))
		return "", err
	}
	return ret, nil
}

// This function is a simple wrapper which will call QueryDb but the return value will be
// a big int instead of a string
func (sdi *StateDiffIndexer) QueryDbToBigInt(queryString string) (*big.Int, error) {
	ret := new(big.Int)
	res, err := sdi.QueryDb(queryString)
	if err != nil {
		return ret, err
	}
	ret, ok := ret.SetString(res, 10)
	if !ok {
		log.Error(fmt.Sprint("Can't turn the res ", res, "into a bigInt"))
		return ret, fmt.Errorf("Can't turn %s into a bigInt", res)
	}
	return ret, nil
}

// Users provide the latestBlockInDb and the latestBlockOnChain
// as well as the expected difference. This function does some simple math.
// The expected difference for the time being is going to be 1, but as we run
// More geth nodes, the expected difference might fluctuate.
func isGap(latestBlockInDb *big.Int, latestBlockOnChain *big.Int, expectedDifference *big.Int) bool {
	latestBlock := big.NewInt(0)
	if latestBlock.Add(latestBlockOnChain, expectedDifference) != latestBlockInDb {
		return true
	}
	return false

}

// This function will check for Gaps and update the DB if gaps are found.
// The processingKey will currently be set to 0, but as we start to leverage horizontal scaling
// It might be a useful parameter to update depending on the geth node.
// TODO:
// REmove the return value
// Write to file if err in writing to DB
func (sdi *StateDiffIndexer) FindAndUpdateGaps(latestBlockOnChain *big.Int, expectedDifference *big.Int, processingKey int64, fileIndexer interfaces.StateDiffIndexer) error {
	dbQueryString := "SELECT MAX(block_number) FROM eth.header_cids"
	latestBlockInDb, err := sdi.QueryDbToBigInt(dbQueryString)
	if err != nil {
		return err
	}

	gapExists := isGap(latestBlockInDb, latestBlockOnChain, expectedDifference)
	if gapExists {
		startBlock := big.NewInt(0)
		endBlock := big.NewInt(0)
		startBlock.Add(latestBlockInDb, expectedDifference)
		endBlock.Sub(latestBlockOnChain, expectedDifference)

		log.Warn(fmt.Sprint("Found Gaps starting at, ", startBlock, " and ending at, ", endBlock))
		err := sdi.PushKnownGaps(startBlock, endBlock, false, processingKey, fileIndexer)
		if err != nil {
			// Write to file SQL file instead!!!
			// If write to SQL file fails, write to disk. Handle this within the write to SQL file function!
			return err
		}
	}

	return nil
}
