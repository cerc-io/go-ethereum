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
	"bytes"
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	metrics2 "github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of an SQL sql
type StateDiffIndexer struct {
	ctx         context.Context
	chainConfig *params.ChainConfig
	dbWriter    *Writer
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
				metrics2.DBMetrics.Update(sdi.dbWriter.db.Stats())
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
	blockHash := block.Hash()
	blockHashStr := blockHash.String()
	height := block.NumberU64()
	traceMsg := fmt.Sprintf("indexer stats for statediff at %d with hash %s:\r\n", height, blockHashStr)
	transactions := block.Transactions()
	// Derive any missing fields
	if err := receipts.DeriveFields(sdi.chainConfig, blockHash, height, block.BaseFee(), transactions); err != nil {
		return nil, err
	}

	// Generate the block iplds
	headerNode, txNodes, rctNodes, logNodes, err := ipld.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, fmt.Errorf("error creating IPLD nodes from block and receipts: %v", err)
	}

	if len(txNodes) != len(rctNodes) {
		return nil, fmt.Errorf("expected number of transactions (%d), receipts (%d)", len(txNodes), len(rctNodes))
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

	// Begin new DB tx for everything
	tx := NewDelayedTx(sdi.dbWriter.db)
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
				metrics2.IndexerMetrics.StateStoreCodeProcessingTimer.Update(tDiff)
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
				metrics2.IndexerMetrics.PostgresCommitTimer.Update(tDiff)
				traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			}
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	go blockTx.cache()

	tDiff := time.Since(t)
	metrics2.IndexerMetrics.FreePostgresTimer.Update(tDiff)

	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// Publish and index header, collect headerID
	var headerID string
	headerID, err = sdi.processHeader(blockTx, block.Header(), headerNode, reward, totalDifficulty)
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	metrics2.IndexerMetrics.HeaderProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index uncles
	err = sdi.processUncles(blockTx, headerID, block.Number(), block.UncleHash(), block.Uncles())
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	metrics2.IndexerMetrics.UncleProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index receipts and txs
	err = sdi.processReceiptsAndTxs(blockTx, processArgs{
		headerID:    headerID,
		blockNumber: block.Number(),
		receipts:    receipts,
		txs:         transactions,
		rctNodes:    rctNodes,
		txNodes:     txNodes,
		logNodes:    logNodes,
	})
	if err != nil {
		return nil, err
	}
	tDiff = time.Since(t)
	metrics2.IndexerMetrics.TxAndRecProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("tx and receipt processing time: %s\r\n", tDiff.String())
	t = time.Now()

	return blockTx, err
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(tx *BatchTx, header *types.Header, headerNode ipld.IPLD, reward, td *big.Int) (string, error) {
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
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       headerID,
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UnclesHash:      header.UncleHash.String(),
		Timestamp:       header.Time,
		Coinbase:        header.Coinbase.String(),
	})
}

// processUncles publishes and indexes uncle IPLDs in Postgres
func (sdi *StateDiffIndexer) processUncles(tx *BatchTx, headerID string, blockNumber *big.Int, unclesHash common.Hash, uncles []*types.Header) error {
	// publish and index uncles
	uncleEncoding, err := rlp.EncodeToBytes(uncles)
	if err != nil {
		return err
	}
	preparedHash := crypto.Keccak256Hash(uncleEncoding)
	if !bytes.Equal(preparedHash.Bytes(), unclesHash.Bytes()) {
		return fmt.Errorf("derived uncles hash (%s) does not match the hash in the header (%s)", preparedHash.Hex(), unclesHash.Hex())
	}
	unclesCID, err := ipld.RawdataToCid(ipld.MEthHeaderList, uncleEncoding, multihash.KECCAK_256)
	if err != nil {
		return err
	}
	tx.cacheDirect(unclesCID.String(), uncleEncoding)
	for i, uncle := range uncles {
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber.Uint64(), uncle.Number.Uint64())
		}
		uncle := models.UncleModel{
			BlockNumber: blockNumber.String(),
			HeaderID:    headerID,
			CID:         unclesCID.String(),
			ParentHash:  uncle.ParentHash.String(),
			BlockHash:   uncle.Hash().String(),
			Reward:      uncleReward.String(),
			Index:       int64(i),
		}
		if err := sdi.dbWriter.upsertUncleCID(tx.dbtx, uncle); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arguments to processReceiptsAndTxs
type processArgs struct {
	headerID    string
	blockNumber *big.Int
	receipts    types.Receipts
	txs         types.Transactions
	rctNodes    []*ipld.EthReceipt
	txNodes     []*ipld.EthTx
	logNodes    [][]*ipld.EthLog
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdi *StateDiffIndexer) processReceiptsAndTxs(tx *BatchTx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		txNode := args.txNodes[i]
		tx.cacheIPLD(txNode)
		tx.cacheIPLD(args.rctNodes[i])

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
			BlockNumber: args.blockNumber.String(),
			HeaderID:    args.headerID,
			Dst:         shared.HandleZeroAddrPointer(trx.To()),
			Src:         shared.HandleZeroAddr(from),
			TxHash:      txID,
			Index:       int64(i),
			CID:         txNode.Cid().String(),
			Type:        trx.Type(),
			Value:       val,
		}
		if err := sdi.dbWriter.upsertTransactionCID(tx.dbtx, txModel); err != nil {
			return err
		}

		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)

		rctModel := &models.ReceiptModel{
			BlockNumber: args.blockNumber.String(),
			HeaderID:    args.headerID,
			TxID:        txID,
			Contract:    contract,
			CID:         args.rctNodes[i].Cid().String(),
		}
		if len(receipt.PostState) == 0 {
			rctModel.PostStatus = receipt.Status
		} else {
			rctModel.PostState = common.BytesToHash(receipt.PostState).String()
		}

		if err := sdi.dbWriter.upsertReceiptCID(tx.dbtx, rctModel); err != nil {
			return err
		}

		// index logs
		logDataSet := make([]*models.LogsModel, len(receipt.Logs))
		for idx, l := range receipt.Logs {
			tx.cacheIPLD(args.logNodes[i][idx])
			topicSet := make([]string, 4)
			for ti, topic := range l.Topics {
				topicSet[ti] = topic.Hex()
			}

			logDataSet[idx] = &models.LogsModel{
				BlockNumber: args.blockNumber.String(),
				HeaderID:    args.headerID,
				ReceiptID:   txID,
				Address:     l.Address.String(),
				Index:       int64(l.Index),
				CID:         args.logNodes[i][idx].Cid().String(),
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

	return nil
}

// PushStateNode publishes and indexes a state diff node object (including any child storage nodes) in the IPLD sql
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateLeafNode, headerID string) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	var stateModel models.StateNodeModel
	if stateNode.Removed {
		tx.cacheRemoved(shared.RemovedNodeStateCID, []byte{})
		stateModel = models.StateNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			StateKey:    common.BytesToHash(stateNode.AccountWrapper.LeafKey).String(),
			CID:         shared.RemovedNodeStateCID,
			Removed:     true,
		}
	} else {
		stateModel = models.StateNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			StateKey:    common.BytesToHash(stateNode.AccountWrapper.LeafKey).String(),
			CID:         stateNode.AccountWrapper.CID,
			Removed:     false,
			Balance:     stateNode.AccountWrapper.Account.Balance.String(),
			Nonce:       stateNode.AccountWrapper.Account.Nonce,
			CodeHash:    common.BytesToHash(stateNode.AccountWrapper.Account.CodeHash).String(),
			StorageRoot: stateNode.AccountWrapper.Account.Root.String(),
		}
	}

	// index the state node
	if err := sdi.dbWriter.upsertStateCID(tx.dbtx, stateModel); err != nil {
		return err
	}

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageDiff {
		if storageNode.Removed {
			tx.cacheRemoved(shared.RemovedNodeStorageCID, []byte{})
			storageModel := models.StorageNodeModel{
				BlockNumber: tx.BlockNumber,
				HeaderID:    headerID,
				StateKey:    common.BytesToHash(stateNode.AccountWrapper.LeafKey).String(),
				StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
				CID:         shared.RemovedNodeStorageCID,
				Removed:     true,
				Value:       []byte{},
			}
			if err := sdi.dbWriter.upsertStorageCID(tx.dbtx, storageModel); err != nil {
				return err
			}
			continue
		}
		storageModel := models.StorageNodeModel{
			BlockNumber: tx.BlockNumber,
			HeaderID:    headerID,
			StateKey:    common.BytesToHash(stateNode.AccountWrapper.LeafKey).String(),
			StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
			CID:         storageNode.CID,
			Removed:     false,
			Value:       storageNode.Value,
		}
		if err := sdi.dbWriter.upsertStorageCID(tx.dbtx, storageModel); err != nil {
			return err
		}
	}

	return nil
}

// PushIPLD publishes iplds to ipld.blocks
func (sdi *StateDiffIndexer) PushIPLD(batch interfaces.Batch, ipld sdtypes.IPLD) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	tx.cacheDirect(ipld.CID, ipld.Content)
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
func (sdi *StateDiffIndexer) InsertWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) (err error) {
	tx := NewDelayedTx(sdi.dbWriter.db)
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
func (sdi *StateDiffIndexer) RemoveWatchedAddresses(args []sdtypes.WatchAddressArg) (err error) {
	tx := NewDelayedTx(sdi.dbWriter.db)
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
func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) (err error) {
	tx := NewDelayedTx(sdi.dbWriter.db)
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
