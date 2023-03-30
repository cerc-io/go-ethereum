// VulcanizeDB
// Copyright © 2021 Vulcanize

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

const defaultCSVOutputDir = "./statediff_output"
const defaultSQLFilePath = "./statediff.sql"
const defaultWatchedAddressesCSVFilePath = "./statediff-watched-addresses.csv"
const defaultWatchedAddressesSQLFilePath = "./statediff-watched-addresses.sql"

const watchedAddressesInsert = "INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at) VALUES ('%s', '%d', '%d') ON CONFLICT (address) DO NOTHING;"

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of a void
type StateDiffIndexer struct {
	fileWriter       FileWriter
	chainConfig      *params.ChainConfig
	nodeID           string
	wg               *sync.WaitGroup
	removedCacheFlag *uint32
}

// NewStateDiffIndexer creates a void implementation of interfaces.StateDiffIndexer
func NewStateDiffIndexer(ctx context.Context, chainConfig *params.ChainConfig, config Config) (*StateDiffIndexer, error) {
	var err error
	var writer FileWriter

	watchedAddressesFilePath := config.WatchedAddressesFilePath

	switch config.Mode {
	case CSV:
		outputDir := config.OutputDir
		if outputDir == "" {
			outputDir = defaultCSVOutputDir
		}

		if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("cannot create output directory, directory (%s) already exists", outputDir)
		}
		log.Info("Writing statediff CSV files to directory", "file", outputDir)

		if watchedAddressesFilePath == "" {
			watchedAddressesFilePath = defaultWatchedAddressesCSVFilePath
		}
		log.Info("Writing watched addresses to file", "file", watchedAddressesFilePath)

		writer, err = NewCSVWriter(outputDir, watchedAddressesFilePath)
		if err != nil {
			return nil, err
		}
	case SQL:
		filePath := config.FilePath
		if filePath == "" {
			filePath = defaultSQLFilePath
		}
		if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("cannot create file, file (%s) already exists", filePath)
		}
		file, err := os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
		}
		log.Info("Writing statediff SQL statements to file", "file", filePath)

		if watchedAddressesFilePath == "" {
			watchedAddressesFilePath = defaultWatchedAddressesSQLFilePath
		}
		log.Info("Writing watched addresses to file", "file", watchedAddressesFilePath)

		writer = NewSQLWriter(file, watchedAddressesFilePath)
	default:
		return nil, fmt.Errorf("unrecognized file mode: %s", config.Mode)
	}

	wg := new(sync.WaitGroup)
	writer.Loop()
	writer.upsertNode(config.NodeInfo)

	return &StateDiffIndexer{
		fileWriter:  writer,
		chainConfig: chainConfig,
		nodeID:      config.NodeInfo.ID,
		wg:          wg,
	}, nil
}

// ReportDBMetrics has nothing to report for dump
func (sdi *StateDiffIndexer) ReportDBMetrics(time.Duration, <-chan bool) {}

// PushBlock pushes and indexes block data in sql, except state & storage nodes (includes header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (interfaces.Batch, error) {
	sdi.removedCacheFlag = new(uint32)
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

	blockTx := &BatchTx{
		BlockNumber: block.Number().String(),
		submit: func(self *BatchTx, err error) error {
			tDiff := time.Since(t)
			metrics.IndexerMetrics.StateStoreCodeProcessingTimer.Update(tDiff)
			traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
			t = time.Now()
			sdi.fileWriter.Flush()
			tDiff = time.Since(t)
			metrics.IndexerMetrics.PostgresCommitTimer.Update(tDiff)
			traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	tDiff := time.Since(t)
	metrics.IndexerMetrics.FreePostgresTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// write header, collect headerID
	headerID := sdi.processHeader(block.Header(), headerNode, reward, totalDifficulty)
	tDiff = time.Since(t)
	metrics.IndexerMetrics.HeaderProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()

	// write uncles
	sdi.processUncles(headerID, block.Number(), block.UncleHash(), block.Uncles())
	tDiff = time.Since(t)
	metrics.IndexerMetrics.UncleProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()

	// write receipts and txs
	err = sdi.processReceiptsAndTxs(processArgs{
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
	metrics.IndexerMetrics.TxAndRecProcessingTimer.Update(tDiff)
	traceMsg += fmt.Sprintf("tx and receipt processing time: %s\r\n", tDiff.String())
	t = time.Now()

	return blockTx, err
}

// processHeader write a header IPLD insert SQL stmt to a file
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(header *types.Header, headerNode ipld.IPLD, reward, td *big.Int) string {
	sdi.fileWriter.upsertIPLDNode(header.Number.String(), headerNode)

	var baseFee *string
	if header.BaseFee != nil {
		baseFee = new(string)
		*baseFee = header.BaseFee.String()
	}
	headerID := header.Hash().String()
	sdi.fileWriter.upsertHeaderCID(models.HeaderModel{
		NodeIDs:         pq.StringArray([]string{sdi.nodeID}),
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
	return headerID
}

// processUncles publishes and indexes uncle IPLDs in Postgres
func (sdi *StateDiffIndexer) processUncles(headerID string, blockNumber *big.Int, unclesHash common.Hash, uncles []*types.Header) error {
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
	sdi.fileWriter.upsertIPLDDirect(blockNumber.String(), unclesCID.String(), uncleEncoding)
	for i, uncle := range uncles {
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber.Uint64(), uncle.Number.Uint64())
		}
		sdi.fileWriter.upsertUncleCID(models.UncleModel{
			BlockNumber: blockNumber.String(),
			HeaderID:    headerID,
			CID:         unclesCID.String(),
			ParentHash:  uncle.ParentHash.String(),
			BlockHash:   uncle.Hash().String(),
			Reward:      uncleReward.String(),
			Index:       int64(i),
		})
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

// processReceiptsAndTxs writes receipt and tx IPLD insert SQL stmts to a file
func (sdi *StateDiffIndexer) processReceiptsAndTxs(args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		txNode := args.txNodes[i]
		sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), txNode)
		sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), args.rctNodes[i])

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
		sdi.fileWriter.upsertTransactionCID(txModel)

		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)

		// index receipt
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
		sdi.fileWriter.upsertReceiptCID(rctModel)

		// index logs
		logDataSet := make([]*models.LogsModel, len(receipt.Logs))
		for idx, l := range receipt.Logs {
			sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), args.logNodes[i][idx])
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
		sdi.fileWriter.upsertLogCID(logDataSet)
	}

	return nil
}

// PushStateNode writes a state diff node object (including any child storage nodes) IPLD insert SQL stmt to a file
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateLeafNode, headerID string) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("file: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	var stateModel models.StateNodeModel
	if stateNode.Removed {
		if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
			atomic.StoreUint32(sdi.removedCacheFlag, 1)
			sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, shared.RemovedNodeStateCID, []byte{})
		}
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
	sdi.fileWriter.upsertStateCID(stateModel)

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageDiff {
		if storageNode.Removed {
			if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
				atomic.StoreUint32(sdi.removedCacheFlag, 1)
				sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, shared.RemovedNodeStorageCID, []byte{})
			}
			storageModel := models.StorageNodeModel{
				BlockNumber: tx.BlockNumber,
				HeaderID:    headerID,
				StateKey:    common.BytesToHash(stateNode.AccountWrapper.LeafKey).String(),
				StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
				CID:         shared.RemovedNodeStorageCID,
				Removed:     true,
				Value:       []byte{},
			}
			sdi.fileWriter.upsertStorageCID(storageModel)
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
		sdi.fileWriter.upsertStorageCID(storageModel)
	}

	return nil
}

// PushIPLD writes iplds to ipld.blocks
func (sdi *StateDiffIndexer) PushIPLD(batch interfaces.Batch, ipld sdtypes.IPLD) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("file: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, ipld.CID, ipld.Content)
	return nil
}

// Close satisfies io.Closer
func (sdi *StateDiffIndexer) Close() error {
	return sdi.fileWriter.Close()
}

// LoadWatchedAddresses loads watched addresses from a file
func (sdi *StateDiffIndexer) LoadWatchedAddresses() ([]common.Address, error) {
	return sdi.fileWriter.loadWatchedAddresses()
}

// InsertWatchedAddresses inserts the given addresses in a file
func (sdi *StateDiffIndexer) InsertWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	return sdi.fileWriter.insertWatchedAddresses(args, currentBlockNumber)
}

// RemoveWatchedAddresses removes the given watched addresses from a file
func (sdi *StateDiffIndexer) RemoveWatchedAddresses(args []sdtypes.WatchAddressArg) error {
	return sdi.fileWriter.removeWatchedAddresses(args)
}

// SetWatchedAddresses clears and inserts the given addresses in a file
func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	return sdi.fileWriter.setWatchedAddresses(args, currentBlockNumber)
}

// ClearWatchedAddresses clears all the watched addresses from a file
func (sdi *StateDiffIndexer) ClearWatchedAddresses() error {
	return sdi.SetWatchedAddresses([]sdtypes.WatchAddressArg{}, big.NewInt(0))
}
