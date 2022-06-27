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
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
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

const defaultOutputDir = "./statediff_output"
const defaultFilePath = "./statediff.sql"
const defaultWatchedAddressesFilePath = "./statediff-watched-addresses.csv"

const watchedAddressesInsert = "INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at) VALUES ('%s', '%d', '%d') ON CONFLICT (address) DO NOTHING;"

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
)

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
	if watchedAddressesFilePath == "" {
		watchedAddressesFilePath = defaultWatchedAddressesFilePath
	}
	log.Info("Writing watched addresses to file", "file", watchedAddressesFilePath)

	switch config.Mode {
	case CSV:
		outputDir := config.OutputDir
		if outputDir == "" {
			outputDir = defaultOutputDir
		}

		if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("cannot create output directory, directory (%s) already exists", outputDir)
		}

		log.Info("Writing statediff CSV files to directory", "file", outputDir)

		writer, err = NewCSVWriter(outputDir, watchedAddressesFilePath)
		if err != nil {
			return nil, err
		}
	case SQL:
		filePath := config.FilePath
		if filePath == "" {
			filePath = defaultFilePath
		}
		if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("cannot create file, file (%s) already exists", filePath)
		}
		file, err := os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
		}
		log.Info("Writing statediff SQL statements to file", "file", filePath)

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

	blockTx := &BatchTx{
		BlockNumber: block.Number().String(),
		submit: func(self *BatchTx, err error) error {
			tDiff := time.Since(t)
			indexerMetrics.tStateStoreCodeProcessing.Update(tDiff)
			traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
			t = time.Now()
			sdi.fileWriter.Flush()
			tDiff = time.Since(t)
			indexerMetrics.tPostgresCommit.Update(tDiff)
			traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	tDiff := time.Since(t)
	indexerMetrics.tFreePostgres.Update(tDiff)
	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// write header, collect headerID
	headerID := sdi.processHeader(block.Header(), headerNode, reward, totalDifficulty)
	tDiff = time.Since(t)
	indexerMetrics.tHeaderProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()

	// write uncles
	sdi.processUncles(headerID, block.Number(), uncleNodes)
	tDiff = time.Since(t)
	indexerMetrics.tUncleProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()

	// write receipts and txs
	err = sdi.processReceiptsAndTxs(processArgs{
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

// processHeader write a header IPLD insert SQL stmt to a file
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(header *types.Header, headerNode node.Node, reward, td *big.Int) string {
	sdi.fileWriter.upsertIPLDNode(header.Number.String(), headerNode)

	var baseFee *string
	if header.BaseFee != nil {
		baseFee = new(string)
		*baseFee = header.BaseFee.String()
	}
	headerID := header.Hash().String()
	sdi.fileWriter.upsertHeaderCID(models.HeaderModel{
		NodeID:          sdi.nodeID,
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
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
	return headerID
}

// processUncles writes uncle IPLD insert SQL stmts to a file
func (sdi *StateDiffIndexer) processUncles(headerID string, blockNumber *big.Int, uncleNodes []*ipld2.EthHeader) {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		sdi.fileWriter.upsertIPLDNode(blockNumber.String(), uncleNode)
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber.Uint64(), uncleNode.Number.Uint64())
		}
		sdi.fileWriter.upsertUncleCID(models.UncleModel{
			BlockNumber: blockNumber.String(),
			HeaderID:    headerID,
			CID:         uncleNode.Cid().String(),
			MhKey:       shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash:  uncleNode.ParentHash.String(),
			BlockHash:   uncleNode.Hash().String(),
			Reward:      uncleReward.String(),
		})
	}
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

// processReceiptsAndTxs writes receipt and tx IPLD insert SQL stmts to a file
func (sdi *StateDiffIndexer) processReceiptsAndTxs(args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		for _, logTrieNode := range args.logTrieNodes[i] {
			sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), logTrieNode)
		}
		txNode := args.txNodes[i]
		sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), txNode)

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
			Data:        trx.Data(),
			CID:         txNode.Cid().String(),
			MhKey:       shared.MultihashKeyFromCID(txNode.Cid()),
			Type:        trx.Type(),
			Value:       val,
		}
		sdi.fileWriter.upsertTransactionCID(txModel)

		// index access list if this is one
		for j, accessListElement := range trx.AccessList() {
			storageKeys := make([]string, len(accessListElement.StorageKeys))
			for k, storageKey := range accessListElement.StorageKeys {
				storageKeys[k] = storageKey.Hex()
			}
			accessListElementModel := models.AccessListElementModel{
				BlockNumber: args.blockNumber.String(),
				TxID:        txID,
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}
			sdi.fileWriter.upsertAccessListElement(accessListElementModel)
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
			BlockNumber:  args.blockNumber.String(),
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
		sdi.fileWriter.upsertReceiptCID(rctModel)

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
				BlockNumber: args.blockNumber.String(),
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
		sdi.fileWriter.upsertLogCID(logDataSet)
	}

	// publish trie nodes, these aren't indexed directly
	for i, n := range args.txTrieNodes {
		sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), n)
		sdi.fileWriter.upsertIPLDNode(args.blockNumber.String(), args.rctTrieNodes[i])
	}

	return nil
}

// PushStateNode writes a state diff node object (including any child storage nodes) IPLD insert SQL stmt to a file
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateNode, headerID string) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("file: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	var stateModel models.StateNodeModel
	if stateNode.NodeType == sdtypes.Removed {
		if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
			atomic.StoreUint32(sdi.removedCacheFlag, 1)
			sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, shared.RemovedNodeMhKey, []byte{})
		}
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
		stateCIDStr, stateMhKey, err := sdi.fileWriter.upsertIPLDRaw(tx.BlockNumber, ipld2.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
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
	sdi.fileWriter.upsertStateCID(stateModel)

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
		sdi.fileWriter.upsertStateAccount(accountModel)
	}

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		if storageNode.NodeType == sdtypes.Removed {
			if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
				atomic.StoreUint32(sdi.removedCacheFlag, 1)
				sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, shared.RemovedNodeMhKey, []byte{})
			}
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
			sdi.fileWriter.upsertStorageCID(storageModel)
			continue
		}
		storageCIDStr, storageMhKey, err := sdi.fileWriter.upsertIPLDRaw(tx.BlockNumber, ipld2.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
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
		sdi.fileWriter.upsertStorageCID(storageModel)
	}

	return nil
}

// PushCodeAndCodeHash writes code and codehash pairs insert SQL stmts to a file
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(batch interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("file: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// codec doesn't matter since db key is multihash-based
	mhKey, err := shared.MultihashKeyFromKeccak256(codeAndCodeHash.Hash)
	if err != nil {
		return fmt.Errorf("error deriving multihash key from codehash: %v", err)
	}
	sdi.fileWriter.upsertIPLDDirect(tx.BlockNumber, mhKey, codeAndCodeHash.Code)
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
