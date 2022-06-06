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
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	node "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/thoas/go-funk"

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

const defaultFilePath = "./statediff.sql"
const defaultWatchedAddressesFilePath = "./statediff-watched-addresses.sql"

const watchedAddressesInsert = "INSERT INTO eth_meta.watched_addresses (address, created_at, watched_at) VALUES ('%s', '%d', '%d') ON CONFLICT (address) DO NOTHING;"

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
)

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of a void
type StateDiffIndexer struct {
	fileWriter       *SQLWriter
	chainConfig      *params.ChainConfig
	nodeID           string
	wg               *sync.WaitGroup
	blockNumber      string
	removedCacheFlag *uint32

	watchedAddressesFilePath string
}

// NewStateDiffIndexer creates a void implementation of interfaces.StateDiffIndexer
func NewStateDiffIndexer(ctx context.Context, chainConfig *params.ChainConfig, config Config) (*StateDiffIndexer, error) {
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

	watchedAddressesFilePath := config.WatchedAddressesFilePath
	if watchedAddressesFilePath == "" {
		watchedAddressesFilePath = defaultWatchedAddressesFilePath
	}
	log.Info("Writing watched addresses SQL statements to file", "file", watchedAddressesFilePath)

	w := NewSQLWriter(file)
	wg := new(sync.WaitGroup)
	w.Loop()
	w.upsertNode(config.NodeInfo)
	return &StateDiffIndexer{
		fileWriter:               w,
		chainConfig:              chainConfig,
		nodeID:                   config.NodeInfo.ID,
		wg:                       wg,
		watchedAddressesFilePath: watchedAddressesFilePath,
	}, nil
}

// ReportDBMetrics has nothing to report for dump
func (sdi *StateDiffIndexer) ReportDBMetrics(time.Duration, <-chan bool) {}

// PushBlock pushes and indexes block data in sql, except state & storage nodes (includes header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (interfaces.Batch, error) {
	sdi.removedCacheFlag = new(uint32)
	sdi.blockNumber = block.Number().String()
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
	headerNode, uncleNodes, txNodes, rctNodes, logNodes, err := ipld2.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, fmt.Errorf("error creating IPLD nodes from block and receipts: %v", err)
	}

	if len(txNodes) != len(rctNodes) {
		return nil, fmt.Errorf("expected number of transactions (%d) and receipts (%d) to be equal", len(txNodes), len(rctNodes))
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
		BlockNumber: sdi.blockNumber,
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
	sdi.processUncles(headerID, height, uncleNodes)
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
		txNodes:         txNodes,
		logNodes:    logNodes,
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
	sdi.fileWriter.upsertIPLDNode(sdi.blockNumber, headerNode)

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
	return headerID
}

// processUncles writes uncle IPLD insert SQL stmts to a file
func (sdi *StateDiffIndexer) processUncles(headerID string, blockNumber uint64, uncleNodes []*ipld2.EthHeader) {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		sdi.fileWriter.upsertIPLDNode(sdi.blockNumber, uncleNode)
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber, uncleNode.Number.Uint64())
		}
		sdi.fileWriter.upsertUncleCID(models.UncleModel{
			BlockNumber: sdi.blockNumber,
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
	txNodes         []*ipld2.EthTx
	logNodes        [][]*ipld2.EthLog
}

// processReceiptsAndTxs writes receipt and tx IPLD insert SQL stmts to a file
func (sdi *StateDiffIndexer) processReceiptsAndTxs(args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		txNode := args.txNodes[i]
		sdi.fileWriter.upsertIPLDNode(sdi.blockNumber, txNode)

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
		txCid := txNode.Cid()
		txModel := models.TxModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    args.headerID,
			Dst:         shared.HandleZeroAddrPointer(trx.To()),
			Src:         shared.HandleZeroAddr(from),
			TxHash:      txID,
			Index:       int64(i),
			Data:        trx.Data(),
			CID:         txCid.String(),
			MhKey:       shared.MultihashKeyFromCID(txCid),
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
				BlockNumber: sdi.blockNumber,
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
		rctCid := args.rctNodes[i].Cid()
		rctModel := &models.ReceiptModel{
			BlockNumber:  sdi.blockNumber,
			TxID:         txID,
			Contract:     contract,
			ContractHash: contractHash,
			CID:      rctCid.String(),
			MhKey:    shared.MultihashKeyFromCID(rctCid),
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
			logCid := args.logNodes[i][idx].Cid()
			logDataSet[idx] = &models.LogsModel{
				BlockNumber: sdi.blockNumber,
				ReceiptID:   txID,
				Address:     l.Address.String(),
				Index:       int64(l.Index),
				Data:        l.Data,
				CID:     logCid.String(),
				MhKey:   shared.MultihashKeyFromCID(logCid),
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
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateNode, headerID string) error {
	// publish the state node
	var stateModel models.StateLeafModel
	if stateNode.NodeType == sdtypes.Removed {
		// TODO: still need to handle removed leaves
		if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
			atomic.StoreUint32(sdi.removedCacheFlag, 1)
			sdi.fileWriter.upsertIPLDDirect(sdi.blockNumber, shared.RemovedNodeMhKey, []byte{})
		}
		stateModel = models.StateLeafModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    headerID,
			Path:        stateNode.Path,
			StateKey:    common.BytesToHash(stateNode.LeafKey).String(),
			CID:         shared.RemovedNodeStateCID,
			MhKey:       shared.RemovedNodeMhKey,
		}
	} else {
		stateCIDStr, stateMhKey, err := sdi.fileWriter.upsertIPLDRaw(sdi.blockNumber, ipld2.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error generating and cacheing state node IPLD: %v", err)
		}
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
		stateModel = models.StateLeafModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    headerID,
			Path:        stateNode.Path,
			StateKey:    common.BytesToHash(stateNode.LeafKey).String(),
			CID:         stateCIDStr,
			MhKey:       stateMhKey,
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}
	}

	// index the state node
	sdi.fileWriter.upsertStateCID(stateModel)

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		if storageNode.NodeType == sdtypes.Removed {
			// TODO: still need to handle leaf deletions
			if atomic.LoadUint32(sdi.removedCacheFlag) == 0 {
				atomic.StoreUint32(sdi.removedCacheFlag, 1)
				sdi.fileWriter.upsertIPLDDirect(sdi.blockNumber, shared.RemovedNodeMhKey, []byte{})
			}
			storageModel := models.StorageLeafModel{
				BlockNumber: sdi.blockNumber,
				HeaderID:    headerID,
				StatePath:   stateNode.Path,
				Path:        storageNode.Path,
				StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
				CID:         shared.RemovedNodeStorageCID,
				MhKey:       shared.RemovedNodeMhKey,
			}
			sdi.fileWriter.upsertStorageCID(storageModel)
			continue
		}
		storageCIDStr, storageMhKey, err := sdi.fileWriter.upsertIPLDRaw(sdi.blockNumber, ipld2.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error generating and cacheing storage node IPLD: %v", err)
		}
		storageModel := models.StorageLeafModel{
			BlockNumber: sdi.blockNumber,
			HeaderID:    headerID,
			StatePath:   stateNode.Path,
			Path:        storageNode.Path,
			StorageKey:  common.BytesToHash(storageNode.LeafKey).String(),
			CID:         storageCIDStr,
			MhKey:       storageMhKey,
		}
		sdi.fileWriter.upsertStorageCID(storageModel)
	}

	return nil
}

// PushCodeAndCodeHash writes code and codehash pairs insert SQL stmts to a file
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(batch interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	// codec doesn't matter since db key is multihash-based
	mhKey, err := shared.MultihashKeyFromKeccak256(codeAndCodeHash.Hash)
	if err != nil {
		return fmt.Errorf("error deriving multihash key from codehash: %v", err)
	}
	sdi.fileWriter.upsertIPLDDirect(sdi.blockNumber, mhKey, codeAndCodeHash.Code)
	return nil
}

// Close satisfies io.Closer
func (sdi *StateDiffIndexer) Close() error {
	return sdi.fileWriter.Close()
}

// LoadWatchedAddresses loads watched addresses from a file
func (sdi *StateDiffIndexer) LoadWatchedAddresses() ([]common.Address, error) {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sdi.watchedAddressesFilePath)
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
func (sdi *StateDiffIndexer) InsertWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sdi.watchedAddressesFilePath)
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

	return dumpWatchedAddressesStatements(sdi.watchedAddressesFilePath, stmts)
}

// RemoveWatchedAddresses removes the given watched addresses from a file
func (sdi *StateDiffIndexer) RemoveWatchedAddresses(args []sdtypes.WatchAddressArg) error {
	// load sql statements from watched addresses file
	stmts, err := loadWatchedAddressesStatements(sdi.watchedAddressesFilePath)
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

		toRemove := funk.Contains(args, func(arg sdtypes.WatchAddressArg) bool {
			return arg.Address == addressString
		})

		if !toRemove {
			filteredStmts = append(filteredStmts, stmt)
		}
	}

	return dumpWatchedAddressesStatements(sdi.watchedAddressesFilePath, filteredStmts)
}

// SetWatchedAddresses clears and inserts the given addresses in a file
func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	var stmts []string
	for _, arg := range args {
		stmt := fmt.Sprintf(watchedAddressesInsert, arg.Address, arg.CreatedAt, currentBlockNumber.Uint64())
		stmts = append(stmts, stmt)
	}

	return dumpWatchedAddressesStatements(sdi.watchedAddressesFilePath, stmts)
}

// ClearWatchedAddresses clears all the watched addresses from a file
func (sdi *StateDiffIndexer) ClearWatchedAddresses() error {
	return sdi.SetWatchedAddresses([]sdtypes.WatchAddressArg{}, big.NewInt(0))
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
