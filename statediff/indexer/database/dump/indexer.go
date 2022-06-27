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

package dump

import (
	"fmt"
	"io"
	"math/big"
	"time"

	ipld2 "github.com/ethereum/go-ethereum/statediff/indexer/ipld"

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
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
)

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of a void
type StateDiffIndexer struct {
	dump        io.WriteCloser
	chainConfig *params.ChainConfig
}

// NewStateDiffIndexer creates a void implementation of interfaces.StateDiffIndexer
func NewStateDiffIndexer(chainConfig *params.ChainConfig, config Config) *StateDiffIndexer {
	return &StateDiffIndexer{
		dump:        config.Dump,
		chainConfig: chainConfig,
	}
}

// ReportDBMetrics has nothing to report for dump
func (sdi *StateDiffIndexer) ReportDBMetrics(time.Duration, <-chan bool) {}

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
		dump:        sdi.dump,
		iplds:       make(chan models.IPLDModel),
		quit:        make(chan struct{}),
		ipldCache:   models.IPLDBatch{},
		submit: func(self *BatchTx, err error) error {
			close(self.quit)
			close(self.iplds)
			tDiff := time.Since(t)
			indexerMetrics.tStateStoreCodeProcessing.Update(tDiff)
			traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
			t = time.Now()
			if err := self.flush(); err != nil {
				traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
				log.Debug(traceMsg)
				return err
			}
			tDiff = time.Since(t)
			indexerMetrics.tPostgresCommit.Update(tDiff)
			traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
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
	err = sdi.processUncles(blockTx, headerID, block.Number(), uncleNodes)
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

	headerID := header.Hash().String()
	mod := models.HeaderModel{
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
	}
	_, err := fmt.Fprintf(sdi.dump, "%+v\r\n", mod)
	return headerID, err
}

// processUncles publishes and indexes uncle IPLDs in Postgres
func (sdi *StateDiffIndexer) processUncles(tx *BatchTx, headerID string, blockNumber *big.Int, uncleNodes []*ipld2.EthHeader) error {
	// publish and index uncles
	if len(uncleNodes) <= 0 {
		return nil
	}

	// RLP encode the array
	uncleNodesRlp, err := rlp.EncodeToBytes(uncleNodes)
	if err != nil {
		return err
	}

	// Calculate total block rewards
	var uncleReward *big.Int = big.NewInt(0)
	for _, uncleNode := range uncleNodes {
		tx.cacheIPLD(uncleNode) // Not sure if this is needed....
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique == nil {
			uncleReward = uncleReward.Add(shared.CalcUncleMinerReward(blockNumber.Uint64(), uncleNode.Number.Uint64()), uncleReward)
		}
	}

	// Create a CID and write the uncle list.
	uncleCid, err := ipld2.RawdataToCid(ipld2.MEthHeaderList, uncleNodesRlp, multihash.KECCAK_256)
	if err != nil {
		return err
	}

	uncle := models.UncleModel{
		BlockNumber: blockNumber.String(),
		HeaderID:    headerID,
		CID:         uncleCid.String(),
		MhKey:       shared.MultihashKeyFromCID(uncleCid),
		ParentHash:  uncleNodes[0].ParentHash.String(),
		// I don't think we want this field anymore, since we no longer have a single BlockHash.
		BlockHash: uncleNodes[0].Hash().String(), // How do we calculate the blockHash since we are no longer using a single header? rlpHash?
		Reward:    uncleReward.String(),
	}
	if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", uncle); err != nil {
		return err
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

		// Indexing
		// index tx
		trx := args.txs[i]
		trxID := trx.Hash().String()

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
			TxHash:      trxID,
			Index:       int64(i),
			Data:        trx.Data(),
			CID:         txNode.Cid().String(),
			MhKey:       shared.MultihashKeyFromCID(txNode.Cid()),
			Type:        trx.Type(),
			Value:       val,
		}
		if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", txModel); err != nil {
			return err
		}

		// index access list if this is one
		for j, accessListElement := range trx.AccessList() {
			storageKeys := make([]string, len(accessListElement.StorageKeys))
			for k, storageKey := range accessListElement.StorageKeys {
				storageKeys[k] = storageKey.Hex()
			}
			accessListElementModel := models.AccessListElementModel{
				BlockNumber: args.blockNumber.String(),
				TxID:        trxID,
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}
			if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", accessListElementModel); err != nil {
				return err
			}
		}

		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		if contract != "" {
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}

		// index the receipt
		if !args.rctLeafNodeCIDs[i].Defined() {
			return fmt.Errorf("invalid receipt leaf node cid")
		}

		rctModel := &models.ReceiptModel{
			BlockNumber:  args.blockNumber.String(),
			TxID:         trxID,
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

		if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", rctModel); err != nil {
			return err
		}

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
				ReceiptID:   trxID,
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

		if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", logDataSet); err != nil {
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
		return fmt.Errorf("dump: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	var stateModel models.StateNodeModel
	if stateNode.NodeType == sdtypes.Removed {
		// short circuit if it is a Removed node
		// this assumes the db has been initialized and a public.blocks entry for the Removed node is present
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

	// index the state node, collect the stateID to reference by FK
	if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", stateModel); err != nil {
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
		if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", accountModel); err != nil {
			return err
		}
	}

	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		if storageNode.NodeType == sdtypes.Removed {
			// short circuit if it is a Removed node
			// this assumes the db has been initialized and a public.blocks entry for the Removed node is present
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
			if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", storageModel); err != nil {
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
		if _, err := fmt.Fprintf(sdi.dump, "%+v\r\n", storageModel); err != nil {
			return err
		}
	}

	return nil
}

// PushCodeAndCodeHash publishes code and codehash pairs to the ipld sql
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(batch interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("dump: batch is expected to be of type %T, got %T", &BatchTx{}, batch)
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
	return sdi.dump.Close()
}

// LoadWatchedAddresses satisfies the interfaces.StateDiffIndexer interface
func (sdi *StateDiffIndexer) LoadWatchedAddresses() ([]common.Address, error) {
	return nil, nil
}

// InsertWatchedAddresses satisfies the interfaces.StateDiffIndexer interface
func (sdi *StateDiffIndexer) InsertWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	return nil
}

// RemoveWatchedAddresses satisfies the interfaces.StateDiffIndexer interface
func (sdi *StateDiffIndexer) RemoveWatchedAddresses(args []sdtypes.WatchAddressArg) error {
	return nil
}

// SetWatchedAddresses satisfies the interfaces.StateDiffIndexer interface
func (sdi *StateDiffIndexer) SetWatchedAddresses(args []sdtypes.WatchAddressArg, currentBlockNumber *big.Int) error {
	return nil
}

// ClearWatchedAddresses satisfies the interfaces.StateDiffIndexer interface
func (sdi *StateDiffIndexer) ClearWatchedAddresses() error {
	return nil
}
