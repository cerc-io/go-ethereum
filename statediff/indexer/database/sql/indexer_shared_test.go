package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

var (
	db        sql.Database
	err       error
	ind       interfaces.StateDiffIndexer
	ipfsPgGet = `SELECT data FROM public.blocks
					WHERE key = $1 AND block_number = $2`
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5                             []byte
	nonCanonicalRct1, nonCanonicalRct2                                                []byte
	mockBlock, mockNonCanonicalBlock                                                  *types.Block
	headerCID, mockNonCanonicalHeaderCID, trx1CID, trx2CID, trx3CID, trx4CID, trx5CID cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                                       cid.Cid
	nonCanonicalRct1CID, nonCanonicalRct2CID                                          cid.Cid
	rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5                                  []byte
	nonCanonicalRctLeaf1, nonCanonicalRctLeaf2                                        []byte
	state1CID, state2CID, storageCID                                                  cid.Cid
	contract1Address, contract2Address, contract3Address, contract4Address            string
	contract1CreatedAt, contract2CreatedAt, contract3CreatedAt, contract4CreatedAt    uint64
	lastFilledAt, watchedAt1, watchedAt2, watchedAt3                                  uint64
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}

	mockBlock = mocks.MockBlock
	txs, rcts := mocks.MockBlock.Transactions(), mocks.MockReceipts

	mockNonCanonicalBlock = mocks.MockNonCanonicalBlock
	nonCanonicalBlockRcts := mocks.MockNonCanonicalBlockReceipts

	buf := new(bytes.Buffer)
	txs.EncodeIndex(0, buf)
	tx1 = make([]byte, buf.Len())
	copy(tx1, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(1, buf)
	tx2 = make([]byte, buf.Len())
	copy(tx2, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(2, buf)
	tx3 = make([]byte, buf.Len())
	copy(tx3, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(3, buf)
	tx4 = make([]byte, buf.Len())
	copy(tx4, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(4, buf)
	tx5 = make([]byte, buf.Len())
	copy(tx5, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(0, buf)
	rct1 = make([]byte, buf.Len())
	copy(rct1, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(1, buf)
	rct2 = make([]byte, buf.Len())
	copy(rct2, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(2, buf)
	rct3 = make([]byte, buf.Len())
	copy(rct3, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(3, buf)
	rct4 = make([]byte, buf.Len())
	copy(rct4, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(4, buf)
	rct5 = make([]byte, buf.Len())
	copy(rct5, buf.Bytes())
	buf.Reset()

	nonCanonicalBlockRcts.EncodeIndex(0, buf)
	nonCanonicalRct1 = make([]byte, buf.Len())
	copy(nonCanonicalRct1, buf.Bytes())
	buf.Reset()

	nonCanonicalBlockRcts.EncodeIndex(1, buf)
	nonCanonicalRct2 = make([]byte, buf.Len())
	copy(nonCanonicalRct2, buf.Bytes())
	buf.Reset()

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
	mockNonCanonicalHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockNonCanonicalHeaderRlp, multihash.KECCAK_256)
	trx1CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx1, multihash.KECCAK_256)
	trx2CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx2, multihash.KECCAK_256)
	trx3CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx3, multihash.KECCAK_256)
	trx4CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx4, multihash.KECCAK_256)
	trx5CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)

	receiptTrie := ipld.NewRctTrie()

	receiptTrie.Add(0, rct1)
	receiptTrie.Add(1, rct2)
	receiptTrie.Add(2, rct3)
	receiptTrie.Add(3, rct4)
	receiptTrie.Add(4, rct5)

	rctLeafNodes, keys, _ := receiptTrie.GetLeafNodes()

	rctleafNodeCids := make([]cid.Cid, len(rctLeafNodes))
	orderedRctLeafNodes := make([][]byte, len(rctLeafNodes))
	for i, rln := range rctLeafNodes {
		var idx uint

		r := bytes.NewReader(keys[i].TrieKey)
		rlp.Decode(r, &idx)
		rctleafNodeCids[idx] = rln.Cid()
		orderedRctLeafNodes[idx] = rln.RawData()
	}

	rct1CID = rctleafNodeCids[0]
	rct2CID = rctleafNodeCids[1]
	rct3CID = rctleafNodeCids[2]
	rct4CID = rctleafNodeCids[3]
	rct5CID = rctleafNodeCids[4]

	rctLeaf1 = orderedRctLeafNodes[0]
	rctLeaf2 = orderedRctLeafNodes[1]
	rctLeaf3 = orderedRctLeafNodes[2]
	rctLeaf4 = orderedRctLeafNodes[3]
	rctLeaf5 = orderedRctLeafNodes[4]

	nonCanonicalRctTrie := ipld.NewRctTrie()

	nonCanonicalRctTrie.Add(0, nonCanonicalRct1)
	nonCanonicalRctTrie.Add(1, nonCanonicalRct2)

	nonCanonicalRctLeafNodes, keys, _ := nonCanonicalRctTrie.GetLeafNodes()

	nonCanonicalRctLeafNodeCids := make([]cid.Cid, len(nonCanonicalRctLeafNodes))
	nonCanonicalRawRctLeafNodes := make([][]byte, len(nonCanonicalRctLeafNodes))
	for i, rln := range nonCanonicalRctLeafNodes {
		var idx uint

		r := bytes.NewReader(keys[i].TrieKey)
		rlp.Decode(r, &idx)
		nonCanonicalRctLeafNodeCids[idx] = rln.Cid()
		nonCanonicalRawRctLeafNodes[idx] = rln.RawData()
	}

	nonCanonicalRct1CID = nonCanonicalRctLeafNodeCids[0]
	nonCanonicalRct2CID = nonCanonicalRctLeafNodeCids[1]

	nonCanonicalRctLeaf1 = nonCanonicalRawRctLeafNodes[0]
	nonCanonicalRctLeaf2 = nonCanonicalRawRctLeafNodes[1]

	contract1Address = "0x5d663F5269090bD2A7DC2390c911dF6083D7b28F"
	contract2Address = "0x6Eb7e5C66DB8af2E96159AC440cbc8CDB7fbD26B"
	contract3Address = "0xcfeB164C328CA13EFd3C77E1980d94975aDfedfc"
	contract4Address = "0x0Edf0c4f393a628DE4828B228C48175b3EA297fc"
	contract1CreatedAt = uint64(1)
	contract2CreatedAt = uint64(2)
	contract3CreatedAt = uint64(3)
	contract4CreatedAt = uint64(4)

	lastFilledAt = uint64(0)
	watchedAt1 = uint64(10)
	watchedAt2 = uint64(15)
	watchedAt3 = uint64(20)
}

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}

func checkTxClosure(t *testing.T, idle, inUse, open int64) {
	require.Equal(t, idle, db.Stats().Idle())
	require.Equal(t, inUse, db.Stats().InUse())
	require.Equal(t, open, db.Stats().Open())
}

func tearDown(t *testing.T) {
	sql.TearDownDB(t, db)
	err := ind.Close()
	require.NoError(t, err)
}

// TODO Refactor rest of the tests to avoid duplication
// func testPublishAndIndexHeaderIPLDs() (t *testing.T) {

// }

func setupTestData(t *testing.T) {
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx.(*sql.BatchTx).BlockNumber)
}

func setupTestDataNonCanonical(t *testing.T) {
	var tx1 interfaces.Batch

	tx1, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx1, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx1.(*sql.BatchTx).BlockNumber)
	if err := tx1.Submit(err); err != nil {
		t.Fatal(err)
	}

	var tx2 interfaces.Batch
	tx2, err = ind.PushBlock(
		mocks.MockNonCanonicalBlock,
		mocks.MockNonCanonicalBlockReceipts,
		mocks.MockNonCanonicalBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx2.(*sql.BatchTx).BlockNumber)
	if err := tx2.Submit(err); err != nil {
		t.Fatal(err)
	}

	// TODO index state & storage nodes for second block
}

func testPublishAndIndexHeaderNonCanonical(t *testing.T) {
	// check indexed headers
	pgStr := `SELECT block_hash, cid, cast(td AS TEXT), cast(reward AS TEXT),
			tx_root, receipt_root, uncle_root, coinbase
			FROM eth.header_cids
			WHERE block_number = $1`
	headerRes := make([]models.HeaderModel, 0)
	err = db.Select(context.Background(), &headerRes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}

	expectedRes := []models.HeaderModel{
		{
			BlockHash:       mockBlock.Hash().String(),
			CID:             headerCID.String(),
			TotalDifficulty: mockBlock.Difficulty().String(),
			TxRoot:          mockBlock.TxHash().String(),
			RctRoot:         mockBlock.ReceiptHash().String(),
			UncleRoot:       mockBlock.UncleHash().String(),
			Coinbase:        mocks.MockHeader.Coinbase.String(),
		},
		{
			BlockHash:       mockNonCanonicalBlock.Hash().String(),
			CID:             mockNonCanonicalHeaderCID.String(),
			TotalDifficulty: mockNonCanonicalBlock.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock.ReceiptHash().String(),
			UncleRoot:       mockNonCanonicalBlock.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader.Coinbase.String(),
		},
	}
	expectedRes[0].Reward = shared.CalcEthBlockReward(mockBlock.Header(), mockBlock.Uncles(), mockBlock.Transactions(), mocks.MockReceipts).String()
	expectedRes[1].Reward = shared.CalcEthBlockReward(mockNonCanonicalBlock.Header(), mockNonCanonicalBlock.Uncles(), mockNonCanonicalBlock.Transactions(), mocks.MockNonCanonicalBlockReceipts).String()

	require.Equal(t, len(expectedRes), len(headerRes))
	require.ElementsMatch(t,
		[]string{mockBlock.Hash().String(), mocks.MockNonCanonicalBlock.Hash().String()},
		[]string{headerRes[0].BlockHash, headerRes[1].BlockHash},
	)

	if headerRes[0].BlockHash == mockBlock.Hash().String() {
		require.Equal(t, expectedRes[0], headerRes[0])
		require.Equal(t, expectedRes[1], headerRes[1])
	} else {
		require.Equal(t, expectedRes[1], headerRes[0])
		require.Equal(t, expectedRes[0], headerRes[1])
	}

	// check indexed IPLD blocks
	var data []byte
	var prefixedKey string

	prefixedKey = shared.MultihashKeyFromCID(headerCID)
	err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, mocks.MockHeaderRlp, data)

	prefixedKey = shared.MultihashKeyFromCID(mockNonCanonicalHeaderCID)
	err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, mocks.MockNonCanonicalHeaderRlp, data)
}

func testPublishAndIndexTransactionsNonCanonical(t *testing.T) {
	// check indexed transactions
	pgStr := `SELECT header_id, tx_hash, cid, dst, src, index,
		tx_data, tx_type, CAST(value as TEXT)
		FROM eth.transaction_cids
		WHERE block_number = $1`
	txRes := make([]models.TxModel, 0)
	err = db.Select(context.Background(), &txRes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}

	mockBlockTxs := mocks.MockBlock.Transactions()
	expectedBlockTxs := []models.TxModel{
		{
			HeaderID: mockBlock.Hash().String(),
			TxHash:   mockBlockTxs[0].Hash().String(),
			CID:      trx1CID.String(),
			Dst:      shared.HandleZeroAddrPointer(mockBlockTxs[0].To()),
			Src:      mocks.SenderAddr.String(),
			Index:    0,
			Data:     mockBlockTxs[0].Data(),
			Type:     mockBlockTxs[0].Type(),
			Value:    mockBlockTxs[0].Value().String(),
		},
		{
			HeaderID: mockBlock.Hash().String(),
			TxHash:   mockBlockTxs[1].Hash().String(),
			CID:      trx2CID.String(),
			Dst:      shared.HandleZeroAddrPointer(mockBlockTxs[1].To()),
			Src:      mocks.SenderAddr.String(),
			Index:    1,
			Data:     mockBlockTxs[1].Data(),
			Type:     mockBlockTxs[1].Type(),
			Value:    mockBlockTxs[1].Value().String(),
		},
		{
			HeaderID: mockBlock.Hash().String(),
			TxHash:   mockBlockTxs[2].Hash().String(),
			CID:      trx3CID.String(),
			Dst:      shared.HandleZeroAddrPointer(mockBlockTxs[2].To()),
			Src:      mocks.SenderAddr.String(),
			Index:    2,
			Data:     mockBlockTxs[2].Data(),
			Type:     mockBlockTxs[2].Type(),
			Value:    mockBlockTxs[2].Value().String(),
		},
		{
			HeaderID: mockBlock.Hash().String(),
			TxHash:   mockBlockTxs[3].Hash().String(),
			CID:      trx4CID.String(),
			Dst:      shared.HandleZeroAddrPointer(mockBlockTxs[3].To()),
			Src:      mocks.SenderAddr.String(),
			Index:    3,
			Data:     mockBlockTxs[3].Data(),
			Type:     mockBlockTxs[3].Type(),
			Value:    mockBlockTxs[3].Value().String(),
		},
		{
			HeaderID: mockBlock.Hash().String(),
			TxHash:   mockBlockTxs[4].Hash().String(),
			CID:      trx5CID.String(),
			Dst:      shared.HandleZeroAddrPointer(mockBlockTxs[4].To()),
			Src:      mocks.SenderAddr.String(),
			Index:    4,
			Data:     mockBlockTxs[4].Data(),
			Type:     mockBlockTxs[4].Type(),
			Value:    mockBlockTxs[4].Value().String(),
		},
	}

	mockNonCanonicalBlockTxs := mocks.MockNonCanonicalBlock.Transactions()
	expectedNonCanonicalBlockTxs := []models.TxModel{
		{
			HeaderID: mockNonCanonicalBlock.Hash().String(),
			TxHash:   mockNonCanonicalBlockTxs[0].Hash().String(),
			CID:      trx2CID.String(),
			Dst:      mockNonCanonicalBlockTxs[0].To().String(),
			Src:      mocks.SenderAddr.String(),
			Index:    0,
			Data:     mockNonCanonicalBlockTxs[0].Data(),
			Type:     mockNonCanonicalBlockTxs[0].Type(),
			Value:    mockNonCanonicalBlockTxs[0].Value().String(),
		},
		{
			HeaderID: mockNonCanonicalBlock.Hash().String(),
			TxHash:   mockNonCanonicalBlockTxs[1].Hash().String(),
			CID:      trx5CID.String(),
			Dst:      mockNonCanonicalBlockTxs[1].To().String(),
			Src:      mocks.SenderAddr.String(),
			Index:    1,
			Data:     mockNonCanonicalBlockTxs[1].Data(),
			Type:     mockNonCanonicalBlockTxs[1].Type(),
			Value:    mockNonCanonicalBlockTxs[1].Value().String(),
		},
	}

	require.Equal(t, len(expectedBlockTxs)+len(expectedNonCanonicalBlockTxs), len(txRes))

	// sort results such that non-canonical block reciepts come after canonical block ones
	sort.Slice(txRes, func(i, j int) bool {
		if txRes[i].HeaderID == txRes[j].HeaderID {
			return txRes[i].Index < txRes[j].Index
		} else if txRes[i].HeaderID == mockBlock.Hash().String() {
			return true
		} else {
			return false
		}
	})

	for i, expectedTx := range expectedBlockTxs {
		require.Equal(t, expectedTx, txRes[i])
	}
	for i, expectedTx := range expectedNonCanonicalBlockTxs {
		require.Equal(t, expectedTx, txRes[len(expectedBlockTxs)+i])
	}

	// check indexed IPLD blocks
	var data []byte
	var prefixedKey string

	txCIDs := []cid.Cid{trx1CID, trx2CID, trx3CID, trx4CID, trx5CID}
	txRLPs := [][]byte{tx1, tx2, tx3, tx4, tx5}
	for i, txCID := range txCIDs {
		prefixedKey = shared.MultihashKeyFromCID(txCID)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, txRLPs[i], data)
	}
}

func testPublishAndIndexReceiptsNonCanonical(t *testing.T) {
	// check indexed receipts
	pgStr := `SELECT header_id, tx_id, leaf_cid, leaf_mh_key, post_status, post_state, contract, contract_hash, log_root
		FROM eth.receipt_cids
		WHERE block_number = $1`
	rctRes := make([]models.ReceiptModel, 0)
	err = db.Select(context.Background(), &rctRes, pgStr, mocks.BlockNumber.Uint64())
	if err != nil {
		t.Fatal(err)
	}

	mockBlockRcts := mocks.MockReceipts
	rctCids := []cid.Cid{rct1CID, rct2CID, rct3CID, rct4CID, rct5CID}
	expectedBlockRcts := make(map[string]models.ReceiptModel, len(mockBlockRcts))
	for i, mockBlockRct := range mockBlockRcts {
		rctModel := models.ReceiptModel{
			HeaderID:  mockBlock.Hash().String(),
			TxID:      mockBlockRct.TxHash.String(),
			LeafCID:   rctCids[i].String(),
			LeafMhKey: shared.MultihashKeyFromCID(rctCids[i]),
			LogRoot:   mockBlockRct.LogRoot.String(),
		}

		contract := shared.HandleZeroAddr(mockBlockRct.ContractAddress)
		rctModel.Contract = contract
		if contract != "" {
			rctModel.ContractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}

		if len(mockBlockRct.PostState) == 0 {
			rctModel.PostStatus = mockBlockRct.Status
		} else {
			rctModel.PostState = common.Bytes2Hex(mockBlockRct.PostState)
		}

		expectedBlockRcts[rctCids[i].String()] = rctModel
	}

	mockNonCanonicalBlockRcts := mocks.MockNonCanonicalBlockReceipts
	nonCanonicalRctCids := []cid.Cid{nonCanonicalRct1CID, nonCanonicalRct2CID}
	expectedNonCanonicalBlockRcts := make(map[string]models.ReceiptModel, len(mockNonCanonicalBlockRcts))
	for i, mockNonCanonicalBlockRct := range mockNonCanonicalBlockRcts {
		rctModel := models.ReceiptModel{
			HeaderID:  mockNonCanonicalBlock.Hash().String(),
			TxID:      mockNonCanonicalBlockRct.TxHash.String(),
			LeafCID:   nonCanonicalRctCids[i].String(),
			LeafMhKey: shared.MultihashKeyFromCID(nonCanonicalRctCids[i]),
			LogRoot:   mockNonCanonicalBlockRct.LogRoot.String(),
		}

		contract := shared.HandleZeroAddr(mockNonCanonicalBlockRct.ContractAddress)
		rctModel.Contract = contract
		if contract != "" {
			rctModel.ContractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}

		if len(mockNonCanonicalBlockRct.PostState) == 0 {
			rctModel.PostStatus = mockNonCanonicalBlockRct.Status
		} else {
			rctModel.PostState = common.Bytes2Hex(mockNonCanonicalBlockRct.PostState)
		}

		expectedBlockRcts[nonCanonicalRctCids[i].String()] = rctModel
	}

	require.Equal(t, len(expectedBlockRcts)+len(expectedNonCanonicalBlockRcts), len(rctRes))

	// sort results such that non-canonical block reciepts come after canonical block ones
	sort.Slice(rctRes, func(i, j int) bool {
		if rctRes[i].HeaderID == rctRes[j].HeaderID {
			return false
		} else {
			if rctRes[i].HeaderID == mockBlock.Hash().String() {
				return true
			} else {
				return false
			}
		}
	})

	for i := 0; i < len(expectedBlockRcts); i++ {
		rct := rctRes[i]
		require.Contains(t, expectedBlockRcts, rct.LeafCID)
		require.Equal(t, expectedBlockRcts[rct.LeafCID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlockRcts); i++ {
		rct := rctRes[len(expectedBlockRcts)+i]
		require.Contains(t, expectedNonCanonicalBlockRcts, rct.LeafCID)
		require.Equal(t, expectedNonCanonicalBlockRcts[rct.LeafCID], rct)
	}

	// check indexed rct IPLD blocks
	var data []byte
	var prefixedKey string

	rctRLPs := [][]byte{rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5}
	for i, rctCid := range rctCids {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, rctRLPs[i], data)
	}

	nonCanonicalrctRLPs := [][]byte{nonCanonicalRctLeaf1, nonCanonicalRctLeaf2}
	for i, rctCid := range nonCanonicalRctCids {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, nonCanonicalrctRLPs[i], data)
	}
}

func testPublishAndIndexLogsNonCanonical(t *testing.T) {
	// check indexed logs
	pgStr := `SELECT address, log_data, topic0, topic1, topic2, topic3, data
		FROM eth.log_cids
		INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
		WHERE log_cids.block_number = $1 AND header_id = $2 AND rct_id = $3
		ORDER BY log_cids.index ASC`

	type rctWithBlockHash struct {
		rct       *types.Receipt
		blockHash string
	}
	mockRcts := make([]rctWithBlockHash, 0)

	for _, mockBlockRct := range mocks.MockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockBlock.Hash().String(),
		})
	}

	for _, mockBlockRct := range mocks.MockNonCanonicalBlockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockNonCanonicalBlock.Hash().String(),
		})
	}

	for _, mockRct := range mockRcts {
		type logWithIPLD struct {
			models.LogsModel
			IPLDData []byte `db:"data"`
		}
		logRes := make([]logWithIPLD, 0)
		err = db.Select(context.Background(), &logRes, pgStr, mocks.BlockNumber.Uint64(), mockRct.blockHash, mockRct.rct.TxHash.String())
		require.NoError(t, err)
		require.Equal(t, len(mockRct.rct.Logs), len(logRes))

		for i, log := range mockRct.rct.Logs {
			topicSet := make([]string, 4)
			for ti, topic := range log.Topics {
				topicSet[ti] = topic.Hex()
			}

			expectedLog := models.LogsModel{
				Address: log.Address.String(),
				Data:    log.Data,
				Topic0:  topicSet[0],
				Topic1:  topicSet[1],
				Topic2:  topicSet[2],
				Topic3:  topicSet[3],
			}
			require.Equal(t, expectedLog, logRes[i].LogsModel)

			// check indexed log IPLD block
			var nodeElements []interface{}
			err = rlp.DecodeBytes(logRes[i].IPLDData, &nodeElements)
			require.NoError(t, err)

			if len(nodeElements) == 2 {
				logRaw, err := rlp.EncodeToBytes(&log)
				require.NoError(t, err)
				// 2nd element of the leaf node contains the encoded log data.
				require.Equal(t, nodeElements[1].([]byte), logRaw)
			} else {
				logRaw, err := rlp.EncodeToBytes(&log)
				require.NoError(t, err)
				// raw log was IPLDized
				require.Equal(t, logRes[i].IPLDData, logRaw)
			}
		}
	}
}
