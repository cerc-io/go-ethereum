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
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5                          []byte
	nonCanonicalBlockRct1, nonCanonicalBlockRct2                                   []byte
	nonCanonicalBlock2Rct1, nonCanonicalBlock2Rct2                                 []byte
	mockBlock, mockNonCanonicalBlock, mockNonCanonicalBlock2                       *types.Block
	headerCID, mockNonCanonicalHeaderCID, mockNonCanonicalHeader2CID               cid.Cid
	trx1CID, trx2CID, trx3CID, trx4CID, trx5CID                                    cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                                    cid.Cid
	nonCanonicalBlockRct1CID, nonCanonicalBlockRct2CID                             cid.Cid
	nonCanonicalBlock2Rct1CID, nonCanonicalBlock2Rct2CID                           cid.Cid
	rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5                               []byte
	nonCanonicalBlockRctLeaf1, nonCanonicalBlockRctLeaf2                           []byte
	nonCanonicalBlock2RctLeaf1, nonCanonicalBlock2RctLeaf2                         []byte
	state1CID, state2CID, storageCID                                               cid.Cid
	contract1Address, contract2Address, contract3Address, contract4Address         string
	contract1CreatedAt, contract2CreatedAt, contract3CreatedAt, contract4CreatedAt uint64
	lastFilledAt, watchedAt1, watchedAt2, watchedAt3                               uint64
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

	mockNonCanonicalBlock2 = mocks.MockNonCanonicalBlock2
	nonCanonicalBlock2Rcts := mocks.MockNonCanonicalBlock2Receipts

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
	nonCanonicalBlockRct1 = make([]byte, buf.Len())
	copy(nonCanonicalBlockRct1, buf.Bytes())
	buf.Reset()

	nonCanonicalBlockRcts.EncodeIndex(1, buf)
	nonCanonicalBlockRct2 = make([]byte, buf.Len())
	copy(nonCanonicalBlockRct2, buf.Bytes())
	buf.Reset()

	nonCanonicalBlock2Rcts.EncodeIndex(0, buf)
	nonCanonicalBlock2Rct1 = make([]byte, buf.Len())
	copy(nonCanonicalBlock2Rct1, buf.Bytes())
	buf.Reset()

	nonCanonicalBlock2Rcts.EncodeIndex(1, buf)
	nonCanonicalBlock2Rct2 = make([]byte, buf.Len())
	copy(nonCanonicalBlock2Rct2, buf.Bytes())
	buf.Reset()

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
	mockNonCanonicalHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockNonCanonicalHeaderRlp, multihash.KECCAK_256)
	mockNonCanonicalHeader2CID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockNonCanonicalHeader2Rlp, multihash.KECCAK_256)
	trx1CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx1, multihash.KECCAK_256)
	trx2CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx2, multihash.KECCAK_256)
	trx3CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx3, multihash.KECCAK_256)
	trx4CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx4, multihash.KECCAK_256)
	trx5CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)

	rawRctLeafNodes, rctleafNodeCids := createRctTrie([][]byte{rct1, rct2, rct3, rct4, rct5})

	rct1CID = rctleafNodeCids[0]
	rct2CID = rctleafNodeCids[1]
	rct3CID = rctleafNodeCids[2]
	rct4CID = rctleafNodeCids[3]
	rct5CID = rctleafNodeCids[4]

	rctLeaf1 = rawRctLeafNodes[0]
	rctLeaf2 = rawRctLeafNodes[1]
	rctLeaf3 = rawRctLeafNodes[2]
	rctLeaf4 = rawRctLeafNodes[3]
	rctLeaf5 = rawRctLeafNodes[4]

	nonCanonicalBlockRawRctLeafNodes, nonCanonicalBlockRctLeafNodeCids := createRctTrie([][]byte{nonCanonicalBlockRct1, nonCanonicalBlockRct2})

	nonCanonicalBlockRct1CID = nonCanonicalBlockRctLeafNodeCids[0]
	nonCanonicalBlockRct2CID = nonCanonicalBlockRctLeafNodeCids[1]

	nonCanonicalBlockRctLeaf1 = nonCanonicalBlockRawRctLeafNodes[0]
	nonCanonicalBlockRctLeaf2 = nonCanonicalBlockRawRctLeafNodes[1]

	nonCanonicalBlock2RawRctLeafNodes, nonCanonicalBlock2RctLeafNodeCids := createRctTrie([][]byte{nonCanonicalBlockRct1, nonCanonicalBlockRct2})

	nonCanonicalBlock2Rct1CID = nonCanonicalBlock2RctLeafNodeCids[0]
	nonCanonicalBlock2Rct2CID = nonCanonicalBlock2RctLeafNodeCids[1]

	nonCanonicalBlock2RctLeaf1 = nonCanonicalBlock2RawRctLeafNodes[0]
	nonCanonicalBlock2RctLeaf2 = nonCanonicalBlock2RawRctLeafNodes[1]

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

func createRctTrie(rcts [][]byte) ([][]byte, []cid.Cid) {
	receiptTrie := ipld.NewRctTrie()

	for i, rct := range rcts {
		receiptTrie.Add(i, rct)
	}
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

	return orderedRctLeafNodes, rctleafNodeCids
}

func createRctModel(rct *types.Receipt, cid cid.Cid, blockNumber string) models.ReceiptModel {
	rctModel := models.ReceiptModel{
		BlockNumber: blockNumber,
		HeaderID:    rct.BlockHash.String(),
		TxID:        rct.TxHash.String(),
		LeafCID:     cid.String(),
		LeafMhKey:   shared.MultihashKeyFromCID(cid),
		LogRoot:     rct.LogRoot.String(),
	}

	contract := shared.HandleZeroAddr(rct.ContractAddress)
	rctModel.Contract = contract
	if contract != "" {
		rctModel.ContractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
	}

	if len(rct.PostState) == 0 {
		rctModel.PostStatus = rct.Status
	} else {
		rctModel.PostState = common.Bytes2Hex(rct.PostState)
	}

	return rctModel
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
		mockNonCanonicalBlock,
		mocks.MockNonCanonicalBlockReceipts,
		mockNonCanonicalBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx2, node, mockNonCanonicalBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx2.(*sql.BatchTx).BlockNumber)
	if err := tx2.Submit(err); err != nil {
		t.Fatal(err)
	}

	var tx3 interfaces.Batch
	tx3, err = ind.PushBlock(
		mockNonCanonicalBlock2,
		mocks.MockNonCanonicalBlock2Receipts,
		mockNonCanonicalBlock2.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range mocks.StateDiffs[:2] {
		err = ind.PushStateNode(tx3, node, mockNonCanonicalBlock2.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.Block2Number.String(), tx3.(*sql.BatchTx).BlockNumber)
	if err := tx3.Submit(err); err != nil {
		t.Fatal(err)
	}
}

func testPublishAndIndexHeaderNonCanonical(t *testing.T) {
	// check indexed headers
	pgStr := `SELECT CAST(block_number as TEXT), block_hash, cid, cast(td AS TEXT), cast(reward AS TEXT),
			tx_root, receipt_root, uncle_root, coinbase
			FROM eth.header_cids
			ORDER BY block_number`
	headerRes := make([]models.HeaderModel, 0)
	err = db.Select(context.Background(), &headerRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	expectedRes := []models.HeaderModel{
		{
			BlockNumber:     mockBlock.Number().String(),
			BlockHash:       mockBlock.Hash().String(),
			CID:             headerCID.String(),
			TotalDifficulty: mockBlock.Difficulty().String(),
			TxRoot:          mockBlock.TxHash().String(),
			RctRoot:         mockBlock.ReceiptHash().String(),
			UncleRoot:       mockBlock.UncleHash().String(),
			Coinbase:        mocks.MockHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock.Number().String(),
			BlockHash:       mockNonCanonicalBlock.Hash().String(),
			CID:             mockNonCanonicalHeaderCID.String(),
			TotalDifficulty: mockNonCanonicalBlock.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock.ReceiptHash().String(),
			UncleRoot:       mockNonCanonicalBlock.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader.Coinbase.String(),
		},
		{
			BlockNumber:     mockNonCanonicalBlock2.Number().String(),
			BlockHash:       mockNonCanonicalBlock2.Hash().String(),
			CID:             mockNonCanonicalHeader2CID.String(),
			TotalDifficulty: mockNonCanonicalBlock2.Difficulty().String(),
			TxRoot:          mockNonCanonicalBlock2.TxHash().String(),
			RctRoot:         mockNonCanonicalBlock2.ReceiptHash().String(),
			UncleRoot:       mockNonCanonicalBlock2.UncleHash().String(),
			Coinbase:        mocks.MockNonCanonicalHeader2.Coinbase.String(),
		},
	}
	expectedRes[0].Reward = shared.CalcEthBlockReward(mockBlock.Header(), mockBlock.Uncles(), mockBlock.Transactions(), mocks.MockReceipts).String()
	expectedRes[1].Reward = shared.CalcEthBlockReward(mockNonCanonicalBlock.Header(), mockNonCanonicalBlock.Uncles(), mockNonCanonicalBlock.Transactions(), mocks.MockNonCanonicalBlockReceipts).String()
	expectedRes[2].Reward = shared.CalcEthBlockReward(mockNonCanonicalBlock2.Header(), mockNonCanonicalBlock2.Uncles(), mockNonCanonicalBlock2.Transactions(), mocks.MockNonCanonicalBlock2Receipts).String()

	require.Equal(t, len(expectedRes), len(headerRes))
	require.ElementsMatch(t,
		[]string{mockBlock.Hash().String(), mockNonCanonicalBlock.Hash().String(), mockNonCanonicalBlock2.Hash().String()},
		[]string{headerRes[0].BlockHash, headerRes[1].BlockHash, headerRes[2].BlockHash},
	)

	if headerRes[0].BlockHash == mockBlock.Hash().String() {
		require.Equal(t, expectedRes[0], headerRes[0])
		require.Equal(t, expectedRes[1], headerRes[1])
		require.Equal(t, expectedRes[2], headerRes[2])
	} else {
		require.Equal(t, expectedRes[1], headerRes[0])
		require.Equal(t, expectedRes[0], headerRes[1])
		require.Equal(t, expectedRes[2], headerRes[2])
	}

	// check indexed IPLD blocks
	headerCIDs := []cid.Cid{headerCID, mockNonCanonicalHeaderCID, mockNonCanonicalHeader2CID}
	blockNumbers := []uint64{mocks.BlockNumber.Uint64(), mocks.BlockNumber.Uint64(), mocks.Block2Number.Uint64()}
	headerRLPs := [][]byte{mocks.MockHeaderRlp, mocks.MockNonCanonicalHeaderRlp, mocks.MockNonCanonicalHeader2Rlp}
	for i := range expectedRes {
		var data []byte
		prefixedKey := shared.MultihashKeyFromCID(headerCIDs[i])
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, blockNumbers[i])
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, headerRLPs[i], data)
	}
}

func testPublishAndIndexTransactionsNonCanonical(t *testing.T) {
	// check indexed transactions
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_hash, cid, dst, src, index,
		tx_data, tx_type, CAST(value as TEXT)
		FROM eth.transaction_cids
		ORDER BY block_number, index`
	txRes := make([]models.TxModel, 0)
	err = db.Select(context.Background(), &txRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	mockBlockTxs := mocks.MockBlock.Transactions()
	expectedBlockTxs := []models.TxModel{
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[0].Hash().String(),
			CID:         trx1CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[0].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockBlockTxs[0].Data(),
			Type:        mockBlockTxs[0].Type(),
			Value:       mockBlockTxs[0].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[1].Hash().String(),
			CID:         trx2CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[1].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockBlockTxs[1].Data(),
			Type:        mockBlockTxs[1].Type(),
			Value:       mockBlockTxs[1].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[2].Hash().String(),
			CID:         trx3CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[2].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       2,
			Data:        mockBlockTxs[2].Data(),
			Type:        mockBlockTxs[2].Type(),
			Value:       mockBlockTxs[2].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[3].Hash().String(),
			CID:         trx4CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[3].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       3,
			Data:        mockBlockTxs[3].Data(),
			Type:        mockBlockTxs[3].Type(),
			Value:       mockBlockTxs[3].Value().String(),
		},
		{
			BlockNumber: mockBlock.Number().String(),
			HeaderID:    mockBlock.Hash().String(),
			TxHash:      mockBlockTxs[4].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         shared.HandleZeroAddrPointer(mockBlockTxs[4].To()),
			Src:         mocks.SenderAddr.String(),
			Index:       4,
			Data:        mockBlockTxs[4].Data(),
			Type:        mockBlockTxs[4].Type(),
			Value:       mockBlockTxs[4].Value().String(),
		},
	}

	mockNonCanonicalBlockTxs := mockNonCanonicalBlock.Transactions()
	expectedNonCanonicalBlockTxs := []models.TxModel{
		{
			BlockNumber: mockNonCanonicalBlock.Number().String(),
			HeaderID:    mockNonCanonicalBlock.Hash().String(),
			TxHash:      mockNonCanonicalBlockTxs[0].Hash().String(),
			CID:         trx2CID.String(),
			Dst:         mockNonCanonicalBlockTxs[0].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockNonCanonicalBlockTxs[0].Data(),
			Type:        mockNonCanonicalBlockTxs[0].Type(),
			Value:       mockNonCanonicalBlockTxs[0].Value().String(),
		},
		{
			BlockNumber: mockNonCanonicalBlock.Number().String(),
			HeaderID:    mockNonCanonicalBlock.Hash().String(),
			TxHash:      mockNonCanonicalBlockTxs[1].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         mockNonCanonicalBlockTxs[1].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockNonCanonicalBlockTxs[1].Data(),
			Type:        mockNonCanonicalBlockTxs[1].Type(),
			Value:       mockNonCanonicalBlockTxs[1].Value().String(),
		},
	}

	mockNonCanonicalBlock2Txs := mockNonCanonicalBlock2.Transactions()
	expectedNonCanonicalBlock2Txs := []models.TxModel{
		{
			BlockNumber: mockNonCanonicalBlock2.Number().String(),
			HeaderID:    mockNonCanonicalBlock2.Hash().String(),
			TxHash:      mockNonCanonicalBlock2Txs[0].Hash().String(),
			CID:         trx3CID.String(),
			Dst:         "",
			Src:         mocks.SenderAddr.String(),
			Index:       0,
			Data:        mockNonCanonicalBlock2Txs[0].Data(),
			Type:        mockNonCanonicalBlock2Txs[0].Type(),
			Value:       mockNonCanonicalBlock2Txs[0].Value().String(),
		},
		{
			BlockNumber: mockNonCanonicalBlock2.Number().String(),
			HeaderID:    mockNonCanonicalBlock2.Hash().String(),
			TxHash:      mockNonCanonicalBlock2Txs[1].Hash().String(),
			CID:         trx5CID.String(),
			Dst:         mockNonCanonicalBlock2Txs[1].To().String(),
			Src:         mocks.SenderAddr.String(),
			Index:       1,
			Data:        mockNonCanonicalBlock2Txs[1].Data(),
			Type:        mockNonCanonicalBlock2Txs[1].Type(),
			Value:       mockNonCanonicalBlock2Txs[1].Value().String(),
		},
	}

	require.Equal(t, len(expectedBlockTxs)+len(expectedNonCanonicalBlockTxs)+len(expectedNonCanonicalBlock2Txs), len(txRes))

	// sort results such that non-canonical block transactions come after canonical block ones
	sort.SliceStable(txRes, func(i, j int) bool {
		if txRes[i].BlockNumber < txRes[j].BlockNumber {
			return true
		} else if txRes[i].HeaderID == txRes[j].HeaderID {
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
	for i, expectedTx := range expectedNonCanonicalBlock2Txs {
		require.Equal(t, expectedTx, txRes[len(expectedBlockTxs)+len(expectedNonCanonicalBlockTxs)+i])
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
	pgStr := `SELECT CAST(block_number as TEXT), header_id, tx_id, leaf_cid, leaf_mh_key, post_status, post_state, contract, contract_hash, log_root
		FROM eth.receipt_cids
		ORDER BY block_number`
	rctRes := make([]models.ReceiptModel, 0)
	err = db.Select(context.Background(), &rctRes, pgStr)
	if err != nil {
		t.Fatal(err)
	}

	rctCids := []cid.Cid{rct1CID, rct2CID, rct3CID, rct4CID, rct5CID}
	expectedBlockRctsMap := make(map[string]models.ReceiptModel, len(mocks.MockReceipts))
	for i, mockBlockRct := range mocks.MockReceipts {
		rctModel := createRctModel(mockBlockRct, rctCids[i], mockBlock.Number().String())
		expectedBlockRctsMap[rctCids[i].String()] = rctModel
	}

	nonCanonicalBlockRctCids := []cid.Cid{nonCanonicalBlockRct1CID, nonCanonicalBlockRct2CID}
	expectedNonCanonicalBlockRctsMap := make(map[string]models.ReceiptModel, len(mocks.MockNonCanonicalBlockReceipts))
	for i, mockNonCanonicalBlockRct := range mocks.MockNonCanonicalBlockReceipts {
		rctModel := createRctModel(mockNonCanonicalBlockRct, nonCanonicalBlockRctCids[i], mockNonCanonicalBlock.Number().String())
		expectedNonCanonicalBlockRctsMap[nonCanonicalBlockRctCids[i].String()] = rctModel
	}

	nonCanonicalBlock2RctCids := []cid.Cid{nonCanonicalBlock2Rct1CID, nonCanonicalBlock2Rct2CID}
	expectedNonCanonicalBlock2RctsMap := make(map[string]models.ReceiptModel, len(mocks.MockNonCanonicalBlock2Receipts))
	for i, mockNonCanonicalBlock2Rct := range mocks.MockNonCanonicalBlock2Receipts {
		rctModel := createRctModel(mockNonCanonicalBlock2Rct, nonCanonicalBlock2RctCids[i], mockNonCanonicalBlock2.Number().String())
		expectedNonCanonicalBlock2RctsMap[nonCanonicalBlock2RctCids[i].String()] = rctModel
	}

	require.Equal(t, len(expectedBlockRctsMap)+len(expectedNonCanonicalBlockRctsMap)+len(expectedNonCanonicalBlock2RctsMap), len(rctRes))

	// sort results such that non-canonical block reciepts come after canonical block ones
	sort.SliceStable(rctRes, func(i, j int) bool {
		if rctRes[i].BlockNumber < rctRes[j].BlockNumber {
			return true
		} else if rctRes[i].HeaderID == rctRes[j].HeaderID {
			return false
		} else if rctRes[i].HeaderID == mockBlock.Hash().String() {
			return true
		} else {
			return false
		}
	})

	for i := 0; i < len(expectedBlockRctsMap); i++ {
		rct := rctRes[i]
		require.Contains(t, expectedBlockRctsMap, rct.LeafCID)
		require.Equal(t, expectedBlockRctsMap[rct.LeafCID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlockRctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlockRctsMap, rct.LeafCID)
		require.Equal(t, expectedNonCanonicalBlockRctsMap[rct.LeafCID], rct)
	}

	for i := 0; i < len(expectedNonCanonicalBlock2RctsMap); i++ {
		rct := rctRes[len(expectedBlockRctsMap)+len(expectedNonCanonicalBlockRctsMap)+i]
		require.Contains(t, expectedNonCanonicalBlock2RctsMap, rct.LeafCID)
		require.Equal(t, expectedNonCanonicalBlock2RctsMap[rct.LeafCID], rct)
	}

	// check indexed rct IPLD blocks
	var data []byte
	var prefixedKey string

	rctRLPs := [][]byte{
		rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5,
		nonCanonicalBlockRctLeaf1, nonCanonicalBlockRctLeaf2,
	}
	for i, rctCid := range append(rctCids, nonCanonicalBlockRctCids...) {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, rctRLPs[i], data)
	}

	nonCanonicalBlock2RctRLPs := [][]byte{nonCanonicalBlock2RctLeaf1, nonCanonicalBlock2RctLeaf2}
	for i, rctCid := range nonCanonicalBlock2RctCids {
		prefixedKey = shared.MultihashKeyFromCID(rctCid)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey, mocks.Block2Number.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, nonCanonicalBlock2RctRLPs[i], data)
	}
}

func testPublishAndIndexLogsNonCanonical(t *testing.T) {
	// check indexed logs
	pgStr := `SELECT address, log_data, topic0, topic1, topic2, topic3, data
		FROM eth.log_cids
		INNER JOIN public.blocks ON (log_cids.block_number = blocks.block_number AND log_cids.leaf_mh_key = blocks.key)
		WHERE log_cids.block_number = $1 AND header_id = $2 AND rct_id = $3
		ORDER BY log_cids.index ASC`

	type rctWithBlockHash struct {
		rct         *types.Receipt
		blockHash   string
		blockNumber uint64
	}
	mockRcts := make([]rctWithBlockHash, 0)

	for _, mockBlockRct := range mocks.MockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockBlock.Hash().String(),
			mockBlock.NumberU64(),
		})
	}

	for _, mockBlockRct := range mocks.MockNonCanonicalBlockReceipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockNonCanonicalBlock.Hash().String(),
			mockNonCanonicalBlock.NumberU64(),
		})
	}

	for _, mockBlockRct := range mocks.MockNonCanonicalBlock2Receipts {
		mockRcts = append(mockRcts, rctWithBlockHash{
			mockBlockRct,
			mockNonCanonicalBlock2.Hash().String(),
			mockNonCanonicalBlock2.NumberU64(),
		})
	}

	for _, mockRct := range mockRcts {
		type logWithIPLD struct {
			models.LogsModel
			IPLDData []byte `db:"data"`
		}
		logRes := make([]logWithIPLD, 0)
		err = db.Select(context.Background(), &logRes, pgStr, mockRct.blockNumber, mockRct.blockHash, mockRct.rct.TxHash.String())
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
				logRaw, err := rlp.EncodeToBytes(log)
				require.NoError(t, err)
				// 2nd element of the leaf node contains the encoded log data.
				require.Equal(t, nodeElements[1].([]byte), logRaw)
			} else {
				logRaw, err := rlp.EncodeToBytes(log)
				require.NoError(t, err)
				// raw log was IPLDized
				require.Equal(t, logRes[i].IPLDData, logRaw)
			}
		}
	}
}

func testPublishAndIndexStateNonCanonical(t *testing.T) {
	// check indexed state nodes
	pgStr := `SELECT state_path, state_leaf_key, node_type, cid, mh_key, diff
					FROM eth.state_cids
					WHERE block_number = $1
					AND header_id = $2
					ORDER BY state_path`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStateCID)
	stateNodeCIDs := []cid.Cid{state1CID, state2CID, removedNodeCID, removedNodeCID}

	expectedStateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs {
		expectedStateNodes = append(expectedStateNodes, models.StateNodeModel{
			Path:     stateDiff.Path,
			StateKey: common.BytesToHash(stateDiff.LeafKey).Hex(),
			NodeType: stateDiff.NodeType.Int(),
			CID:      stateNodeCIDs[i].String(),
			MhKey:    shared.MultihashKeyFromCID(stateNodeCIDs[i]),
			Diff:     true,
		})
	}
	sort.Slice(expectedStateNodes, func(i, j int) bool {
		if bytes.Compare(expectedStateNodes[i].Path, expectedStateNodes[j].Path) < 0 {
			return true
		} else {
			return false
		}
	})

	expectedNonCanonicalBlock2StateNodes := make([]models.StateNodeModel, 0)
	for i, stateDiff := range mocks.StateDiffs[:2] {
		expectedNonCanonicalBlock2StateNodes = append(expectedNonCanonicalBlock2StateNodes, models.StateNodeModel{
			Path:     stateDiff.Path,
			StateKey: common.BytesToHash(stateDiff.LeafKey).Hex(),
			NodeType: stateDiff.NodeType.Int(),
			CID:      stateNodeCIDs[i].String(),
			MhKey:    shared.MultihashKeyFromCID(stateNodeCIDs[i]),
			Diff:     true,
		})
	}

	// check state nodes for canonical block
	stateNodes := make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64(), mockBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedStateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}

	// check state nodes for non-canonical block
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedStateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}

	// check state nodes for non-canonical block at another height
	stateNodes = make([]models.StateNodeModel, 0)
	err = db.Select(context.Background(), &stateNodes, pgStr, mocks.Block2Number.Uint64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StateNodes), len(stateNodes))

	for i, expectedStateNode := range expectedNonCanonicalBlock2StateNodes {
		require.Equal(t, expectedStateNode, stateNodes[i])
	}
}

func testPublishAndIndexStorageNonCanonical(t *testing.T) {
	// check indexed storage nodes
	pgStr := `SELECT state_path, storage_path, storage_leaf_key, node_type, cid, mh_key, diff
					FROM eth.storage_cids
					WHERE block_number = $1
					AND header_id = $2
					ORDER BY state_path, storage_path`

	removedNodeCID, _ := cid.Decode(shared.RemovedNodeStorageCID)
	storageNodeCIDs := []cid.Cid{storageCID, removedNodeCID, removedNodeCID, removedNodeCID}

	expectedStorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex := 0
	for _, stateDiff := range mocks.StateDiffs {
		for _, storageNode := range stateDiff.StorageNodes {
			expectedStorageNodes = append(expectedStorageNodes, models.StorageNodeModel{
				StatePath:  stateDiff.Path,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				NodeType:   storageNode.NodeType.Int(),
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				MhKey:      shared.MultihashKeyFromCID(storageNodeCIDs[storageNodeIndex]),
				Diff:       true,
			})
			storageNodeIndex++
		}
	}
	sort.Slice(expectedStorageNodes, func(i, j int) bool {
		if bytes.Compare(expectedStorageNodes[i].Path, expectedStorageNodes[j].Path) < 0 {
			return true
		} else {
			return false
		}
	})

	expectedNonCanonicalBlock2StorageNodes := make([]models.StorageNodeModel, 0)
	storageNodeIndex = 0
	for _, stateDiff := range mocks.StateDiffs[:2] {
		for _, storageNode := range stateDiff.StorageNodes {
			expectedNonCanonicalBlock2StorageNodes = append(expectedNonCanonicalBlock2StorageNodes, models.StorageNodeModel{
				StatePath:  stateDiff.Path,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).Hex(),
				NodeType:   storageNode.NodeType.Int(),
				CID:        storageNodeCIDs[storageNodeIndex].String(),
				MhKey:      shared.MultihashKeyFromCID(storageNodeCIDs[storageNodeIndex]),
				Diff:       true,
			})
			storageNodeIndex++
		}
	}

	// check storage nodes for canonical block
	storageNodes := make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64(), mockBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedStorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}

	// check storage nodes for non-canonical block
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64(), mockNonCanonicalBlock.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedStorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedStorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}

	// check storage nodes for non-canonical block at another height
	storageNodes = make([]models.StorageNodeModel, 0)
	err = db.Select(context.Background(), &storageNodes, pgStr, mockNonCanonicalBlock2.NumberU64(), mockNonCanonicalBlock2.Hash().String())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(expectedNonCanonicalBlock2StorageNodes), len(storageNodes))

	for i, expectedStorageNode := range expectedNonCanonicalBlock2StorageNodes {
		require.Equal(t, expectedStorageNode, storageNodes[i])
	}
}
