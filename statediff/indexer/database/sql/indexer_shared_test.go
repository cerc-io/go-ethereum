package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
)

var (
	db        sql.Database
	err       error
	ind       interfaces.StateDiffIndexer
	chainConf = params.MainnetChainConfig
	ipfsPgGet = `SELECT data FROM public.blocks
					WHERE key = $1 AND block_number = $2`
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5                          []byte
	mockBlock                                                                      *types.Block
	headerCID, trx1CID, trx2CID, trx3CID, trx4CID, trx5CID                         cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                                    cid.Cid
	rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5                               []byte
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

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
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

func setupDb(t *testing.T) (*sql.StateDiffIndexer, error) {
	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
	stateDiff, err := sql.NewStateDiffIndexer(context.Background(), chainConf, db)
	return stateDiff, err
}

func tearDown(t *testing.T) {
	sql.TearDownDB(t, db)
	err := ind.Close()
	require.NoError(t, err)
}
func TestKnownGapsUpsert(t *testing.T) {
	testKnownGapsUpsert(t)

}
func testKnownGapsUpsert(t *testing.T) {
	gapDifference := big.NewInt(10)     // Set a difference between latestBlock in DB and on Chain
	expectedDifference := big.NewInt(1) // Set what the expected difference between latestBlock in DB and on Chain should be

	stateDiff, err := setupDb(t)
	if err != nil {
		t.Fatal(err)
	}

	// Get the latest block from the DB
	latestBlockInDb, err := stateDiff.QueryDbToBigInt("SELECT MAX(block_number) FROM eth.header_cids")
	if err != nil {
		t.Fatal("Can't find a block in the eth.header_cids table.. Please put one there")
	}

	// Add the gapDifference for testing purposes
	latestBlockOnChain := big.NewInt(0)
	latestBlockOnChain.Add(latestBlockInDb, gapDifference)

	t.Log("The latest block on the chain is: ", latestBlockOnChain)
	t.Log("The latest block on the DB is: ", latestBlockInDb)

	gapUpsertErr := stateDiff.FindAndUpdateGaps(latestBlockOnChain, expectedDifference, 0)
	require.NoError(t, gapUpsertErr)

	// Calculate what the start and end block should be in known_gaps
	// And check to make sure it is properly inserted
	startBlock := big.NewInt(0)
	endBlock := big.NewInt(0)
	startBlock.Add(latestBlockInDb, expectedDifference)
	endBlock.Sub(latestBlockOnChain, expectedDifference)

	queryString := fmt.Sprintf("SELECT starting_block_number from eth.known_gaps WHERE starting_block_number = %d AND ending_block_number = %d", startBlock, endBlock)

	_, queryErr := stateDiff.QueryDb(queryString) // Figure out the string.
	t.Logf("Updated Known Gaps table starting from, %d, and ending at, %d", startBlock, endBlock)
	require.NoError(t, queryErr)

}
