package statediff

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/stretchr/testify/require"
)

var (
	knownGapsFilePath = "./known_gaps.sql"
)

type gapValues struct {
	knownErrorBlocksStart int64
	knownErrorBlocksEnd   int64
	expectedDif           int64
	processingKey         int64
}

// Add clean db
// Test for failures when they are expected, when we go from smaller block to larger block
// We should no longer see the smaller block in DB
func TestKnownGaps(t *testing.T) {

	tests := []gapValues{
		// Known Gaps
		{knownErrorBlocksStart: 115, knownErrorBlocksEnd: 120, expectedDif: 1, processingKey: 1},
		/// Same tests as above with a new expected DIF
		{knownErrorBlocksStart: 1150, knownErrorBlocksEnd: 1200, expectedDif: 2, processingKey: 2},
		// Test update when block number is larger!!
		{knownErrorBlocksStart: 1150, knownErrorBlocksEnd: 1204, expectedDif: 2, processingKey: 2},
		// Update when processing key is different!
		{knownErrorBlocksStart: 1150, knownErrorBlocksEnd: 1204, expectedDif: 2, processingKey: 10},
	}

	testWriteToDb(t, tests, true)
	testWriteToFile(t, tests, true)
	testFindAndUpdateGaps(t, true)
}

// test writing blocks to the DB
func testWriteToDb(t *testing.T, tests []gapValues, wipeDbBeforeStart bool) {
	t.Log("Starting Write to DB test")
	db := setupDb(t)

	// Clear Table first, this is needed because we updated an entry to have a larger endblock number
	// so we can't find the original start and endblock pair.
	if wipeDbBeforeStart {
		t.Log("Cleaning up eth_meta.known_gaps table")
		db.Exec(context.Background(), "DELETE FROM eth_meta.known_gaps")
	}

	for _, tc := range tests {
		// Create an array with knownGaps based on user inputs
		knownGaps := KnownGapsState{
			processingKey:      tc.processingKey,
			expectedDifference: big.NewInt(tc.expectedDif),
			db:                 db,
			statediffMetrics:   RegisterStatediffMetrics(metrics.DefaultRegistry),
		}
		service := &Service{
			KnownGaps: knownGaps,
		}
		knownErrorBlocks := (make([]*big.Int, 0))
		knownErrorBlocks = createKnownErrorBlocks(knownErrorBlocks, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
		service.KnownGaps.knownErrorBlocks = knownErrorBlocks
		// Upsert
		testCaptureErrorBlocks(t, service)
		// Validate that the upsert was done correctly.
		validateUpsert(t, service, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
	}
	tearDown(t, db)

}

// test writing blocks to file and then inserting them to DB
func testWriteToFile(t *testing.T, tests []gapValues, wipeDbBeforeStart bool) {
	t.Log("Starting write to file test")
	db := setupDb(t)
	// Clear Table first, this is needed because we updated an entry to have a larger endblock number
	// so we can't find the original start and endblock pair.
	if wipeDbBeforeStart {
		t.Log("Cleaning up eth_meta.known_gaps table")
		db.Exec(context.Background(), "DELETE FROM eth_meta.known_gaps")
	}
	if _, err := os.Stat(knownGapsFilePath); err == nil {
		err := os.Remove(knownGapsFilePath)
		if err != nil {
			t.Fatal("Can't delete local file")
		}
	}
	tearDown(t, db)
	for _, tc := range tests {
		knownGaps := KnownGapsState{
			processingKey:      tc.processingKey,
			expectedDifference: big.NewInt(tc.expectedDif),
			writeFilePath:      knownGapsFilePath,
			statediffMetrics:   RegisterStatediffMetrics(metrics.DefaultRegistry),
			db:                 nil, // Only set to nil to be verbose that we can't use it
		}
		service := &Service{
			KnownGaps: knownGaps,
		}
		knownErrorBlocks := (make([]*big.Int, 0))
		knownErrorBlocks = createKnownErrorBlocks(knownErrorBlocks, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
		service.KnownGaps.knownErrorBlocks = knownErrorBlocks

		testCaptureErrorBlocks(t, service)
		newDb := setupDb(t)
		service.KnownGaps.db = newDb
		if service.KnownGaps.sqlFileWaitingForWrite {
			writeErr := service.KnownGaps.writeSqlFileStmtToDb()
			require.NoError(t, writeErr)
		}

		// Validate that the upsert was done correctly.
		validateUpsert(t, service, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
		tearDown(t, newDb)
	}
}

// Find a gap, if no gaps exist, it will create an arbitrary one
func testFindAndUpdateGaps(t *testing.T, wipeDbBeforeStart bool) {
	db := setupDb(t)

	if wipeDbBeforeStart {
		db.Exec(context.Background(), "DELETE FROM eth_meta.known_gaps")
	}
	knownGaps := KnownGapsState{
		processingKey:      1,
		expectedDifference: big.NewInt(1),
		db:                 db,
		statediffMetrics:   RegisterStatediffMetrics(metrics.DefaultRegistry),
	}
	service := &Service{
		KnownGaps: knownGaps,
	}

	latestBlockInDb, err := service.KnownGaps.queryDbToBigInt("SELECT MAX(block_number) FROM eth.header_cids")
	if err != nil {
		t.Skip("Can't find a block in the eth.header_cids table.. Please put one there")
	}

	// Add the gapDifference for testing purposes
	gapDifference := big.NewInt(10)     // Set a difference between latestBlock in DB and on Chain
	expectedDifference := big.NewInt(1) // Set what the expected difference between latestBlock in DB and on Chain should be

	latestBlockOnChain := big.NewInt(0)
	latestBlockOnChain.Add(latestBlockInDb, gapDifference)

	t.Log("The latest block on the chain is: ", latestBlockOnChain)
	t.Log("The latest block on the DB is: ", latestBlockInDb)

	gapUpsertErr := service.KnownGaps.findAndUpdateGaps(latestBlockOnChain, expectedDifference, 0)
	require.NoError(t, gapUpsertErr)

	startBlock := big.NewInt(0)
	endBlock := big.NewInt(0)

	startBlock.Add(latestBlockInDb, expectedDifference)
	endBlock.Sub(latestBlockOnChain, expectedDifference)
	validateUpsert(t, service, startBlock.Int64(), endBlock.Int64())

}

// test capturing missed blocks
func testCaptureErrorBlocks(t *testing.T, service *Service) {
	service.KnownGaps.captureErrorBlocks(service.KnownGaps.knownErrorBlocks)
}

// Helper function to create an array of gaps given a start and end block
func createKnownErrorBlocks(knownErrorBlocks []*big.Int, knownErrorBlocksStart int64, knownErrorBlocksEnd int64) []*big.Int {
	for i := knownErrorBlocksStart; i <= knownErrorBlocksEnd; i++ {
		knownErrorBlocks = append(knownErrorBlocks, big.NewInt(i))
	}
	return knownErrorBlocks
}

// Make sure the upsert was performed correctly
func validateUpsert(t *testing.T, service *Service, startingBlock int64, endingBlock int64) {
	t.Logf("Starting to query blocks: %d - %d", startingBlock, endingBlock)
	queryString := fmt.Sprintf("SELECT starting_block_number from eth_meta.known_gaps WHERE starting_block_number = %d AND ending_block_number = %d", startingBlock, endingBlock)

	_, queryErr := service.KnownGaps.queryDb(queryString) // Figure out the string.
	t.Logf("Updated Known Gaps table starting from, %d, and ending at, %d", startingBlock, endingBlock)
	require.NoError(t, queryErr)
}

// Create a DB object to use
func setupDb(t *testing.T) sql.Database {
	db, err := postgres.SetupSQLXDB()
	if err != nil {
		t.Error("Can't create a DB connection....")
		t.Fatal(err)
	}
	return db
}

// Teardown the DB
func tearDown(t *testing.T, db sql.Database) {
	t.Log("Starting tearDown")
	db.Close()
}
