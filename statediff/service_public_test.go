package statediff

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/stretchr/testify/require"
)

var (
	db        sql.Database
	err       error
	indexer   interfaces.StateDiffIndexer
	chainConf = params.MainnetChainConfig
)

type gapValues struct {
	lastProcessedBlock    int64
	currentBlock          int64
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
		// Unprocessed gaps before and after knownErrorBlock
		{lastProcessedBlock: 110, knownErrorBlocksStart: 115, knownErrorBlocksEnd: 120, currentBlock: 125, expectedDif: 1, processingKey: 1},
		// No knownErrorBlocks
		{lastProcessedBlock: 130, knownErrorBlocksStart: 0, knownErrorBlocksEnd: 0, currentBlock: 140, expectedDif: 1, processingKey: 1},
		// No gaps before or after knownErrorBlocks
		{lastProcessedBlock: 150, knownErrorBlocksStart: 151, knownErrorBlocksEnd: 159, currentBlock: 160, expectedDif: 1, processingKey: 1},
		// gaps before knownErrorBlocks but not after
		{lastProcessedBlock: 170, knownErrorBlocksStart: 180, knownErrorBlocksEnd: 189, currentBlock: 190, expectedDif: 1, processingKey: 1},
		// gaps after knownErrorBlocks but not before
		{lastProcessedBlock: 200, knownErrorBlocksStart: 201, knownErrorBlocksEnd: 205, currentBlock: 210, expectedDif: 1, processingKey: 1},
		/// Same tests as above with a new expected DIF
		// Unprocessed gaps before and after knownErrorBlock
		{lastProcessedBlock: 1100, knownErrorBlocksStart: 1150, knownErrorBlocksEnd: 1200, currentBlock: 1250, expectedDif: 2, processingKey: 2},
		// No knownErrorBlocks
		{lastProcessedBlock: 1300, knownErrorBlocksStart: 0, knownErrorBlocksEnd: 0, currentBlock: 1400, expectedDif: 2, processingKey: 2},
		// No gaps before or after knownErrorBlocks
		{lastProcessedBlock: 1500, knownErrorBlocksStart: 1502, knownErrorBlocksEnd: 1598, currentBlock: 1600, expectedDif: 2, processingKey: 2},
		// gaps before knownErrorBlocks but not after
		{lastProcessedBlock: 1700, knownErrorBlocksStart: 1800, knownErrorBlocksEnd: 1898, currentBlock: 1900, expectedDif: 2, processingKey: 2},
		// gaps after knownErrorBlocks but not before
		{lastProcessedBlock: 2000, knownErrorBlocksStart: 2002, knownErrorBlocksEnd: 2050, currentBlock: 2100, expectedDif: 2, processingKey: 2},
		// Test update when block number is larger!!
		{lastProcessedBlock: 2000, knownErrorBlocksStart: 2002, knownErrorBlocksEnd: 2052, currentBlock: 2100, expectedDif: 2, processingKey: 2},
		// Update when processing key is different!
		{lastProcessedBlock: 2000, knownErrorBlocksStart: 2002, knownErrorBlocksEnd: 2052, currentBlock: 2100, expectedDif: 2, processingKey: 10},
	}

	testWriteToDb(t, tests, true)
	testWriteToFile(t, tests, true)
}

// test writing blocks to the DB
func testWriteToDb(t *testing.T, tests []gapValues, wipeDbBeforeStart bool) {
	stateDiff, db, err := setupDb(t)
	require.NoError(t, err)

	// Clear Table first, this is needed because we updated an entry to have a larger endblock number
	// so we can't find the original start and endblock pair.
	if wipeDbBeforeStart {
		db.Exec(context.Background(), "DELETE FROM eth.known_gaps")
	}

	for _, tc := range tests {
		// Create an array with knownGaps based on user inputs
		checkGaps := tc.knownErrorBlocksStart != 0 && tc.knownErrorBlocksEnd != 0
		knownErrorBlocks := (make([]*big.Int, 0))
		if checkGaps {
			knownErrorBlocks = createKnownErrorBlocks(knownErrorBlocks, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
		}
		// Upsert
		testCaptureMissedBlocks(t, tc.lastProcessedBlock, tc.currentBlock, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd,
			tc.expectedDif, tc.processingKey, stateDiff, knownErrorBlocks, nil)
		// Validate that the upsert was done correctly.
		callValidateUpsert(t, checkGaps, stateDiff, tc.lastProcessedBlock, tc.currentBlock, tc.expectedDif, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
	}
	tearDown(t, stateDiff)

}

// test writing blocks to file and then inserting them to DB
func testWriteToFile(t *testing.T, tests []gapValues, wipeDbBeforeStart bool) {
	stateDiff, db, err := setupDb(t)
	require.NoError(t, err)

	// Clear Table first, this is needed because we updated an entry to have a larger endblock number
	// so we can't find the original start and endblock pair.
	if wipeDbBeforeStart {
		db.Exec(context.Background(), "DELETE FROM eth.known_gaps")
	}

	tearDown(t, stateDiff)
	for _, tc := range tests {
		// Reuse processing key from expecteDiff
		checkGaps := tc.knownErrorBlocksStart != 0 && tc.knownErrorBlocksEnd != 0
		knownErrorBlocks := (make([]*big.Int, 0))
		if checkGaps {
			knownErrorBlocks = createKnownErrorBlocks(knownErrorBlocks, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
		}

		fileInd := setupFile(t)
		testCaptureMissedBlocks(t, tc.lastProcessedBlock, tc.currentBlock, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd,
			tc.expectedDif, tc.processingKey, stateDiff, knownErrorBlocks, fileInd)
		fileInd.Close()

		newStateDiff, db, _ := setupDb(t)

		file, ioErr := ioutil.ReadFile(file.TestConfig.FilePath)
		require.NoError(t, ioErr)

		requests := strings.Split(string(file), ";")

		// Skip the first two enteries
		for _, request := range requests[2:] {
			_, err := db.Exec(context.Background(), request)
			require.NoError(t, err)
		}
		callValidateUpsert(t, checkGaps, newStateDiff, tc.lastProcessedBlock, tc.currentBlock, tc.expectedDif, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd)
	}
}

// test capturing missed blocks
func testCaptureMissedBlocks(t *testing.T, lastBlockProcessed int64, currentBlockNum int64, knownErrorBlocksStart int64, knownErrorBlocksEnd int64, expectedDif int64, processingKey int64,
	stateDiff *sql.StateDiffIndexer, knownErrorBlocks []*big.Int, fileInd interfaces.StateDiffIndexer) {

	lastProcessedBlock := big.NewInt(lastBlockProcessed)
	currentBlock := big.NewInt(currentBlockNum)

	knownGaps := KnownGapsState{
		processingKey:      processingKey,
		expectedDifference: big.NewInt(expectedDif),
		fileIndexer:        fileInd,
	}
	service := &Service{
		KnownGaps: knownGaps,
		indexer:   stateDiff,
	}
	service.captureMissedBlocks(currentBlock, knownErrorBlocks, lastProcessedBlock)
}

// Helper function to create an array of gaps given a start and end block
func createKnownErrorBlocks(knownErrorBlocks []*big.Int, knownErrorBlocksStart int64, knownErrorBlocksEnd int64) []*big.Int {
	for i := knownErrorBlocksStart; i <= knownErrorBlocksEnd; i++ {
		knownErrorBlocks = append(knownErrorBlocks, big.NewInt(i))
	}
	return knownErrorBlocks
}

// This function will call the validateUpsert function based on various conditions.
func callValidateUpsert(t *testing.T, checkGaps bool, stateDiff *sql.StateDiffIndexer,
	lastBlockProcessed int64, currentBlockNum int64, expectedDif int64, knownErrorBlocksStart int64, knownErrorBlocksEnd int64) {
	// If there are gaps in knownErrorBlocks array
	if checkGaps {

		// If there are no unexpected gaps before or after the entries in the knownErrorBlocks array
		// Only handle the knownErrorBlocks Array
		if lastBlockProcessed+expectedDif == knownErrorBlocksStart && knownErrorBlocksEnd+expectedDif == currentBlockNum {
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)

			// If there are gaps after knownErrorBlocks array, process them
		} else if lastBlockProcessed+expectedDif == knownErrorBlocksStart {
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)
			validateUpsert(t, stateDiff, knownErrorBlocksEnd+expectedDif, currentBlockNum-expectedDif)

			// If there are gaps before knownErrorBlocks array, process them
		} else if knownErrorBlocksEnd+expectedDif == currentBlockNum {
			validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, knownErrorBlocksStart-expectedDif)
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)

			// if there are gaps before, after, and within the knownErrorBlocks array,handle all the errors.
		} else {
			validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, knownErrorBlocksStart-expectedDif)
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)
			validateUpsert(t, stateDiff, knownErrorBlocksEnd+expectedDif, currentBlockNum-expectedDif)

		}
	} else {
		validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, currentBlockNum-expectedDif)
	}

}

// Make sure the upsert was performed correctly
func validateUpsert(t *testing.T, stateDiff *sql.StateDiffIndexer, startingBlock int64, endingBlock int64) {
	t.Logf("Starting to query blocks: %d - %d", startingBlock, endingBlock)
	queryString := fmt.Sprintf("SELECT starting_block_number from eth.known_gaps WHERE starting_block_number = %d AND ending_block_number = %d", startingBlock, endingBlock)

	_, queryErr := stateDiff.QueryDb(queryString) // Figure out the string.
	t.Logf("Updated Known Gaps table starting from, %d, and ending at, %d", startingBlock, endingBlock)
	require.NoError(t, queryErr)
}

// Create a DB object to use
func setupDb(t *testing.T) (*sql.StateDiffIndexer, sql.Database, error) {
	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
	stateDiff, err := sql.NewStateDiffIndexer(context.Background(), chainConf, db)
	return stateDiff, db, err
}

// Create a file statediff indexer.
func setupFile(t *testing.T) interfaces.StateDiffIndexer {
	if _, err := os.Stat(file.TestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.TestConfig.FilePath)
		require.NoError(t, err)
	}
	indexer, err = file.NewStateDiffIndexer(context.Background(), mocks.TestConfig, file.TestConfig)
	require.NoError(t, err)
	return indexer
}

// Teardown the DB
func tearDown(t *testing.T, stateDiff *sql.StateDiffIndexer) {
	t.Log("Starting tearDown")
	sql.TearDownDB(t, db)
	err := stateDiff.Close()
	require.NoError(t, err)
}
