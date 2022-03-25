package statediff

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
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

// Add clean db
// Test for failures when they are expected, when we go from smaller block to larger block
// We should no longer see the smaller block in DB
func TestKnownGaps(t *testing.T) {
	type gapValues struct {
		lastProcessedBlock    int64
		currentBlock          int64
		knownErrorBlocksStart int64
		knownErrorBlocksEnd   int64
		expectedDif           int64
		processingKey         int64
	}

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
		{lastProcessedBlock: 2000, knownErrorBlocksStart: 2002, knownErrorBlocksEnd: 2052, currentBlock: 2100, expectedDif: 2, processingKey: 10},
	}
	for _, tc := range tests {
		// Reuse processing key from expecteDiff
		testCaptureMissedBlocks(t, tc.lastProcessedBlock, tc.currentBlock, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd, tc.expectedDif, tc.expectedDif, false)
	}
	for _, tc := range tests {
		// Reuse processing key from expecteDiff
		testCaptureMissedBlocks(t, tc.lastProcessedBlock, tc.currentBlock, tc.knownErrorBlocksStart, tc.knownErrorBlocksEnd, tc.expectedDif, tc.expectedDif, true)
	}
}

// It also makes sure we properly calculate any missed gaps not in the known gaps lists
// either before or after the list.
func testCaptureMissedBlocks(t *testing.T, lastBlockProcessed int64, currentBlockNum int64, knownErrorBlocksStart int64, knownErrorBlocksEnd int64, expectedDif int64, processingKey int64, skipDb bool) {

	lastProcessedBlock := big.NewInt(lastBlockProcessed)
	currentBlock := big.NewInt(currentBlockNum)
	knownErrorBlocks := (make([]*big.Int, 0))

	checkGaps := knownErrorBlocksStart != 0 && knownErrorBlocksEnd != 0
	if checkGaps {
		for i := knownErrorBlocksStart; i <= knownErrorBlocksEnd; i++ {
			knownErrorBlocks = append(knownErrorBlocks, big.NewInt(i))
		}
	}

	// Comment out values which should not be used
	fileInd := setupFile(t)
	knownGaps := KnownGapsState{
		processingKey:      processingKey,
		expectedDifference: big.NewInt(expectedDif),
		fileIndexer:        fileInd,
	}
	stateDiff, err := setupDb(t)
	if err != nil {
		t.Fatal(err)
	}

	service := &Service{
		KnownGaps: knownGaps,
		indexer:   stateDiff,
	}
	service.capturedMissedBlocks(currentBlock, knownErrorBlocks, lastProcessedBlock)

	if checkGaps {

		if lastBlockProcessed+expectedDif == knownErrorBlocksStart && knownErrorBlocksEnd+expectedDif == currentBlockNum {
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)

		} else if lastBlockProcessed+expectedDif == knownErrorBlocksStart {
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)
			validateUpsert(t, stateDiff, knownErrorBlocksEnd+expectedDif, currentBlockNum-expectedDif)

		} else if knownErrorBlocksEnd+expectedDif == currentBlockNum {
			validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, knownErrorBlocksStart-expectedDif)
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)

		} else {
			validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, knownErrorBlocksStart-expectedDif)
			validateUpsert(t, stateDiff, knownErrorBlocksStart, knownErrorBlocksEnd)
			validateUpsert(t, stateDiff, knownErrorBlocksEnd+expectedDif, currentBlockNum-expectedDif)

		}
	} else {
		validateUpsert(t, stateDiff, lastBlockProcessed+expectedDif, currentBlockNum-expectedDif)
	}

	tearDown(t, stateDiff)
}

func validateUpsert(t *testing.T, stateDiff *sql.StateDiffIndexer, startingBlock int64, endingBlock int64) {
	t.Logf("Starting to query blocks: %d - %d", startingBlock, endingBlock)
	queryString := fmt.Sprintf("SELECT starting_block_number from eth.known_gaps WHERE starting_block_number = %d AND ending_block_number = %d", startingBlock, endingBlock)

	_, queryErr := stateDiff.QueryDb(queryString) // Figure out the string.
	t.Logf("Updated Known Gaps table starting from, %d, and ending at, %d", startingBlock, endingBlock)
	require.NoError(t, queryErr)
}

// Create a DB object to use
func setupDb(t *testing.T) (*sql.StateDiffIndexer, error) {
	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
	stateDiff, err := sql.NewStateDiffIndexer(context.Background(), chainConf, db)
	return stateDiff, err
}

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
