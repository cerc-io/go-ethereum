// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package statediff

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

var (
	knownGapsInsert = "INSERT INTO eth_meta.known_gaps (starting_block_number, ending_block_number, checked_out, processing_key) " +
		"VALUES ('%s', '%s', %t, %d) " +
		"ON CONFLICT (starting_block_number) DO UPDATE SET (ending_block_number, processing_key) = ('%s', %d) " +
		"WHERE eth_meta.known_gaps.ending_block_number <= '%s';\n"
	dbQueryString        = "SELECT MAX(block_number) FROM eth.header_cids"
	defaultWriteFilePath = "./known_gaps.sql"
)

type KnownGapsState struct {
	// Should we check for gaps by looking at the DB and comparing the latest block with head
	checkForGaps bool
	// Arbitrary processingKey that can be used down the line to differentiate different geth nodes.
	processingKey int64
	// This number indicates the expected difference between blocks.
	// Currently, this is 1 since the geth node processes each block. But down the road this can be used in
	// Tandom with the processingKey to differentiate block processing logic.
	expectedDifference *big.Int
	// Indicates if Geth is in an error state
	// This is used to indicate the right time to upserts
	errorState bool
	// This array keeps track of errorBlocks as they occur.
	// When the errorState is false again, we can process these blocks.
	// Do we need a list, can we have /KnownStartErrorBlock and knownEndErrorBlock ints instead?
	knownErrorBlocks []*big.Int
	// The filepath to write SQL statements if we can't connect to the DB.
	writeFilePath string
	// DB object to use for reading and writing to the DB
	db sql.Database
	//Do we have entries in the local sql file that need to be written to the DB
	sqlFileWaitingForWrite bool
	// Metrics object used to track metrics.
	statediffMetrics statediffMetricsHandles
}

// Create a new KnownGapsState struct, currently unused.
func NewKnownGapsState(checkForGaps bool, processingKey int64, expectedDifference *big.Int,
	errorState bool, writeFilePath string, db sql.Database, statediffMetrics statediffMetricsHandles) *KnownGapsState {

	return &KnownGapsState{
		checkForGaps:       checkForGaps,
		processingKey:      processingKey,
		expectedDifference: expectedDifference,
		errorState:         errorState,
		writeFilePath:      writeFilePath,
		db:                 db,
		statediffMetrics:   statediffMetrics,
	}

}

func minMax(array []*big.Int) (*big.Int, *big.Int) {
	var max *big.Int = array[0]
	var min *big.Int = array[0]
	for _, value := range array {
		if max.Cmp(value) == -1 {
			max = value
		}
		if min.Cmp(value) == 1 {
			min = value
		}
	}
	return min, max
}

// This function actually performs the write of the known gaps. It will try to do the following, it only goes to the next step if a failure occurs.
// 1. Write to the DB directly.
// 2. Write to sql file locally.
// 3. Write to prometheus directly.
// 4. Logs and error.
func (kg *KnownGapsState) pushKnownGaps(startingBlockNumber *big.Int, endingBlockNumber *big.Int, checkedOut bool, processingKey int64) error {
	if startingBlockNumber.Cmp(endingBlockNumber) == 1 {
		return fmt.Errorf("Starting Block %d, is greater than ending block %d", startingBlockNumber, endingBlockNumber)
	}
	knownGap := models.KnownGapsModel{
		StartingBlockNumber: startingBlockNumber.String(),
		EndingBlockNumber:   endingBlockNumber.String(),
		CheckedOut:          checkedOut,
		ProcessingKey:       processingKey,
	}

	log.Info("Updating Metrics for the start and end block")
	kg.statediffMetrics.knownGapStart.Update(startingBlockNumber.Int64())
	kg.statediffMetrics.knownGapEnd.Update(endingBlockNumber.Int64())

	var writeErr error
	log.Info("Writing known gaps to the DB")
	if kg.db != nil {
		dbErr := kg.upsertKnownGaps(knownGap)
		if dbErr != nil {
			log.Warn("Error writing knownGaps to DB, writing them to file instead")
			writeErr = kg.upsertKnownGapsFile(knownGap)
		}
	} else {
		writeErr = kg.upsertKnownGapsFile(knownGap)
	}
	if writeErr != nil {
		log.Error("Unsuccessful when writing to a file", "Error", writeErr)
		log.Error("Updating Metrics for the start and end error block")
		log.Error("Unable to write the following Gaps to DB or File", "startBlock", startingBlockNumber, "endBlock", endingBlockNumber)
		kg.statediffMetrics.knownGapErrorStart.Update(startingBlockNumber.Int64())
		kg.statediffMetrics.knownGapErrorEnd.Update(endingBlockNumber.Int64())
	}
	return nil
}

// This is a simple wrapper function to write gaps from a knownErrorBlocks array.
func (kg *KnownGapsState) captureErrorBlocks(knownErrorBlocks []*big.Int) {
	startErrorBlock, endErrorBlock := minMax(knownErrorBlocks)

	log.Warn("The following Gaps were found", "knownErrorBlocks", knownErrorBlocks)
	log.Warn("Updating known Gaps table", "startErrorBlock", startErrorBlock, "endErrorBlock", endErrorBlock, "processingKey", kg.processingKey)
	kg.pushKnownGaps(startErrorBlock, endErrorBlock, false, kg.processingKey)

}

// Users provide the latestBlockInDb and the latestBlockOnChain
// as well as the expected difference. This function does some simple math.
// The expected difference for the time being is going to be 1, but as we run
// More geth nodes, the expected difference might fluctuate.
func isGap(latestBlockInDb *big.Int, latestBlockOnChain *big.Int, expectedDifference *big.Int) bool {
	latestBlock := big.NewInt(0)
	if latestBlock.Sub(latestBlockOnChain, expectedDifference).Cmp(latestBlockInDb) != 0 {
		log.Warn("We found a gap", "latestBlockInDb", latestBlockInDb, "latestBlockOnChain", latestBlockOnChain, "expectedDifference", expectedDifference)
		return true
	}
	return false

}

// This function will check for Gaps and update the DB if gaps are found.
// The processingKey will currently be set to 0, but as we start to leverage horizontal scaling
// It might be a useful parameter to update depending on the geth node.
// TODO:
// REmove the return value
// Write to file if err in writing to DB
func (kg *KnownGapsState) findAndUpdateGaps(latestBlockOnChain *big.Int, expectedDifference *big.Int, processingKey int64) error {
	// Make this global
	latestBlockInDb, err := kg.queryDbToBigInt(dbQueryString)
	if err != nil {
		return err
	}

	gapExists := isGap(latestBlockInDb, latestBlockOnChain, expectedDifference)
	if gapExists {
		startBlock := big.NewInt(0)
		endBlock := big.NewInt(0)
		startBlock.Add(latestBlockInDb, expectedDifference)
		endBlock.Sub(latestBlockOnChain, expectedDifference)

		log.Warn("Found Gaps starting at", "startBlock", startBlock, "endingBlock", endBlock)
		err := kg.pushKnownGaps(startBlock, endBlock, false, processingKey)
		if err != nil {
			log.Error("We were unable to write the following gap to the DB", "start Block", startBlock, "endBlock", endBlock, "error", err)
			return err
		}
	}

	return nil
}

// Upserts known gaps to the DB.
func (kg *KnownGapsState) upsertKnownGaps(knownGaps models.KnownGapsModel) error {
	_, err := kg.db.Exec(context.Background(), kg.db.InsertKnownGapsStm(),
		knownGaps.StartingBlockNumber, knownGaps.EndingBlockNumber, knownGaps.CheckedOut, knownGaps.ProcessingKey)
	if err != nil {
		return fmt.Errorf("error upserting known_gaps entry: %v", err)
	}
	log.Info("Successfully Wrote gaps to the DB", "startBlock", knownGaps.StartingBlockNumber, "endBlock", knownGaps.EndingBlockNumber)
	return nil
}

// Write upsert statement into a local file.
func (kg *KnownGapsState) upsertKnownGapsFile(knownGaps models.KnownGapsModel) error {
	insertStmt := []byte(fmt.Sprintf(knownGapsInsert, knownGaps.StartingBlockNumber, knownGaps.EndingBlockNumber, knownGaps.CheckedOut, knownGaps.ProcessingKey,
		knownGaps.EndingBlockNumber, knownGaps.ProcessingKey, knownGaps.EndingBlockNumber))
	log.Info("Trying to write file")
	if kg.writeFilePath == "" {
		kg.writeFilePath = defaultWriteFilePath
	}
	f, err := os.OpenFile(kg.writeFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Info("Unable to open a file for writing")
		return err
	}
	defer f.Close()

	if _, err = f.Write(insertStmt); err != nil {
		log.Info("Unable to open write insert statement to file")
		return err
	}
	log.Info("Wrote the gaps to a local SQL file")
	kg.sqlFileWaitingForWrite = true
	return nil
}

func (kg *KnownGapsState) writeSqlFileStmtToDb() error {
	log.Info("Writing the local SQL file for KnownGaps to the DB")
	file, err := ioutil.ReadFile(kg.writeFilePath)

	if err != nil {
		log.Error("Unable to open local SQL File for writing")
		return err
	}

	requests := strings.Split(string(file), ";")

	for _, request := range requests {
		_, err := kg.db.Exec(context.Background(), request)
		if err != nil {
			log.Error("Unable to run insert statement from file to the DB")
			return err
		}
	}
	if err := os.Truncate(kg.writeFilePath, 0); err != nil {
		log.Info("Failed to empty knownGaps file after inserting statements to the DB", "error", err)
	}
	kg.sqlFileWaitingForWrite = false
	return nil
}

// This is a simple wrapper function which will run QueryRow on the DB
func (kg *KnownGapsState) queryDb(queryString string) (string, error) {
	var ret string
	err := kg.db.QueryRow(context.Background(), queryString).Scan(&ret)
	if err != nil {
		log.Error(fmt.Sprint("Can't properly query the DB for query: ", queryString))
		return "", err
	}
	return ret, nil
}

// This function is a simple wrapper which will call QueryDb but the return value will be
// a big int instead of a string
func (kg *KnownGapsState) queryDbToBigInt(queryString string) (*big.Int, error) {
	ret := new(big.Int)
	res, err := kg.queryDb(queryString)
	if err != nil {
		return ret, err
	}
	ret, ok := ret.SetString(res, 10)
	if !ok {
		log.Error(fmt.Sprint("Can't turn the res ", res, "into a bigInt"))
		return ret, fmt.Errorf("Can't turn %s into a bigInt", res)
	}
	return ret, nil
}
