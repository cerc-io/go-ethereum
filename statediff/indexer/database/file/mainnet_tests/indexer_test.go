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

package mainnet_tests

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	sqlxdb    *sqlx.DB
	chainConf = params.MainnetChainConfig
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
	if os.Getenv("STATEDIFF_DB") != "file" {
		fmt.Println("Skipping statediff .sql file writing mode test")
		os.Exit(0)
	}
}

func TestPushBlockAndState(t *testing.T) {
	conf := test_helpers.DefaultTestConfig
	rawURL := os.Getenv(test_helpers.TEST_RAW_URL)
	if rawURL == "" {
		fmt.Printf("Warning: no raw url configured for statediffing mainnet tests, will look for local file and"+
			"then try default endpoint (%s)\r\n", test_helpers.DefaultTestConfig.RawURL)
	} else {
		conf.RawURL = rawURL
	}
	for _, blockNumber := range test_helpers.ProblemBlocks {
		conf.BlockNumber = big.NewInt(blockNumber)
		tb, trs, err := test_helpers.TestBlockAndReceipts(conf)
		require.NoError(t, err)
		testPushBlockAndState(t, tb, trs)
	}
	testBlock, testReceipts, err := test_helpers.TestBlockAndReceiptsFromEnv(conf)
	require.NoError(t, err)
	testPushBlockAndState(t, testBlock, testReceipts)
}

func testPushBlockAndState(t *testing.T, block *types.Block, receipts types.Receipts) {
	t.Run("Test PushBlock and PushStateNode", func(t *testing.T) {
		setup(t, block, receipts)
		dumpData(t)
		tearDown(t)
	})
}

func setup(t *testing.T, testBlock *types.Block, testReceipts types.Receipts) {
	if _, err := os.Stat(file.CSVTestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.CSVTestConfig.FilePath)
		require.NoError(t, err)
	}
	ind, err := file.NewStateDiffIndexer(context.Background(), chainConf, file.CSVTestConfig)
	require.NoError(t, err)
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		testBlock,
		testReceipts,
		testBlock.Difficulty())
	require.NoError(t, err)

	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
		if err := ind.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, testBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, testBlock.Number().String(), tx.(*file.BatchTx).BlockNumber)

	connStr := postgres.DefaultConfig.DbConnectionString()

	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func dumpData(t *testing.T) {
	sqlFileBytes, err := os.ReadFile(file.CSVTestConfig.FilePath)
	require.NoError(t, err)

	_, err = sqlxdb.Exec(string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	file.TearDownDB(t, sqlxdb)
	err := os.Remove(file.CSVTestConfig.FilePath)
	require.NoError(t, err)
	err = sqlxdb.Close()
	require.NoError(t, err)
}
