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

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	err       error
	db        sql.Database
	ind       interfaces.StateDiffIndexer
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
	conf := test_helpers.GetTestConfig()

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
		setupMainnetIndexer(t)
		defer dumpData(t)
		defer tearDown(t)

		test.TestBlock(t, ind, block, receipts)
	})
}

func setupMainnetIndexer(t *testing.T) {
	if _, err := os.Stat(file.CSVTestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.CSVTestConfig.FilePath)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), chainConf, file.CSVTestConfig)
	require.NoError(t, err)

	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
}

func dumpData(t *testing.T) {
	sqlFileBytes, err := os.ReadFile(file.CSVTestConfig.FilePath)
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	test_helpers.TearDownDB(t, db)
	require.NoError(t, db.Close())

	require.NoError(t, os.Remove(file.CSVTestConfig.FilePath))
}
