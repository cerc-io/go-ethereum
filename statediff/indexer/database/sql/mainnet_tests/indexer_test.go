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
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
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
		tearDown(t)
	})
}

func setupDb(t *testing.T) (interfaces.StateDiffIndexer, error) {
	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
	ind, err = sql.NewStateDiffIndexer(context.Background(), chainConf, db)
	return ind, err
}

func setup(t *testing.T, testBlock *types.Block, testReceipts types.Receipts) (interfaces.StateDiffIndexer, interfaces.Batch) {
	ind, err = setupDb(t)
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
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, testBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, testBlock.Number().String(), tx.(*sql.BatchTx).BlockNumber)
}

func tearDown(t *testing.T) {
	require.Equal(t, int64(0), db.Stats().Idle())
	require.Equal(t, int64(0), db.Stats().InUse())
	require.Equal(t, int64(0), db.Stats().Open())
	sql.TearDownDB(t, db)
	err = ind.Close()
	require.NoError(t, err)
}
