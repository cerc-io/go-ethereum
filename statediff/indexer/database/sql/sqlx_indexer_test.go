// VulcanizeDB
// Copyright © 2019 Vulcanize

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

package sql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
)

func setupSQLXIndexer(t *testing.T) {
	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
	ind, err = sql.NewStateDiffIndexer(context.Background(), mocks.TestConfig, db)
	require.NoError(t, err)
}

func setupSQLX(t *testing.T) {
	setupSQLXIndexer(t)
	test.SetupTestData(t, ind)
}

func setupSQLXNonCanonical(t *testing.T) {
	setupPGXIndexer(t)
	test.SetupTestDataNonCanonical(t, ind)
}

// Test indexer for a canonical block
func TestSQLXIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexHeaderIPLDs(t, db)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexTransactionIPLDs(t, db)
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexLogIPLDs(t, db)
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexReceiptIPLDs(t, db)
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexStateIPLDs(t, db)
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexStorageIPLDs(t, db)
	})
}

// Test indexer for a canonical + a non-canonical block at London height + a non-canonical block at London height + 1
func TestSQLXIndexerNonCanonical(t *testing.T) {
	t.Run("Publish and index header", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexHeaderNonCanonical(t, db)
	})

	t.Run("Publish and index transactions", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexTransactionsNonCanonical(t, db)
	})

	t.Run("Publish and index receipts", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexReceiptsNonCanonical(t, db)
	})

	t.Run("Publish and index logs", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexLogsNonCanonical(t, db)
	})

	t.Run("Publish and index state nodes", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexStateNonCanonical(t, db)
	})

	t.Run("Publish and index storage nodes", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestPublishAndIndexStorageNonCanonical(t, db)
	})
}

// func TestSQLXWatchAddressMethods(t *testing.T) {
// 	setupSQLXIndexer(t)
// 	defer tearDown(t)
// 	defer checkTxClosure(t, 0, 0, 0)

// 	type res struct {
// 		Address      string `db:"address"`
// 		CreatedAt    uint64 `db:"created_at"`
// 		WatchedAt    uint64 `db:"watched_at"`
// 		LastFilledAt uint64 `db:"last_filled_at"`
// 	}
// 	pgStr := "SELECT * FROM eth_meta.watched_addresses"

// 	t.Run("Load watched addresses (empty table)", func(t *testing.T) {
// 		expectedData := []common.Address{}

// 		rows, err := ind.LoadWatchedAddresses()
// 		require.NoError(t, err)

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Insert watched addresses", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract1Address,
// 				CreatedAt: contract1CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 		}
// 		expectedData := []res{
// 			{
// 				Address:      contract1Address,
// 				CreatedAt:    contract1CreatedAt,
// 				WatchedAt:    watchedAt1,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract2Address,
// 				CreatedAt:    contract2CreatedAt,
// 				WatchedAt:    watchedAt1,
// 				LastFilledAt: lastFilledAt,
// 			},
// 		}

// 		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt1)))
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract3Address,
// 				CreatedAt: contract3CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 		}
// 		expectedData := []res{
// 			{
// 				Address:      contract1Address,
// 				CreatedAt:    contract1CreatedAt,
// 				WatchedAt:    watchedAt1,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract2Address,
// 				CreatedAt:    contract2CreatedAt,
// 				WatchedAt:    watchedAt1,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract3Address,
// 				CreatedAt:    contract3CreatedAt,
// 				WatchedAt:    watchedAt2,
// 				LastFilledAt: lastFilledAt,
// 			},
// 		}

// 		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Remove watched addresses", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract3Address,
// 				CreatedAt: contract3CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 		}
// 		expectedData := []res{
// 			{
// 				Address:      contract1Address,
// 				CreatedAt:    contract1CreatedAt,
// 				WatchedAt:    watchedAt1,
// 				LastFilledAt: lastFilledAt,
// 			},
// 		}

// 		err = ind.RemoveWatchedAddresses(args)
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract1Address,
// 				CreatedAt: contract1CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 		}
// 		expectedData := []res{}

// 		err = ind.RemoveWatchedAddresses(args)
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Set watched addresses", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract1Address,
// 				CreatedAt: contract1CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 			{
// 				Address:   contract3Address,
// 				CreatedAt: contract3CreatedAt,
// 			},
// 		}
// 		expectedData := []res{
// 			{
// 				Address:      contract1Address,
// 				CreatedAt:    contract1CreatedAt,
// 				WatchedAt:    watchedAt2,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract2Address,
// 				CreatedAt:    contract2CreatedAt,
// 				WatchedAt:    watchedAt2,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract3Address,
// 				CreatedAt:    contract3CreatedAt,
// 				WatchedAt:    watchedAt2,
// 				LastFilledAt: lastFilledAt,
// 			},
// 		}

// 		err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
// 		args := []sdtypes.WatchAddressArg{
// 			{
// 				Address:   contract4Address,
// 				CreatedAt: contract4CreatedAt,
// 			},
// 			{
// 				Address:   contract2Address,
// 				CreatedAt: contract2CreatedAt,
// 			},
// 			{
// 				Address:   contract3Address,
// 				CreatedAt: contract3CreatedAt,
// 			},
// 		}
// 		expectedData := []res{
// 			{
// 				Address:      contract4Address,
// 				CreatedAt:    contract4CreatedAt,
// 				WatchedAt:    watchedAt3,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract2Address,
// 				CreatedAt:    contract2CreatedAt,
// 				WatchedAt:    watchedAt3,
// 				LastFilledAt: lastFilledAt,
// 			},
// 			{
// 				Address:      contract3Address,
// 				CreatedAt:    contract3CreatedAt,
// 				WatchedAt:    watchedAt3,
// 				LastFilledAt: lastFilledAt,
// 			},
// 		}

// 		err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt3)))
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Load watched addresses", func(t *testing.T) {
// 		expectedData := []common.Address{
// 			common.HexToAddress(contract4Address),
// 			common.HexToAddress(contract2Address),
// 			common.HexToAddress(contract3Address),
// 		}

// 		rows, err := ind.LoadWatchedAddresses()
// 		require.NoError(t, err)

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Clear watched addresses", func(t *testing.T) {
// 		expectedData := []res{}

// 		err = ind.ClearWatchedAddresses()
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})

// 	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
// 		expectedData := []res{}

// 		err = ind.ClearWatchedAddresses()
// 		require.NoError(t, err)

// 		rows := []res{}
// 		err = db.Select(context.Background(), &rows, pgStr)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		expectTrue(t, len(rows) == len(expectedData))
// 		for idx, row := range rows {
// 			require.Equal(t, expectedData[idx], row)
// 		}
// 	})
// }
