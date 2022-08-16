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
	"math/big"
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
	setupSQLXIndexer(t)
	test.SetupTestDataNonCanonical(t, ind)
}

// Test indexer for a canonical block
func TestSQLXIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexHeaderIPLDs(t, db)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexTransactionIPLDs(t, db)
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexLogIPLDs(t, db)
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexReceiptIPLDs(t, db)
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexStateIPLDs(t, db)
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setupSQLX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexStorageIPLDs(t, db)
	})
}

// Test indexer for a canonical + a non-canonical block at London height + a non-canonical block at London height + 1
func TestSQLXIndexerNonCanonical(t *testing.T) {
	t.Run("Publish and index header", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexHeaderNonCanonical(t, db)
	})

	t.Run("Publish and index transactions", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexTransactionsNonCanonical(t, db)
	})

	t.Run("Publish and index receipts", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexReceiptsNonCanonical(t, db)
	})

	t.Run("Publish and index logs", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexLogsNonCanonical(t, db)
	})

	t.Run("Publish and index state nodes", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexStateNonCanonical(t, db)
	})

	t.Run("Publish and index storage nodes", func(t *testing.T) {
		setupSQLXNonCanonical(t)
		defer tearDown(t)
		defer checkTxClosure(t, 0, 0, 0)

		test.TestPublishAndIndexStorageNonCanonical(t, db)
	})
}

func TestSQLXWatchAddressMethods(t *testing.T) {
	setupSQLXIndexer(t)
	defer tearDown(t)
	defer checkTxClosure(t, 0, 0, 0)

	t.Run("Load watched addresses (empty table)", func(t *testing.T) {
		test.TestLoadEmptyWatchedAddresses(t, ind)
	})

	t.Run("Insert watched addresses", func(t *testing.T) {
		args := mocks.GetInsertWatchedAddressesArgs()
		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(mocks.WatchedAt1)))
		require.NoError(t, err)

		test.TestInsertWatchedAddresses(t, db)
	})

	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
		args := mocks.GetInsertAlreadyWatchedAddressesArgs()
		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(mocks.WatchedAt2)))
		require.NoError(t, err)

		test.TestInsertAlreadyWatchedAddresses(t, db)
	})

	t.Run("Remove watched addresses", func(t *testing.T) {
		args := mocks.GetRemoveWatchedAddressesArgs()
		err = ind.RemoveWatchedAddresses(args)
		require.NoError(t, err)

		test.TestRemoveWatchedAddresses(t, db)
	})

	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
		args := mocks.GetRemoveNonWatchedAddressesArgs()
		err = ind.RemoveWatchedAddresses(args)
		require.NoError(t, err)

		test.TestRemoveNonWatchedAddresses(t, db)
	})

	t.Run("Set watched addresses", func(t *testing.T) {
		args := mocks.GetSetWatchedAddressesArgs()
		err = ind.SetWatchedAddresses(args, big.NewInt(int64(mocks.WatchedAt2)))
		require.NoError(t, err)

		test.TestSetWatchedAddresses(t, db)
	})

	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
		args := mocks.GetSetAlreadyWatchedAddressesArgs()
		err = ind.SetWatchedAddresses(args, big.NewInt(int64(mocks.WatchedAt3)))
		require.NoError(t, err)

		test.TestSetAlreadyWatchedAddresses(t, db)
	})

	t.Run("Load watched addresses", func(t *testing.T) {
		test.TestLoadWatchedAddresses(t, ind)
	})

	t.Run("Clear watched addresses", func(t *testing.T) {
		err = ind.ClearWatchedAddresses()
		require.NoError(t, err)

		test.TestClearWatchedAddresses(t, db)
	})

	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
		err = ind.ClearWatchedAddresses()
		require.NoError(t, err)

		test.TestClearEmptyWatchedAddresses(t, db)
	})
}
