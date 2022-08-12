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

package file_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
)

func setupIndexer(t *testing.T) {
	if _, err := os.Stat(file.SQLTestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.SQLTestConfig.FilePath)
		require.NoError(t, err)
	}

	if _, err := os.Stat(file.SQLTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.SQLTestConfig.WatchedAddressesFilePath)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), mocks.TestConfig, file.SQLTestConfig)
	require.NoError(t, err)

	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T) {
	setupIndexer(t)
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
		if err := ind.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx.(*file.BatchTx).BlockNumber)
}

func setupSQLNonCanonical(t *testing.T) {
	setupIndexer(t)
	test.SetupTestDataNonCanonical(t, ind)
}

func TestSQLFileIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexHeaderIPLDs(t)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexTransactionIPLDs(t)
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexLogIPLDs(t)
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexReceiptIPLDs(t)
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexStateIPLDs(t)
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setup(t)
		dumpFileData(t)
		defer tearDown(t)

		testPublishAndIndexStorageIPLDs(t)
	})
}

func TestSQLFileIndexerNonCanonical(t *testing.T) {
	t.Run("Publish and index header", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexHeaderNonCanonical(t, db)
	})

	t.Run("Publish and index transactions", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexTransactionsNonCanonical(t, db)
	})

	t.Run("Publish and index receipts", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexReceiptsNonCanonical(t, db)
	})

	t.Run("Publish and index logs", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexLogsNonCanonical(t, db)
	})

	t.Run("Publish and index state nodes", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexStateNonCanonical(t, db)
	})

	t.Run("Publish and index storage nodes", func(t *testing.T) {
		setupSQLNonCanonical(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestPublishAndIndexStorageNonCanonical(t, db)
	})
}

func TestSQLFileWatchAddressMethods(t *testing.T) {
	setupIndexer(t)
	defer tearDown(t)

	t.Run("Load watched addresses (empty table)", func(t *testing.T) {
		testLoadEmptyWatchedAddresses(t)
	})

	t.Run("Insert watched addresses", func(t *testing.T) {
		testInsertWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
		testInsertAlreadyWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Remove watched addresses", func(t *testing.T) {
		testRemoveWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
		testRemoveNonWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Set watched addresses", func(t *testing.T) {
		testSetWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
		testSetAlreadyWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Load watched addresses", func(t *testing.T) {
		testLoadWatchedAddresses(t)
	})

	t.Run("Clear watched addresses", func(t *testing.T) {
		testClearWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})

	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
		testClearEmptyWatchedAddresses(t, resetAndDumpWatchedAddressesFileData)
	})
}
