// VulcanizeDB
// Copyright © 2022 Vulcanize

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

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
)

func setupCSVIndexer(t *testing.T) {
	file.CSVTestConfig.OutputDir = "./statediffing_test"

	if _, err := os.Stat(file.CSVTestConfig.OutputDir); !errors.Is(err, os.ErrNotExist) {
		err := os.RemoveAll(file.CSVTestConfig.OutputDir)
		require.NoError(t, err)
	}

	if _, err := os.Stat(file.CSVTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.CSVTestConfig.WatchedAddressesFilePath)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), mocks.TestConfig, file.CSVTestConfig)
	require.NoError(t, err)

	connStr := postgres.DefaultConfig.DbConnectionString()
	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func setupCSV(t *testing.T) {
	setupCSVIndexer(t)
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

func TestCSVFileIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexHeaderIPLDs(t)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexTransactionIPLDs(t)
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexLogIPLDs(t)
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexReceiptIPLDs(t)
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexStateIPLDs(t)
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		testPublishAndIndexStorageIPLDs(t)
	})
}

func TestCSVFileWatchAddressMethods(t *testing.T) {
	setupCSVIndexer(t)
	defer tearDownCSV(t)

	t.Run("Load watched addresses (empty table)", func(t *testing.T) {
		testLoadEmptyWatchedAddresses(t)
	})

	t.Run("Insert watched addresses", func(t *testing.T) {
		testInsertWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
		testInsertAlreadyWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Remove watched addresses", func(t *testing.T) {
		testRemoveWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
		testRemoveNonWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Set watched addresses", func(t *testing.T) {
		testSetWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
		testSetAlreadyWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Load watched addresses", func(t *testing.T) {
		testLoadWatchedAddresses(t)
	})

	t.Run("Clear watched addresses", func(t *testing.T) {
		testClearWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})

	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
		testClearEmptyWatchedAddresses(t, func(t *testing.T) {
			file.TearDownDB(t, sqlxdb)
			dumpWatchedAddressesCSVFileData(t)
		})
	})
}
