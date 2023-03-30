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
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	db  sql.Database
	err error
	ind interfaces.StateDiffIndexer
)

func setupLegacySQLIndexer(t *testing.T) {
	if _, err := os.Stat(file.SQLTestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.SQLTestConfig.FilePath)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), test.LegacyConfig, file.SQLTestConfig)
	require.NoError(t, err)

	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
}

func setupLegacySQL(t *testing.T) {
	setupLegacySQLIndexer(t)
	test.SetupLegacyTestData(t, ind)
}

func dumpFileData(t *testing.T) {
	err := test_helpers.DedupFile(file.SQLTestConfig.FilePath)
	require.NoError(t, err)

	sqlFileBytes, err := os.ReadFile(file.SQLTestConfig.FilePath)
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), string(sqlFileBytes))
	require.NoError(t, err)
}

func resetAndDumpWatchedAddressesFileData(t *testing.T) {
	test_helpers.TearDownDB(t, db)

	sqlFileBytes, err := os.ReadFile(file.SQLTestConfig.WatchedAddressesFilePath)
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	test_helpers.TearDownDB(t, db)
	require.NoError(t, db.Close())

	require.NoError(t, os.Remove(file.SQLTestConfig.FilePath))

	if err := os.Remove(file.SQLTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}
}

func TestLegacySQLFileIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupLegacySQL(t)
		dumpFileData(t)
		defer tearDown(t)

		test.TestLegacyIndexer(t, db)
	})
}
