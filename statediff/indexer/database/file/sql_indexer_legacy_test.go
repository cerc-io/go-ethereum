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

	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
)

func setupLegacy(t *testing.T) {
	mockLegacyBlock = legacyData.MockBlock
	legacyHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, legacyData.MockHeaderRlp, multihash.KECCAK_256)

	if _, err := os.Stat(file.SQLTestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.SQLTestConfig.FilePath)
		require.NoError(t, err)
	}
	ind, err := file.NewStateDiffIndexer(context.Background(), legacyData.Config, file.SQLTestConfig)
	require.NoError(t, err)
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		mockLegacyBlock,
		legacyData.MockReceipts,
		legacyData.MockBlock.Difficulty())
	require.NoError(t, err)

	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
		if err := ind.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for _, node := range legacyData.StateDiffs {
		err = ind.PushStateNode(tx, node, legacyData.MockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, legacyData.BlockNumber.String(), tx.(*file.BatchTx).BlockNumber)

	connStr := postgres.DefaultConfig.DbConnectionString()
	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func dumpFileData(t *testing.T) {
	sqlFileBytes, err := os.ReadFile(file.SQLTestConfig.FilePath)
	require.NoError(t, err)

	_, err = sqlxdb.Exec(string(sqlFileBytes))
	require.NoError(t, err)
}

func resetAndDumpWatchedAddressesFileData(t *testing.T) {
	resetDB(t)

	sqlFileBytes, err := os.ReadFile(file.SQLTestConfig.WatchedAddressesFilePath)
	require.NoError(t, err)

	_, err = sqlxdb.Exec(string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	file.TearDownDB(t, sqlxdb)

	err := os.Remove(file.SQLTestConfig.FilePath)
	require.NoError(t, err)

	if err := os.Remove(file.SQLTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}

	err = sqlxdb.Close()
	require.NoError(t, err)
}

func TestSQLFileIndexerLegacy(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupLegacy(t)
		dumpFileData(t)
		defer tearDown(t)
		testLegacyPublishAndIndexHeaderIPLDs(t)
	})
}
