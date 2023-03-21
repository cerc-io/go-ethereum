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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared/schema"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

const dbDirectory = "/file_indexer"
const pgCopyStatement = `COPY %s FROM '%s' CSV`

func setupLegacyCSVIndexer(t *testing.T) {
	if _, err := os.Stat(file.CSVTestConfig.OutputDir); !errors.Is(err, os.ErrNotExist) {
		err := os.RemoveAll(file.CSVTestConfig.OutputDir)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), test.LegacyConfig, file.CSVTestConfig)
	require.NoError(t, err)

	db, err = postgres.SetupSQLXDB()
	if err != nil {
		t.Fatal(err)
	}
}

func setupLegacyCSV(t *testing.T) {
	setupLegacyCSVIndexer(t)
	test.SetupLegacyTestData(t, ind)
}

func dumpCSVFileData(t *testing.T) {
	outputDir := filepath.Join(dbDirectory, file.CSVTestConfig.OutputDir)
	workingDir, err := os.Getwd()
	require.NoError(t, err)

	localOutputDir := filepath.Join(workingDir, file.CSVTestConfig.OutputDir)

	for _, tbl := range file.Tables {
		err := test_helpers.DedupFile(file.TableFilePath(localOutputDir, tbl.Name))
		require.NoError(t, err)

		var stmt string
		varcharColumns := tbl.VarcharColumns()
		if len(varcharColumns) > 0 {
			stmt = fmt.Sprintf(
				pgCopyStatement+" FORCE NOT NULL %s",
				tbl.Name,
				file.TableFilePath(outputDir, tbl.Name),
				strings.Join(varcharColumns, ", "),
			)
		} else {
			stmt = fmt.Sprintf(pgCopyStatement, tbl.Name, file.TableFilePath(outputDir, tbl.Name))
		}

		_, err = db.Exec(context.Background(), stmt)
		require.NoError(t, err)
	}
}

func resetAndDumpWatchedAddressesCSVFileData(t *testing.T) {
	test_helpers.TearDownDB(t, db)

	outputFilePath := filepath.Join(dbDirectory, file.CSVTestConfig.WatchedAddressesFilePath)
	stmt := fmt.Sprintf(pgCopyStatement, schema.TableWatchedAddresses.Name, outputFilePath)

	_, err = db.Exec(context.Background(), stmt)
	require.NoError(t, err)
}

func tearDownCSV(t *testing.T) {
	test_helpers.TearDownDB(t, db)
	require.NoError(t, db.Close())

	require.NoError(t, os.RemoveAll(file.CSVTestConfig.OutputDir))

	if err := os.Remove(file.CSVTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}
}

func TestLegacyCSVFileIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupLegacyCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		test.TestLegacyIndexer(t, db)
	})
}
