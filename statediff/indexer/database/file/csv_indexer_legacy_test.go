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

	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
)

const dbDirectory = "/file_indexer"
const pgCopyStatement = `COPY %s FROM '%s' CSV`

func setupCSVLegacy(t *testing.T) {
	mockLegacyBlock = legacyData.MockBlock
	legacyHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, legacyData.MockHeaderRlp, multihash.KECCAK_256)
	file.CSVTestConfig.OutputDir = "./statediffing_legacy_test"

	if _, err := os.Stat(file.CSVTestConfig.OutputDir); !errors.Is(err, os.ErrNotExist) {
		err := os.RemoveAll(file.CSVTestConfig.OutputDir)
		require.NoError(t, err)
	}

	ind, err := file.NewStateDiffIndexer(context.Background(), legacyData.Config, file.CSVTestConfig)
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

func dumpCSVFileData(t *testing.T) {
	outputDir := filepath.Join(dbDirectory, file.CSVTestConfig.OutputDir)

	for _, tbl := range file.Tables {
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

		_, err = sqlxdb.Exec(stmt)
		require.NoError(t, err)
	}
}

func dumpWatchedAddressesCSVFileData(t *testing.T) {
	outputFilePath := filepath.Join(dbDirectory, file.CSVTestConfig.WatchedAddressesFilePath)
	stmt := fmt.Sprintf(pgCopyStatement, types.TableWatchedAddresses.Name, outputFilePath)

	_, err = sqlxdb.Exec(stmt)
	require.NoError(t, err)
}

func tearDownCSV(t *testing.T) {
	file.TearDownDB(t, sqlxdb)

	err := os.RemoveAll(file.CSVTestConfig.OutputDir)
	require.NoError(t, err)

	if err := os.Remove(file.CSVTestConfig.WatchedAddressesFilePath); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}

	err = sqlxdb.Close()
	require.NoError(t, err)
}

func TestCSVFileIndexerLegacy(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupCSVLegacy(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)
		pgStr := `SELECT cid, td, reward, block_hash, coinbase
				FROM eth.header_cids
				WHERE block_number = $1`
		// check header was properly indexed
		type res struct {
			CID       string
			TD        string
			Reward    string
			BlockHash string `db:"block_hash"`
			Coinbase  string `db:"coinbase"`
		}
		header := new(res)
		err = sqlxdb.QueryRowx(pgStr, legacyData.BlockNumber.Uint64()).StructScan(header)
		require.NoError(t, err)

		require.Equal(t, legacyHeaderCID.String(), header.CID)
		require.Equal(t, legacyData.MockBlock.Difficulty().String(), header.TD)
		require.Equal(t, "5000000000000011250", header.Reward)
		require.Equal(t, legacyData.MockBlock.Coinbase().String(), header.Coinbase)
		require.Nil(t, legacyData.MockHeader.BaseFee)
	})
}
