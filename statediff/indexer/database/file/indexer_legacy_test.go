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
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ipfs/go-cid"
	"github.com/jmoiron/sqlx"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	legacyData      = mocks.NewLegacyData()
	mockLegacyBlock *types.Block
	legacyHeaderCID cid.Cid
)

func setupLegacy(t *testing.T) {
	mockLegacyBlock = legacyData.MockBlock
	legacyHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, legacyData.MockHeaderRlp, multihash.KECCAK_256)

	ind, err := file.NewStateDiffIndexer(context.Background(), legacyData.Config, file.TestConfig)
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

	test_helpers.ExpectEqual(t, tx.(*sql.BatchTx).BlockNumber, legacyData.BlockNumber.Uint64())

	connStr := postgres.DefaultConfig.DbConnectionString()

	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func dumpData(t *testing.T) {
	sqlFileBytes, err := os.ReadFile(file.TestConfig.FilePath)
	require.NoError(t, err)

	_, err = sqlxdb.Exec(string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	err := os.Remove(file.TestConfig.FilePath)
	require.NoError(t, err)
	err = sqlxdb.Close()
	require.NoError(t, err)
}

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}

func TestFIleIndexerLegacy(t *testing.T) {
	t.Run("Publish and index header IPLDs in a legacy tx", func(t *testing.T) {
		setupLegacy(t)
		dumpData(t)
		defer tearDown(t)
		pgStr := `SELECT cid, cast(td AS TEXT), cast(reward AS TEXT), block_hash, base_fee
				FROM eth.header_cids
				WHERE block_number = $1`
		// check header was properly indexed
		type res struct {
			CID       string
			TD        string
			Reward    string
			BlockHash string `db:"block_hash"`
			BaseFee   *int64 `db:"base_fee"`
		}
		header := new(res)

		err = sqlxdb.QueryRow(pgStr, legacyData.BlockNumber.Uint64()).Scan(
			&header.CID, &header.TD, &header.Reward, &header.BlockHash, &header.BaseFee)
		require.NoError(t, err)

		test_helpers.ExpectEqual(t, header.CID, legacyHeaderCID.String())
		test_helpers.ExpectEqual(t, header.TD, legacyData.MockBlock.Difficulty().String())
		test_helpers.ExpectEqual(t, header.Reward, "5000000000000011250")
		require.Nil(t, legacyData.MockHeader.BaseFee)
		require.Nil(t, header.BaseFee)
	})
}
