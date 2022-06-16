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

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
)

func setupLegacyPGX(t *testing.T) {
	mockLegacyBlock = legacyData.MockBlock
	legacyHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, legacyData.MockHeaderRlp, multihash.KECCAK_256)

	db, err = postgres.SetupPGXDB()
	require.NoError(t, err)

	ind, err = sql.NewStateDiffIndexer(context.Background(), legacyData.Config, db)
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
	}()
	for _, node := range legacyData.StateDiffs {
		err = ind.PushStateNode(tx, node, legacyData.MockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, legacyData.BlockNumber.String(), tx.(*sql.BatchTx).BlockNumber)
}

func TestLegacyPGXIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupLegacyPGX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)
		pgStr := `SELECT cid, cast(td AS TEXT), cast(reward AS TEXT), block_hash, coinbase
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

		err = db.QueryRow(context.Background(), pgStr, legacyData.BlockNumber.Uint64()).Scan(
			&header.CID, &header.TD, &header.Reward, &header.BlockHash, &header.Coinbase)
		require.NoError(t, err)

		require.Equal(t, legacyHeaderCID.String(), header.CID)
		require.Equal(t, legacyData.MockBlock.Difficulty().String(), header.TD)
		require.Equal(t, "5000000000000011250", header.Reward)
		require.Equal(t, legacyData.MockHeader.Coinbase.String(), header.Coinbase)
		require.Nil(t, legacyData.MockHeader.BaseFee)
	})
}
