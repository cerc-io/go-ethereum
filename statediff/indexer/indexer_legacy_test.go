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

package indexer_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var (
	legacyData      = mocks.NewLegacyData()
	mockLegacyBlock *types.Block
	legacyHeaderCID cid.Cid
)

func setupLegacy(t *testing.T) {
	mockLegacyBlock = legacyData.MockBlock
	legacyHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, legacyData.MockHeaderRlp, multihash.KECCAK_256)

	db, err = shared.SetupDB()
	require.NoError(t, err)

	ind, err = indexer.NewStateDiffIndexer(legacyData.Config, db)
	require.NoError(t, err)
	var tx *indexer.BlockTx
	tx, err = ind.PushBlock(
		mockLegacyBlock,
		legacyData.MockReceipts,
		legacyData.MockBlock.Difficulty())
	require.NoError(t, err)

	defer tx.Close(err)
	for _, node := range legacyData.StateDiffs {
		err = ind.PushStateNode(tx, node)
		require.NoError(t, err)
	}

	shared.ExpectEqual(t, tx.BlockNumber, legacyData.BlockNumber.Uint64())
}

func TestPublishAndIndexerLegacy(t *testing.T) {
	t.Run("Publish and index header IPLDs in a legacy tx", func(t *testing.T) {
		setupLegacy(t)
		defer tearDown(t)
		pgStr := `SELECT cid, td, reward, id, base_fee
				FROM eth.header_cids
				WHERE block_number = $1`
		// check header was properly indexed
		type res struct {
			CID     string
			TD      string
			Reward  string
			ID      int
			BaseFee *int64 `db:"base_fee"`
		}
		header := new(res)
		err = db.QueryRowx(pgStr, legacyData.BlockNumber.Uint64()).StructScan(header)
		require.NoError(t, err)

		shared.ExpectEqual(t, header.CID, legacyHeaderCID.String())
		shared.ExpectEqual(t, header.TD, legacyData.MockBlock.Difficulty().String())
		shared.ExpectEqual(t, header.Reward, "5000000000000011250")
		require.Nil(t, legacyData.MockHeader.BaseFee)
		require.Nil(t, header.BaseFee)
	})
}
