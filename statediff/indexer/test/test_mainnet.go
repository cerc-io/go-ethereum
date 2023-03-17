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

package test

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T, ind interfaces.StateDiffIndexer, testBlock *types.Block, testReceipts types.Receipts) {
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		testBlock,
		testReceipts,
		testBlock.Difficulty())
	require.NoError(t, err)

	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, testBlock.Hash().String())
		require.NoError(t, err)
	}

	if batchTx, ok := tx.(*sql.BatchTx); ok {
		require.Equal(t, testBlock.Number().String(), batchTx.BlockNumber)
	} else if batchTx, ok := tx.(*file.BatchTx); ok {
		require.Equal(t, testBlock.Number().String(), batchTx.BlockNumber)
	}
}
