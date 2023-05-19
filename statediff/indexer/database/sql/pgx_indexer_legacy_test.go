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

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/test"
)

func setupLegacyPGXIndexer(t *testing.T) {
	db, err = postgres.SetupPGXDB(postgres.TestConfig)
	if err != nil {
		t.Fatal(err)
	}
	ind, err = sql.NewStateDiffIndexer(context.Background(), test.LegacyConfig, db)
	require.NoError(t, err)
}

func setupLegacyPGX(t *testing.T) {
	setupLegacyPGXIndexer(t)
	test.SetupLegacyTestData(t, ind)
}

func TestLegacyPGXIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs", func(t *testing.T) {
		setupLegacyPGX(t)
		defer tearDown(t)
		defer checkTxClosure(t, 1, 0, 1)

		test.TestLegacyIndexer(t, db)
	})
}
