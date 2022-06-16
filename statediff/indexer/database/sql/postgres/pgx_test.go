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

package postgres_test

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
)

var (
	pgConfig, _ = postgres.MakeConfig(postgres.DefaultConfig)
	ctx         = context.Background()
)

func expectContainsSubstring(t *testing.T, full string, sub string) {
	if !strings.Contains(full, sub) {
		t.Fatalf("Expected \"%v\" to contain substring \"%v\"\n", full, sub)
	}
}

func TestPostgresPGX(t *testing.T) {
	t.Run("connects to the sql", func(t *testing.T) {
		dbPool, err := pgxpool.ConnectConfig(context.Background(), pgConfig)
		if err != nil {
			t.Fatalf("failed to connect to db with connection string: %s err: %v", pgConfig.ConnString(), err)
		}
		if dbPool == nil {
			t.Fatal("DB pool is nil")
		}
		dbPool.Close()
	})

	t.Run("serializes big.Int to db", func(t *testing.T) {
		// postgres driver doesn't support go big.Int type
		// various casts in golang uint64, int64, overflow for
		// transaction value (in wei) even though
		// postgres numeric can handle an arbitrary
		// sized int, so use string representation of big.Int
		// and cast on insert

		dbPool, err := pgxpool.ConnectConfig(context.Background(), pgConfig)
		if err != nil {
			t.Fatalf("failed to connect to db with connection string: %s err: %v", pgConfig.ConnString(), err)
		}
		defer dbPool.Close()

		bi := new(big.Int)
		bi.SetString("34940183920000000000", 10)
		require.Equal(t, "34940183920000000000", bi.String())

		defer dbPool.Exec(ctx, `DROP TABLE IF EXISTS example`)
		_, err = dbPool.Exec(ctx, "CREATE TABLE example ( id INTEGER, data NUMERIC )")
		if err != nil {
			t.Fatal(err)
		}

		sqlStatement := `
			INSERT INTO example (id, data)
			VALUES (1, cast($1 AS NUMERIC))`
		_, err = dbPool.Exec(ctx, sqlStatement, bi.String())
		if err != nil {
			t.Fatal(err)
		}

		var data string
		err = dbPool.QueryRow(ctx, `SELECT cast(data AS TEXT) FROM example WHERE id = 1`).Scan(&data)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, data, bi.String())
		actual := new(big.Int)
		actual.SetString(data, 10)
		require.Equal(t, bi, actual)
	})

	t.Run("throws error when can't connect to the database", func(t *testing.T) {
		goodInfo := node.Info{GenesisBlock: "GENESIS", NetworkID: "1", ID: "x123", ClientName: "geth"}
		_, err := postgres.NewPGXDriver(ctx, postgres.Config{}, goodInfo)
		if err == nil {
			t.Fatal("Expected an error")
		}

		expectContainsSubstring(t, err.Error(), postgres.DbConnectionFailedMsg)
	})

	t.Run("throws error when can't create node", func(t *testing.T) {
		badHash := fmt.Sprintf("x %s", strings.Repeat("1", 100))
		badInfo := node.Info{GenesisBlock: badHash, NetworkID: "1", ID: "x123", ClientName: "geth"}

		_, err := postgres.NewPGXDriver(ctx, postgres.DefaultConfig, badInfo)
		if err == nil {
			t.Fatal("Expected an error")
		}

		expectContainsSubstring(t, err.Error(), postgres.SettingNodeFailedMsg)
	})
}
