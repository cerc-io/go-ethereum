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
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
)

func TestPostgresSQLX(t *testing.T) {
	var sqlxdb *sqlx.DB

	t.Run("connects to the database", func(t *testing.T) {
		var err error
		connStr := postgres.DefaultConfig.DbConnectionString()

		sqlxdb, err = sqlx.Connect("postgres", connStr)
		if err != nil {
			t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
		}
		if sqlxdb == nil {
			t.Fatal("DB is nil")
		}
		err = sqlxdb.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("serializes big.Int to db", func(t *testing.T) {
		// postgres driver doesn't support go big.Int type
		// various casts in golang uint64, int64, overflow for
		// transaction value (in wei) even though
		// postgres numeric can handle an arbitrary
		// sized int, so use string representation of big.Int
		// and cast on insert

		connStr := postgres.DefaultConfig.DbConnectionString()
		db, err := sqlx.Connect("postgres", connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		bi := new(big.Int)
		bi.SetString("34940183920000000000", 10)
		require.Equal(t, "34940183920000000000", bi.String())

		defer db.Exec(`DROP TABLE IF EXISTS example`)
		_, err = db.Exec("CREATE TABLE example ( id INTEGER, data NUMERIC )")
		if err != nil {
			t.Fatal(err)
		}

		sqlStatement := `
			INSERT INTO example (id, data)
			VALUES (1, cast($1 AS NUMERIC))`
		_, err = db.Exec(sqlStatement, bi.String())
		if err != nil {
			t.Fatal(err)
		}

		var data string
		err = db.QueryRow(`SELECT data FROM example WHERE id = 1`).Scan(&data)
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
		_, err := postgres.NewSQLXDriver(ctx, postgres.Config{}, goodInfo)
		if err == nil {
			t.Fatal("Expected an error")
		}

		expectContainsSubstring(t, err.Error(), postgres.DbConnectionFailedMsg)
	})

	t.Run("throws error when can't create node", func(t *testing.T) {
		badHash := fmt.Sprintf("x %s", strings.Repeat("1", 100))
		badInfo := node.Info{GenesisBlock: badHash, NetworkID: "1", ID: "x123", ClientName: "geth"}

		_, err := postgres.NewSQLXDriver(ctx, postgres.DefaultConfig, badInfo)
		if err == nil {
			t.Fatal("Expected an error")
		}

		expectContainsSubstring(t, err.Error(), postgres.SettingNodeFailedMsg)
	})
}
