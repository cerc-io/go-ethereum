// VulcanizeDB
// Copyright © 2021 Vulcanize

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

package postgres

import (
	"context"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
)

// SetupSQLXDB is used to setup a sqlx db for tests
func SetupSQLXDB() (sql.Database, error) {
	conf := DefaultConfig
	conf.MaxIdle = 0
	driver, err := NewSQLXDriver(context.Background(), conf, node.Info{})
	if err != nil {
		return nil, err
	}
	return NewPostgresDB(driver), nil
}

// SetupPGXDB is used to setup a pgx db for tests
func SetupPGXDB() (sql.Database, error) {
	driver, err := NewPGXDriver(context.Background(), DefaultConfig, node.Info{})
	if err != nil {
		return nil, err
	}
	return NewPostgresDB(driver), nil
}
