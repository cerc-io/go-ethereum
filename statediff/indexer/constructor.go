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

package indexer

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/dump"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

// NewStateDiffIndexer creates and returns an implementation of the StateDiffIndexer interface
func NewStateDiffIndexer(ctx context.Context, chainConfig *params.ChainConfig, nodeInfo node.Info, config interfaces.Config) (interfaces.StateDiffIndexer, error) {
	switch config.Type() {
	case shared.FILE:
		fc, ok := config.(file.Config)
		if !ok {
			return nil, fmt.Errorf("file config is not the correct type: got %T, expected %T", config, file.Config{})
		}
		return file.NewStateDiffIndexer(ctx, chainConfig, fc)
	case shared.POSTGRES:
		pgc, ok := config.(postgres.Config)
		if !ok {
			return nil, fmt.Errorf("postgres config is not the correct type: got %T, expected %T", config, postgres.Config{})
		}
		var err error
		var driver sql.Driver
		switch pgc.Driver {
		case postgres.PGX:
			driver, err = postgres.NewPGXDriver(ctx, pgc, nodeInfo)
			if err != nil {
				return nil, err
			}
		case postgres.SQLX:
			driver, err = postgres.NewSQLXDriver(ctx, pgc, nodeInfo)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unrecongized Postgres driver type: %s", pgc.Driver)
		}
		return sql.NewStateDiffIndexer(ctx, chainConfig, postgres.NewPostgresDB(driver))
	case shared.DUMP:
		dumpc, ok := config.(dump.Config)
		if !ok {
			return nil, fmt.Errorf("dump config is not the correct type: got %T, expected %T", config, dump.Config{})
		}
		return dump.NewStateDiffIndexer(chainConfig, dumpc), nil
	default:
		return nil, fmt.Errorf("unrecognized database type: %s", config.Type())
	}
}
