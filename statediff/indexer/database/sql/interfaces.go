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

package sql

import (
	"context"
	"io"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
)

// Database interfaces required by the sql indexer
type Database interface {
	Driver
	Statements
}

// Driver interface has all the methods required by a driver implementation to support the sql indexer
type Driver interface {
	UseCopyFrom() bool
	QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow
	Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Begin(ctx context.Context) (Tx, error)
	Stats() metrics.DbStats
	NodeID() string
	Context() context.Context
	io.Closer
}

// Statements interface to accommodate different SQL query syntax
type Statements interface {
	InsertHeaderStm() string
	InsertUncleStm() string
	InsertTxStm() string
	InsertRctStm() string
	InsertLogStm() string
	InsertStateStm() string
	InsertStorageStm() string
	InsertIPLDStm() string
	InsertIPLDsStm() string

	// Table/column descriptions for use with CopyFrom and similar commands.
	LogTableName() []string
	LogColumnNames() []string
	RctTableName() []string
	RctColumnNames() []string
	StateTableName() []string
	StateColumnNames() []string
	StorageTableName() []string
	StorageColumnNames() []string
	TxTableName() []string
	TxColumnNames() []string
}

// Tx interface to accommodate different concrete SQL transaction types
type Tx interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow
	Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
	CopyFrom(ctx context.Context, tableName []string, columnNames []string, rows [][]interface{}) (int64, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ScannableRow interface to accommodate different concrete row types
type ScannableRow interface {
	Scan(dest ...interface{}) error
}

// Result interface to accommodate different concrete result types
type Result interface {
	RowsAffected() (int64, error)
}
