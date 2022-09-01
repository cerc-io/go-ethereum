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

package postgres

import (
	"context"
	coresql "database/sql"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
)

// SQLXDriver driver, implements sql.Driver
type SQLXDriver struct {
	ctx      context.Context
	db       *sqlx.DB
	nodeInfo node.Info
	nodeID   string
}

// NewSQLXDriver returns a new sqlx driver for Postgres
// it initializes the connection pool and creates the node info table
func NewSQLXDriver(ctx context.Context, config Config, node node.Info) (*SQLXDriver, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", config.DbConnectionString())
	if err != nil {
		return &SQLXDriver{}, ErrDBConnectionFailed(err)
	}
	if config.MaxConns > 0 {
		db.SetMaxOpenConns(config.MaxConns)
	}
	if config.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(config.MaxConnLifetime)
	}
	db.SetMaxIdleConns(config.MaxIdle)
	driver := &SQLXDriver{ctx: ctx, db: db, nodeInfo: node}
	if err := driver.createNode(); err != nil {
		return &SQLXDriver{}, ErrUnableToSetNode(err)
	}
	return driver, nil
}

func (driver *SQLXDriver) createNode() error {
	_, err := driver.db.Exec(
		createNodeStm,
		driver.nodeInfo.GenesisBlock, driver.nodeInfo.NetworkID,
		driver.nodeInfo.ID, driver.nodeInfo.ClientName,
		driver.nodeInfo.ChainID)
	if err != nil {
		return ErrUnableToSetNode(err)
	}
	driver.nodeID = driver.nodeInfo.ID
	return nil
}

// QueryRow satisfies sql.Database
func (driver *SQLXDriver) QueryRow(_ context.Context, sql string, args ...interface{}) sql.ScannableRow {
	return driver.db.QueryRowx(sql, args...)
}

// Exec satisfies sql.Database
func (driver *SQLXDriver) Exec(_ context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return driver.db.Exec(sql, args...)
}

// Select satisfies sql.Database
func (driver *SQLXDriver) Select(_ context.Context, dest interface{}, query string, args ...interface{}) error {
	return driver.db.Select(dest, query, args...)
}

// Get satisfies sql.Database
func (driver *SQLXDriver) Get(_ context.Context, dest interface{}, query string, args ...interface{}) error {
	return driver.db.Get(dest, query, args...)
}

// Begin satisfies sql.Database
func (driver *SQLXDriver) Begin(_ context.Context) (sql.Tx, error) {
	tx, err := driver.db.Beginx()
	if err != nil {
		return nil, err
	}
	return sqlxTxWrapper{tx: tx}, nil
}

func (driver *SQLXDriver) Stats() sql.Stats {
	stats := driver.db.Stats()
	return sqlxStatsWrapper{stats: stats}
}

// NodeID satisfies sql.Database
func (driver *SQLXDriver) NodeID() string {
	return driver.nodeID
}

// Close satisfies sql.Database/io.Closer
func (driver *SQLXDriver) Close() error {
	return driver.db.Close()
}

// Context satisfies sql.Database
func (driver *SQLXDriver) Context() context.Context {
	return driver.ctx
}

type sqlxStatsWrapper struct {
	stats coresql.DBStats
}

// MaxOpen satisfies sql.Stats
func (s sqlxStatsWrapper) MaxOpen() int64 {
	return int64(s.stats.MaxOpenConnections)
}

// Open satisfies sql.Stats
func (s sqlxStatsWrapper) Open() int64 {
	return int64(s.stats.OpenConnections)
}

// InUse satisfies sql.Stats
func (s sqlxStatsWrapper) InUse() int64 {
	return int64(s.stats.InUse)
}

// Idle satisfies sql.Stats
func (s sqlxStatsWrapper) Idle() int64 {
	return int64(s.stats.Idle)
}

// WaitCount satisfies sql.Stats
func (s sqlxStatsWrapper) WaitCount() int64 {
	return s.stats.WaitCount
}

// WaitDuration satisfies sql.Stats
func (s sqlxStatsWrapper) WaitDuration() time.Duration {
	return s.stats.WaitDuration
}

// MaxIdleClosed satisfies sql.Stats
func (s sqlxStatsWrapper) MaxIdleClosed() int64 {
	return s.stats.MaxIdleClosed
}

// MaxLifetimeClosed satisfies sql.Stats
func (s sqlxStatsWrapper) MaxLifetimeClosed() int64 {
	return s.stats.MaxLifetimeClosed
}

type sqlxTxWrapper struct {
	tx *sqlx.Tx
}

// QueryRow satisfies sql.Tx
func (t sqlxTxWrapper) QueryRow(ctx context.Context, sql string, args ...interface{}) sql.ScannableRow {
	return t.tx.QueryRowx(sql, args...)
}

// Exec satisfies sql.Tx
func (t sqlxTxWrapper) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return t.tx.Exec(sql, args...)
}

// Commit satisfies sql.Tx
func (t sqlxTxWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

// Rollback satisfies sql.Tx
func (t sqlxTxWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback()
}
