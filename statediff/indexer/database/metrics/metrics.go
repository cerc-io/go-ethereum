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

package metrics

import (
	"github.com/ethereum/go-ethereum/log"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	namespace = "statediff"
)

var (
	IndexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
	DBMetrics      = RegisterDBMetrics(metrics.DefaultRegistry)
)

// Build a fully qualified metric name
func metricName(subsystem, name string) string {
	if name == "" {
		return ""
	}
	parts := []string{namespace, name}
	if subsystem != "" {
		parts = []string{namespace, subsystem, name}
	}
	// Prometheus uses _ but geth metrics uses / and replaces
	return strings.Join(parts, "/")
}

type IndexerMetricsHandles struct {
	// The total number of processed BlocksCounter
	BlocksCounter metrics.Counter
	// The total number of processed transactions
	TransactionsCounter metrics.Counter
	// The total number of processed receipts
	ReceiptsCounter metrics.Counter
	// The total number of processed logs
	LogsCounter metrics.Counter
	// The total number of access list entries processed
	AccessListEntriesCounter metrics.Counter
	// Time spent waiting for free postgres tx
	FreePostgresTimer metrics.Timer
	// Postgres transaction commit duration
	PostgresCommitTimer metrics.Timer
	// Header processing time
	HeaderProcessingTimer metrics.Timer
	// Uncle processing time
	UncleProcessingTimer metrics.Timer
	// Tx and receipt processing time
	TxAndRecProcessingTimer metrics.Timer
	// State, storage, and code combined processing time
	StateStoreCodeProcessingTimer metrics.Timer
}

func RegisterIndexerMetrics(reg metrics.Registry) IndexerMetricsHandles {
	ctx := IndexerMetricsHandles{
		BlocksCounter:                 metrics.NewCounter(),
		TransactionsCounter:           metrics.NewCounter(),
		ReceiptsCounter:               metrics.NewCounter(),
		LogsCounter:                   metrics.NewCounter(),
		AccessListEntriesCounter:      metrics.NewCounter(),
		FreePostgresTimer:             metrics.NewTimer(),
		PostgresCommitTimer:           metrics.NewTimer(),
		HeaderProcessingTimer:         metrics.NewTimer(),
		UncleProcessingTimer:          metrics.NewTimer(),
		TxAndRecProcessingTimer:       metrics.NewTimer(),
		StateStoreCodeProcessingTimer: metrics.NewTimer(),
	}
	subsys := "indexer"
	reg.Register(metricName(subsys, "blocks"), ctx.BlocksCounter)
	reg.Register(metricName(subsys, "transactions"), ctx.TransactionsCounter)
	reg.Register(metricName(subsys, "receipts"), ctx.ReceiptsCounter)
	reg.Register(metricName(subsys, "logs"), ctx.LogsCounter)
	reg.Register(metricName(subsys, "access_list_entries"), ctx.AccessListEntriesCounter)
	reg.Register(metricName(subsys, "t_free_postgres"), ctx.FreePostgresTimer)
	reg.Register(metricName(subsys, "t_postgres_commit"), ctx.PostgresCommitTimer)
	reg.Register(metricName(subsys, "t_header_processing"), ctx.HeaderProcessingTimer)
	reg.Register(metricName(subsys, "t_uncle_processing"), ctx.UncleProcessingTimer)
	reg.Register(metricName(subsys, "t_tx_receipt_processing"), ctx.TxAndRecProcessingTimer)
	reg.Register(metricName(subsys, "t_state_store_code_processing"), ctx.StateStoreCodeProcessingTimer)

	log.Debug("Registering statediff indexer metrics.")
	return ctx
}

type dbMetricsHandles struct {
	// Maximum number of open connections to the sql
	maxOpen metrics.Gauge
	// The number of established connections both in use and idle
	open metrics.Gauge
	// The number of connections currently in use
	inUse metrics.Gauge
	// The number of idle connections
	idle metrics.Gauge
	// The total number of connections waited for
	waitedFor metrics.Counter
	// The total time blocked waiting for a new connection
	blockedMilliseconds metrics.Counter
	// The total number of connections closed due to SetMaxIdleConns
	closedMaxIdle metrics.Counter
	// The total number of connections closed due to SetConnMaxLifetime
	closedMaxLifetime metrics.Counter
}

func RegisterDBMetrics(reg metrics.Registry) dbMetricsHandles {
	ctx := dbMetricsHandles{
		maxOpen:             metrics.NewGauge(),
		open:                metrics.NewGauge(),
		inUse:               metrics.NewGauge(),
		idle:                metrics.NewGauge(),
		waitedFor:           metrics.NewCounter(),
		blockedMilliseconds: metrics.NewCounter(),
		closedMaxIdle:       metrics.NewCounter(),
		closedMaxLifetime:   metrics.NewCounter(),
	}
	subsys := "connections"
	reg.Register(metricName(subsys, "max_open"), ctx.maxOpen)
	reg.Register(metricName(subsys, "open"), ctx.open)
	reg.Register(metricName(subsys, "in_use"), ctx.inUse)
	reg.Register(metricName(subsys, "idle"), ctx.idle)
	reg.Register(metricName(subsys, "waited_for"), ctx.waitedFor)
	reg.Register(metricName(subsys, "blocked_milliseconds"), ctx.blockedMilliseconds)
	reg.Register(metricName(subsys, "closed_max_idle"), ctx.closedMaxIdle)
	reg.Register(metricName(subsys, "closed_max_lifetime"), ctx.closedMaxLifetime)

	log.Debug("Registering statediff DB metrics.")
	return ctx
}

// DbStats interface to accommodate different concrete sql stats types
type DbStats interface {
	MaxOpen() int64
	Open() int64
	InUse() int64
	Idle() int64
	WaitCount() int64
	WaitDuration() time.Duration
	MaxIdleClosed() int64
	MaxLifetimeClosed() int64
}

func (met *dbMetricsHandles) Update(stats DbStats) {
	met.maxOpen.Update(stats.MaxOpen())
	met.open.Update(stats.Open())
	met.inUse.Update(stats.InUse())
	met.idle.Update(stats.Idle())
	met.waitedFor.Inc(stats.WaitCount())
	met.blockedMilliseconds.Inc(stats.WaitDuration().Milliseconds())
	met.closedMaxIdle.Inc(stats.MaxIdleClosed())
	met.closedMaxLifetime.Inc(stats.MaxLifetimeClosed())
}
