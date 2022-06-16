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

package file

import (
	"strings"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	namespace = "statediff"
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

type indexerMetricsHandles struct {
	// The total number of processed blocks
	blocks metrics.Counter
	// The total number of processed transactions
	transactions metrics.Counter
	// The total number of processed receipts
	receipts metrics.Counter
	// The total number of processed logs
	logs metrics.Counter
	// The total number of access list entries processed
	accessListEntries metrics.Counter
	// Time spent waiting for free postgres tx
	tFreePostgres metrics.Timer
	// Postgres transaction commit duration
	tPostgresCommit metrics.Timer
	// Header processing time
	tHeaderProcessing metrics.Timer
	// Uncle processing time
	tUncleProcessing metrics.Timer
	// Tx and receipt processing time
	tTxAndRecProcessing metrics.Timer
	// State, storage, and code combined processing time
	tStateStoreCodeProcessing metrics.Timer
}

func RegisterIndexerMetrics(reg metrics.Registry) indexerMetricsHandles {
	ctx := indexerMetricsHandles{
		blocks:                    metrics.NewCounter(),
		transactions:              metrics.NewCounter(),
		receipts:                  metrics.NewCounter(),
		logs:                      metrics.NewCounter(),
		accessListEntries:         metrics.NewCounter(),
		tFreePostgres:             metrics.NewTimer(),
		tPostgresCommit:           metrics.NewTimer(),
		tHeaderProcessing:         metrics.NewTimer(),
		tUncleProcessing:          metrics.NewTimer(),
		tTxAndRecProcessing:       metrics.NewTimer(),
		tStateStoreCodeProcessing: metrics.NewTimer(),
	}
	subsys := "indexer"
	reg.Register(metricName(subsys, "blocks"), ctx.blocks)
	reg.Register(metricName(subsys, "transactions"), ctx.transactions)
	reg.Register(metricName(subsys, "receipts"), ctx.receipts)
	reg.Register(metricName(subsys, "logs"), ctx.logs)
	reg.Register(metricName(subsys, "access_list_entries"), ctx.accessListEntries)
	reg.Register(metricName(subsys, "t_free_postgres"), ctx.tFreePostgres)
	reg.Register(metricName(subsys, "t_postgres_commit"), ctx.tPostgresCommit)
	reg.Register(metricName(subsys, "t_header_processing"), ctx.tHeaderProcessing)
	reg.Register(metricName(subsys, "t_uncle_processing"), ctx.tUncleProcessing)
	reg.Register(metricName(subsys, "t_tx_receipt_processing"), ctx.tTxAndRecProcessing)
	reg.Register(metricName(subsys, "t_state_store_code_processing"), ctx.tStateStoreCodeProcessing)
	return ctx
}
