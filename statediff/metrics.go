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

package statediff

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

type statediffMetricsHandles struct {
	// Height of latest synced by core.BlockChain
	// FIXME
	lastSyncHeight metrics.Gauge
	// Height of the latest block received from chainEvent channel
	lastEventHeight metrics.Gauge
	// Height of latest state diff
	lastStatediffHeight metrics.Gauge
	// Current length of chainEvent channels
	serviceLoopChannelLen metrics.Gauge
	writeLoopChannelLen   metrics.Gauge
}

func RegisterStatediffMetrics(reg metrics.Registry) statediffMetricsHandles {
	ctx := statediffMetricsHandles{
		lastSyncHeight:        metrics.NewGauge(),
		lastEventHeight:       metrics.NewGauge(),
		lastStatediffHeight:   metrics.NewGauge(),
		serviceLoopChannelLen: metrics.NewGauge(),
		writeLoopChannelLen:   metrics.NewGauge(),
	}
	subsys := "service"
	reg.Register(metricName(subsys, "last_sync_height"), ctx.lastSyncHeight)
	reg.Register(metricName(subsys, "last_event_height"), ctx.lastEventHeight)
	reg.Register(metricName(subsys, "last_statediff_height"), ctx.lastStatediffHeight)
	reg.Register(metricName(subsys, "service_loop_channel_len"), ctx.serviceLoopChannelLen)
	reg.Register(metricName(subsys, "write_loop_channel_len"), ctx.writeLoopChannelLen)
	return ctx
}
