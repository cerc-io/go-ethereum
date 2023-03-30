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
	"sync"
	"sync/atomic"

	"github.com/lib/pq"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

const startingCacheCapacity = 1024 * 24

// BatchTx wraps a sql tx with the state necessary for building the tx concurrently during trie difference iteration
type BatchTx struct {
	BlockNumber      string
	ctx              context.Context
	dbtx             Tx
	stm              string
	quit             chan struct{}
	iplds            chan models.IPLDModel
	ipldCache        models.IPLDBatch
	removedCacheFlag *uint32
	// Tracks expected cache size and ensures cache is caught up before flush
	cacheWg sync.WaitGroup

	submit func(blockTx *BatchTx, err error) error
}

// Submit satisfies indexer.AtomicTx
func (tx *BatchTx) Submit(err error) error {
	return tx.submit(tx, err)
}

func (tx *BatchTx) flush() error {
	tx.cacheWg.Wait()
	_, err := tx.dbtx.Exec(tx.ctx, tx.stm, pq.Array(tx.ipldCache.BlockNumbers), pq.Array(tx.ipldCache.Keys),
		pq.Array(tx.ipldCache.Values))
	if err != nil {
		log.Debug(insertError{"ipld.blocks", err, tx.stm,
			struct {
				blockNumbers []string
				keys         []string
				values       [][]byte
			}{
				tx.ipldCache.BlockNumbers,
				tx.ipldCache.Keys,
				tx.ipldCache.Values,
			}}.Error())
		return insertError{"ipld.blocks", err, tx.stm, "too many arguments; use debug mode for full list"}
	}
	tx.ipldCache = models.IPLDBatch{}
	return nil
}

// run in background goroutine to synchronize concurrent appends to the ipldCache
func (tx *BatchTx) cache() {
	for {
		select {
		case i := <-tx.iplds:
			tx.ipldCache.BlockNumbers = append(tx.ipldCache.BlockNumbers, i.BlockNumber)
			tx.ipldCache.Keys = append(tx.ipldCache.Keys, i.Key)
			tx.ipldCache.Values = append(tx.ipldCache.Values, i.Data)
			tx.cacheWg.Done()
		case <-tx.quit:
			tx.ipldCache = models.IPLDBatch{}
			return
		}
	}
}

func (tx *BatchTx) cacheDirect(key string, value []byte) {
	tx.cacheWg.Add(1)
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         key,
		Data:        value,
	}
}

func (tx *BatchTx) cacheIPLD(i ipld.IPLD) {
	tx.cacheWg.Add(1)
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         i.Cid().String(),
		Data:        i.RawData(),
	}
}

func (tx *BatchTx) cacheRemoved(key string, value []byte) {
	if atomic.LoadUint32(tx.removedCacheFlag) == 0 {
		atomic.StoreUint32(tx.removedCacheFlag, 1)
		tx.cacheWg.Add(1)
		tx.iplds <- models.IPLDModel{
			BlockNumber: tx.BlockNumber,
			Key:         key,
			Data:        value,
		}
	}
}

// rollback sql transaction and log any error
func rollback(ctx context.Context, tx Tx) {
	if err := tx.Rollback(ctx); err != nil {
		log.Error(err.Error())
	}
}
