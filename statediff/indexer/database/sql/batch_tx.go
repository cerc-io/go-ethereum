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
	"sync/atomic"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	node "github.com/ipfs/go-ipld-format"
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

	submit func(blockTx *BatchTx, err error) error
}

// Submit satisfies indexer.AtomicTx
func (tx *BatchTx) Submit(err error) error {
	return tx.submit(tx, err)
}

func (tx *BatchTx) flush() error {
	_, err := tx.dbtx.Exec(tx.ctx, tx.stm, pq.Array(tx.ipldCache.BlockNumbers), pq.Array(tx.ipldCache.Keys),
		pq.Array(tx.ipldCache.Values))
	if err != nil {
		return err
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
		case <-tx.quit:
			tx.ipldCache = models.IPLDBatch{}
			return
		}
	}
}

func (tx *BatchTx) cacheDirect(key string, value []byte) {
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         key,
		Data:        value,
	}
}

func (tx *BatchTx) cacheIPLD(i node.Node) {
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(i.Cid().Hash()).String(),
		Data:        i.RawData(),
	}
}

func (tx *BatchTx) cacheCid(c cid.Cid, data []byte) {
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String(),
		Data:        data,
	}
}

func (tx *BatchTx) cacheRaw(codec, mh uint64, raw []byte) (string, string, error) {
	c, err := ipld.RawdataToCid(codec, raw, mh)
	if err != nil {
		return "", "", err
	}
	prefixedKey := blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String()
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         prefixedKey,
		Data:        raw,
	}
	return c.String(), prefixedKey, err
}

func (tx *BatchTx) cacheRemoved(key string, value []byte) {
	if atomic.LoadUint32(tx.removedCacheFlag) == 0 {
		atomic.StoreUint32(tx.removedCacheFlag, 1)
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
