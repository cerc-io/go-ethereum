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

package dump

import (
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"

	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

// BatchTx wraps a void with the state necessary for building the tx concurrently during trie difference iteration
type BatchTx struct {
	BlockNumber string
	dump        io.Writer
	quit        chan struct{}
	iplds       chan models.IPLDModel
	ipldCache   models.IPLDBatch

	submit func(blockTx *BatchTx, err error) error
}

// Submit satisfies indexer.AtomicTx
func (tx *BatchTx) Submit(err error) error {
	return tx.submit(tx, err)
}

func (tx *BatchTx) flush() error {
	if _, err := fmt.Fprintf(tx.dump, "%+v\r\n", tx.ipldCache); err != nil {
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

func (tx *BatchTx) cacheIPLD(i ipld.IPLD) {
	tx.iplds <- models.IPLDModel{
		BlockNumber: tx.BlockNumber,
		Key:         i.Cid().String(),
		Data:        i.RawData(),
	}
}
