// VulcanizeDB
// Copyright © 2022 Vulcanize

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
	"math/big"

	node "github.com/ipfs/go-ipld-format"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/types"
)

// Writer interface required by the file indexer
type FileWriter interface {
	// Methods used to control the writer
	Loop()
	Close() error
	Flush()

	// Methods to write out data to tables
	upsertNode(node nodeinfo.Info)
	upsertHeaderCID(header models.HeaderModel)
	upsertUncleCID(uncle models.UncleModel)
	upsertTransactionCID(transaction models.TxModel)
	upsertAccessListElement(accessListElement models.AccessListElementModel)
	upsertReceiptCID(rct *models.ReceiptModel)
	upsertLogCID(logs []*models.LogsModel)
	upsertStateCID(stateNode models.StateNodeModel)
	upsertStateAccount(stateAccount models.StateAccountModel)
	upsertStorageCID(storageCID models.StorageNodeModel)
	upsertIPLD(ipld models.IPLDModel)

	// Methods to upsert IPLD in different ways
	upsertIPLDDirect(blockNumber, key string, value []byte)
	upsertIPLDNode(blockNumber string, i node.Node)
	upsertIPLDRaw(blockNumber string, codec, mh uint64, raw []byte) (string, string, error)

	// Methods to read and write watched addresses
	loadWatchedAddresses() ([]common.Address, error)
	insertWatchedAddresses(args []types.WatchAddressArg, currentBlockNumber *big.Int) error
	removeWatchedAddresses(args []types.WatchAddressArg) error
	setWatchedAddresses(args []types.WatchAddressArg, currentBlockNumber *big.Int) error
}
