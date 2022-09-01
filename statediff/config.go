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
	"context"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
)

// Config contains instantiation parameters for the state diffing service
type Config struct {
	// The configuration used for the stateDiff Indexer
	IndexerConfig interfaces.Config
	// The filepath to write knownGaps insert statements if we can't connect to the DB.
	KnownGapsFilePath string
	// A unique ID used for this service
	ID string
	// Name for the client this service is running
	ClientName string
	// Whether to enable writing state diffs directly to track blockchain head
	EnableWriteLoop bool
	// Size of the worker pool
	NumWorkers uint
	// Should the statediff service wait until geth has synced to the head of the blockchain?
	WaitForSync bool
	// Context
	Context context.Context
}

// Params contains config parameters for the state diff builder
type Params struct {
	IntermediateStateNodes    bool
	IntermediateStorageNodes  bool
	IncludeBlock              bool
	IncludeReceipts           bool
	IncludeTD                 bool
	IncludeCode               bool
	WatchedAddresses          []common.Address
	watchedAddressesLeafPaths [][]byte
}

// ComputeWatchedAddressesLeafPaths populates a slice with paths (hex_encoding(Keccak256)) of each of the WatchedAddresses
func (p *Params) ComputeWatchedAddressesLeafPaths() {
	p.watchedAddressesLeafPaths = make([][]byte, len(p.WatchedAddresses))
	for i, address := range p.WatchedAddresses {
		p.watchedAddressesLeafPaths[i] = keybytesToHex(crypto.Keccak256(address.Bytes()))
	}
}

// ParamsWithMutex allows to lock the parameters while they are being updated | read from
type ParamsWithMutex struct {
	Params
	sync.RWMutex
}

// Args bundles the arguments for the state diff builder
type Args struct {
	OldStateRoot, NewStateRoot, BlockHash common.Hash
	BlockNumber                           *big.Int
}

// https://github.com/ethereum/go-ethereum/blob/master/trie/encoding.go#L97
func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}
