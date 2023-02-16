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

package ipld

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/core/types"
)

// EthTx (eth-tx codec 0x93) represents an ethereum transaction
type EthTx struct {
	cid     cid.Cid
	rawdata []byte
}

// Static (compile time) check that EthTx satisfies the node.Node interface.
var _ IPLD = (*EthTx)(nil)

// NewEthTx converts a *types.Transaction to an EthTx IPLD node
func NewEthTx(tx *types.Transaction) (*EthTx, error) {
	txRaw, err := tx.MarshalBinary()
	if err != nil {
		return nil, err
	}
	c, err := RawdataToCid(MEthTx, txRaw, mh.KECCAK_256)
	if err != nil {
		return nil, err
	}
	return &EthTx{
		cid:     c,
		rawdata: txRaw,
	}, nil
}

// RawData returns the binary of the RLP encode of the transaction.
func (t *EthTx) RawData() []byte {
	return t.rawdata
}

// Cid returns the cid of the transaction.
func (t *EthTx) Cid() cid.Cid {
	return t.cid
}
