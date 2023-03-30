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

type EthReceipt struct {
	rawdata []byte
	cid     cid.Cid
}

// Static (compile time) check that EthReceipt satisfies the node.Node interface.
var _ IPLD = (*EthReceipt)(nil)

// NewReceipt converts a types.ReceiptForStorage to an EthReceipt IPLD node
func NewReceipt(receipt *types.Receipt) (*EthReceipt, error) {
	rctRaw, err := receipt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	c, err := RawdataToCid(MEthTxReceipt, rctRaw, mh.KECCAK_256)
	if err != nil {
		return nil, err
	}
	return &EthReceipt{
		cid:     c,
		rawdata: rctRaw,
	}, nil
}

// RawData returns the binary of the RLP encode of the receipt.
func (r *EthReceipt) RawData() []byte {
	return r.rawdata
}

// Cid returns the cid of the receipt.
func (r *EthReceipt) Cid() cid.Cid {
	return r.cid
}
