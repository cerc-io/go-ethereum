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
	"github.com/ethereum/go-ethereum/rlp"
)

// EthHeader (eth-block, codec 0x90), represents an ethereum block header
type EthHeader struct {
	cid     cid.Cid
	rawdata []byte
}

// Static (compile time) check that EthHeader satisfies the node.Node interface.
var _ IPLD = (*EthHeader)(nil)

// NewEthHeader converts a *types.Header into an EthHeader IPLD node
func NewEthHeader(header *types.Header) (*EthHeader, error) {
	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return nil, err
	}
	c, err := RawdataToCid(MEthHeader, headerRLP, mh.KECCAK_256)
	if err != nil {
		return nil, err
	}
	return &EthHeader{
		cid:     c,
		rawdata: headerRLP,
	}, nil
}

// RawData returns the binary of the RLP encode of the block header.
func (b *EthHeader) RawData() []byte {
	return b.rawdata
}

// Cid returns the cid of the block header.
func (b *EthHeader) Cid() cid.Cid {
	return b.cid
}
