// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package ipfs

import (
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	ipld "gx/ipfs/QmcKKBwfz6FyQdHR2jsXrrF6XeSBXYL86anmWNewpFpoF5/go-ipld-format"

	"github.com/ethereum/go-ethereum/statediff/builder"
)

type StateDiffNode struct {
	*builder.StateDiff

	cid     *cid.Cid
	rawdata []byte
}

func (sdn *StateDiffNode) RawData() []byte {
	return sdn.rawdata
}

func (sdn *StateDiffNode) Cid() cid.Cid {
	return *sdn.cid
}

func (sdn StateDiffNode) String() string {
	return sdn.cid.String()
}

func (sdn StateDiffNode) Loggable() map[string]interface{} {
	return sdn.cid.Loggable()
}

func (sdn StateDiffNode) Resolve(path []string) (interface{}, []string, error) {
	panic("implement me")
}

func (sdn StateDiffNode) Tree(path string, depth int) []string {
	panic("implement me")
}

func (sdn StateDiffNode) ResolveLink(path []string) (*ipld.Link, []string, error) {
	panic("implement me")
}

func (sdn StateDiffNode) Copy() ipld.Node {
	panic("implement me")
}

func (sdn StateDiffNode) Links() []*ipld.Link {
	panic("implement me")
}

func (sdn StateDiffNode) Stat() (*ipld.NodeStat, error) {
	panic("implement me")
}

func (sdn StateDiffNode) Size() (uint64, error) {
	panic("implement me")
}