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
	"bytes"
	"encoding/gob"

	ipld "gx/ipfs/QmcKKBwfz6FyQdHR2jsXrrF6XeSBXYL86anmWNewpFpoF5/go-ipld-format"

	"github.com/ethereum/go-ethereum/statediff/builder"
)

const (
	EthStateDiffCode = 0x99 // Register custom codec for state diff?
)

type DagPutter interface {
	DagPut(sd *builder.StateDiff) (string, error)
}

type dagPutter struct {
	Adder
}

func NewDagPutter(adder Adder) *dagPutter {
	return &dagPutter{Adder: adder}
}

func (bhdp *dagPutter) DagPut(sd *builder.StateDiff) (string, error) {
	nd, err := bhdp.getNode(sd)
	if err != nil {
		return "", err
	}
	err = bhdp.Add(nd)
	if err != nil {
		return "", err
	}
	return nd.Cid().String(), nil
}

func (bhdp *dagPutter) getNode(sd *builder.StateDiff) (ipld.Node, error) {

	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(sd)
	if err != nil {
		return nil, err
	}

	raw := buff.Bytes()
	cid, err := RawToCid(EthStateDiffCode, raw)
	if err != nil {
		return nil, err
	}

	return &StateDiffNode{
		StateDiff: sd,
		cid:  cid,
		rawdata: raw,
	}, nil
}