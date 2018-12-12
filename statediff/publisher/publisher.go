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

package publisher

import (
	"errors"

	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/builder"
	"github.com/ethereum/go-ethereum/statediff/publisher/ipfs"
)

type Publisher interface {
	PublishStateDiff(sd *builder.StateDiff) (string, error)
}

type publisher struct {
	ipfs.DagPutter
	statediff.Config
}

func NewPublisher(config statediff.Config) (*publisher, error) {
	adder, err := ipfs.NewAdder(config.Path)
	if err != nil {
		return nil, err
	}

	return &publisher{
		DagPutter: ipfs.NewDagPutter(adder),
		Config: config,
	}, nil
}

func (p *publisher) PublishStateDiff(sd *builder.StateDiff) (string, error) {
	switch p.Mode {
	case statediff.IPLD:
		cidStr, err := p.DagPut(sd)
		if err != nil {
			return "", err
		}

		return cidStr, err
	case statediff.LDB:
	case statediff.SQL:
	default:
	}

	return "", errors.New("state diff publisher: unhandled publishing mode")
}