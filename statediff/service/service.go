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

package service

import (
	"log"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/builder"
	"github.com/ethereum/go-ethereum/statediff/extractor"
	"github.com/ethereum/go-ethereum/statediff/publisher"
)

type StateDiffService struct {
	builder    builder.Builder
	extractor  extractor.Extractor
	blockchain *core.BlockChain
}

func NewStateDiffService(db ethdb.Database, blockChain *core.BlockChain) (*StateDiffService, error) {
	config := statediff.Config{}
	b := builder.NewBuilder(db)
	p, err := publisher.NewPublisher(config)
	if err != nil {
		return nil, nil
	}

	e, _ := extractor.NewExtractor(b, p)
	return &StateDiffService{
		blockchain: blockChain,
		extractor:  e,
	}, nil
}

func (StateDiffService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}

}

func (StateDiffService) APIs() []rpc.API {
	return []rpc.API{}
}

func (sds *StateDiffService) loop (sub event.Subscription, events chan core.ChainHeadEvent) {
	defer sub.Unsubscribe()

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				log.Fatalf("Error getting chain head event from subscription.")
			}
			log.Println("doing something with an event", ev)
		    previousBlock := ev.Block
			//TODO: figure out the best way to get the previous block
			currentBlock := ev.Block
			sds.extractor.ExtractStateDiff(*previousBlock, *currentBlock)
		}
	}

}
func (sds *StateDiffService) Start(server *p2p.Server) error {
	events := make(chan core.ChainHeadEvent, 10)
	sub := sds.blockchain.SubscribeChainHeadEvent(events)

	go sds.loop(sub, events)

	return nil
}
func (StateDiffService) Stop() error {
	return nil
}
