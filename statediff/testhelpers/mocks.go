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

package testhelpers

import (
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/builder"
)

var MockError = errors.New("mock error")

type MockBuilder struct {
	OldStateRoot common.Hash
	NewStateRoot common.Hash
	BlockNumber int64
	BlockHash common.Hash
	stateDiff *builder.StateDiff
	builderError error
}

func (builder *MockBuilder) BuildStateDiff(oldStateRoot, newStateRoot common.Hash, blockNumber int64, blockHash common.Hash) (*builder.StateDiff, error) {
	builder.OldStateRoot = oldStateRoot
	builder.NewStateRoot = newStateRoot
	builder.BlockNumber = blockNumber
	builder.BlockHash = blockHash

	return builder.stateDiff, builder.builderError
}

func (builder *MockBuilder) SetStateDiffToBuild(stateDiff *builder.StateDiff) {
	builder.stateDiff = stateDiff
}

func (builder *MockBuilder) SetBuilderError(err error) {
	builder.builderError = err
}

type MockPublisher struct{
	StateDiff *builder.StateDiff
	publisherError error
}

func (publisher *MockPublisher) PublishStateDiff(sd *builder.StateDiff) (string, error) {
	publisher.StateDiff = sd
	return "", publisher.publisherError
}

func (publisher *MockPublisher) SetPublisherError(err error) {
	publisher.publisherError = err
}
