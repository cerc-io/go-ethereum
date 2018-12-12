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

package extractor_test

import (
	"math/big"
	"math/rand"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/ethereum/go-ethereum/statediff/extractor"
	statediff "github.com/ethereum/go-ethereum/statediff/builder"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/testhelpers"
)
var _ = ginkgo.Describe("Extractor", func() {
	var publisher testhelpers.MockPublisher
	var builder testhelpers.MockBuilder
	var currentBlockNumber *big.Int
	var parentBlock, currentBlock *types.Block
	var expectedStateDiff statediff.StateDiff
	var ex extractor.Extractor
	var err error

	ginkgo.BeforeEach(func() {
		publisher = testhelpers.MockPublisher{}
		builder = testhelpers.MockBuilder{}
		ex, err = extractor.NewExtractor(&builder, &publisher)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		blockNumber := rand.Int63()
		parentBlockNumber := big.NewInt(blockNumber - int64(1))
		currentBlockNumber = big.NewInt(blockNumber)
		parentBlock = types.NewBlock(&types.Header{Number: parentBlockNumber}, nil, nil, nil)
		currentBlock = types.NewBlock(&types.Header{Number: currentBlockNumber}, nil, nil, nil)

		expectedStateDiff = statediff.StateDiff{
			BlockNumber:     blockNumber,
			BlockHash:       currentBlock.Hash(),
			CreatedAccounts: nil,
			DeletedAccounts: nil,
			UpdatedAccounts: nil,
		}
	})

	ginkgo.It("builds a state diff struct", func() {
		builder.SetStateDiffToBuild(&expectedStateDiff)

		_, err = ex.ExtractStateDiff(*parentBlock, *currentBlock)

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(builder.OldStateRoot).To(gomega.Equal(parentBlock.Root()))
		gomega.Expect(builder.NewStateRoot).To(gomega.Equal(currentBlock.Root()))
		gomega.Expect(builder.BlockNumber).To(gomega.Equal(currentBlockNumber.Int64()))
		gomega.Expect(builder.BlockHash).To(gomega.Equal(currentBlock.Hash()))
	})

	ginkgo.It("returns an error if building the state diff fails", func() {
		builder.SetBuilderError(testhelpers.MockError)

		_, err = ex.ExtractStateDiff(*parentBlock, *currentBlock)

		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(gomega.MatchError(testhelpers.MockError))
	})

	ginkgo.It("publishes the state diff struct", func() {
		builder.SetStateDiffToBuild(&expectedStateDiff)

		_, err = ex.ExtractStateDiff(*parentBlock, *currentBlock)

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(publisher.StateDiff).To(gomega.Equal(&expectedStateDiff))
	})

	ginkgo.It("returns an error if publishing the diff fails", func() {
		publisher.SetPublisherError(testhelpers.MockError)

		_, err = ex.ExtractStateDiff(*parentBlock, *currentBlock)

		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(gomega.MatchError(testhelpers.MockError))
	})
})
