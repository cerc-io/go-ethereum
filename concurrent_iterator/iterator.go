//
// Copyright © 2020 Vulcanize, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package iterator

import (
	"bytes"
	"math/bits"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
)

// PrefixBoundIterator is a NodeIterator constrained by a lower & upper bound (as hex path prefixes)
type PrefixBoundIterator struct {
	trie.NodeIterator
	StartPath []byte
	EndPath   []byte
}

func (it *PrefixBoundIterator) Next(descend bool) bool {
	if it.EndPath == nil {
		return it.NodeIterator.Next(descend)
	}
	if !it.NodeIterator.Next(descend) {
		return false
	}
	// stop if underlying iterator went past endKey
	cmp := bytes.Compare(it.Path(), it.EndPath)
	return cmp <= 0
}

// Iterator with an upper bound value (hex path prefix)
func NewPrefixBoundIterator(it trie.NodeIterator, from []byte, to []byte) *PrefixBoundIterator {
	return &PrefixBoundIterator{NodeIterator: it, StartPath: from, EndPath: to}
}

// generates nibble slice prefixes at uniform intervals
type prefixGenerator struct {
	current   []byte
	step      byte
	stepIndex uint
}

func newPrefixGenerator(nbins uint) prefixGenerator {
	if bits.OnesCount(nbins) != 1 {
		panic("nbins must be a power of 2")
	}
	// determine step dist. and path index at which to step
	var step byte
	var stepIndex uint
	for ; nbins != 0; stepIndex++ {
		divisor := byte(nbins & 0xF)
		if divisor != 0 {
			step = 0x10 / divisor
		}
		nbins = nbins >> 4
	}
	return prefixGenerator{
		current:   make([]byte, stepIndex),
		step:      step,
		stepIndex: stepIndex - 1,
	}
}

func (gen *prefixGenerator) Value() []byte {
	return gen.current
}

func (gen *prefixGenerator) HasNext() bool {
	return gen.current[0] < 0x10
}

func (gen *prefixGenerator) Next() {
	// increment the cursor, and handle overflow
	gen.current[gen.stepIndex] += gen.step
	overflow := false
	for ix := 0; ix < len(gen.current); ix++ {
		rix := len(gen.current) - 1 - ix // index in prefix is reverse
		if overflow {                    // apply overflow
			gen.current[rix]++
			overflow = false
		}
		// detect overflow at this index
		if rix != 0 && gen.current[rix] > 0xf {
			gen.current[rix] = 0
			overflow = true
		}
	}
}

// Generates paths that cut trie domain into `nbins` uniform conterminous bins (w/ opt. prefix)
// eg. MakePaths([], 2) => [[0] [8]]
//	   MakePaths([4], 32) => [[4 0 0] [4 0 8] [4 1 0]... [4 f 8]]
func MakePaths(prefix []byte, nbins uint) [][]byte {
	var res [][]byte
	for it := newPrefixGenerator(nbins); it.HasNext(); it.Next() {
		next := make([]byte, len(prefix))
		copy(next, prefix)
		next = append(next, it.Value()...)
		res = append(res, next)
	}
	return res
}

func eachPrefixRange(prefix []byte, nbins uint, callback func([]byte, []byte)) {
	prefixes := MakePaths(prefix, nbins)
	prefixes = append(prefixes, nil) // include tail
	prefixes[0] = nil                // set bin 0 left bound to nil to include root
	for i := 0; i < len(prefixes)-1; i++ {
		key := prefixes[i]
		if len(key)%2 != 0 { // zero-pad for odd-length keys
			key = append(key, 0)
		}
		callback(key, prefixes[i+1])
	}
}

// Cut a trie by path prefix, returning `nbins` iterators covering its subtries
func SubtrieIterators(tree state.Trie, nbins uint) []trie.NodeIterator {
	var iters []trie.NodeIterator
	eachPrefixRange(nil, nbins, func(from []byte, to []byte) {
		it := tree.NodeIterator(HexToKeyBytes(from))
		iters = append(iters, NewPrefixBoundIterator(it, from, to))
	})
	return iters
}

// Factory for per-bin subtrie iterators
type SubtrieIteratorFactory struct {
	tree                 state.Trie
	startPaths, endPaths [][]byte
}

func (fac *SubtrieIteratorFactory) Length() int { return len(fac.startPaths) }

func (fac *SubtrieIteratorFactory) IteratorAt(bin uint) *PrefixBoundIterator {
	it := fac.tree.NodeIterator(HexToKeyBytes(fac.startPaths[bin]))
	return NewPrefixBoundIterator(it, fac.startPaths[bin], fac.endPaths[bin])
}

// Cut a trie by path prefix, returning `nbins` iterators covering its subtries
func NewSubtrieIteratorFactory(tree state.Trie, nbins uint) SubtrieIteratorFactory {
	starts := make([][]byte, 0, nbins)
	ends := make([][]byte, 0, nbins)
	eachPrefixRange(nil, nbins, func(key []byte, endKey []byte) {
		starts = append(starts, key)
		ends = append(ends, endKey)
	})
	return SubtrieIteratorFactory{tree: tree, startPaths: starts, endPaths: ends}
}
