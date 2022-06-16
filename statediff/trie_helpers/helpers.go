// Copyright 2019 The go-ethereum Authors
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

package trie_helpers

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

// CheckKeyType checks what type of key we have
func CheckKeyType(elements []interface{}) (types.NodeType, error) {
	if len(elements) > 2 {
		return types.Branch, nil
	}
	if len(elements) < 2 {
		return types.Unknown, fmt.Errorf("node cannot be less than two elements in length")
	}
	switch elements[0].([]byte)[0] / 16 {
	case '\x00':
		return types.Extension, nil
	case '\x01':
		return types.Extension, nil
	case '\x02':
		return types.Leaf, nil
	case '\x03':
		return types.Leaf, nil
	default:
		return types.Unknown, fmt.Errorf("unknown hex prefix")
	}
}

// ResolveNode return the state diff node pointed by the iterator.
func ResolveNode(it trie.NodeIterator, trieDB *trie.Database) (types.StateNode, []interface{}, error) {
	nodePath := make([]byte, len(it.Path()))
	copy(nodePath, it.Path())
	node, err := trieDB.Node(it.Hash())
	if err != nil {
		return types.StateNode{}, nil, err
	}
	var nodeElements []interface{}
	if err = rlp.DecodeBytes(node, &nodeElements); err != nil {
		return types.StateNode{}, nil, err
	}
	ty, err := CheckKeyType(nodeElements)
	if err != nil {
		return types.StateNode{}, nil, err
	}
	return types.StateNode{
		NodeType:  ty,
		Path:      nodePath,
		NodeValue: node,
	}, nodeElements, nil
}

// SortKeys sorts the keys in the account map
func SortKeys(data types.AccountMap) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// FindIntersection finds the set of strings from both arrays that are equivalent
// a and b must first be sorted
// this is used to find which keys have been both "deleted" and "created" i.e. they were updated
func FindIntersection(a, b []string) []string {
	lenA := len(a)
	lenB := len(b)
	iOfA, iOfB := 0, 0
	updates := make([]string, 0)
	if iOfA >= lenA || iOfB >= lenB {
		return updates
	}
	for {
		switch strings.Compare(a[iOfA], b[iOfB]) {
		// -1 when a[iOfA] < b[iOfB]
		case -1:
			iOfA++
			if iOfA >= lenA {
				return updates
			}
			// 0 when a[iOfA] == b[iOfB]
		case 0:
			updates = append(updates, a[iOfA])
			iOfA++
			iOfB++
			if iOfA >= lenA || iOfB >= lenB {
				return updates
			}
			// 1 when a[iOfA] > b[iOfB]
		case 1:
			iOfB++
			if iOfB >= lenB {
				return updates
			}
		}
	}

}
