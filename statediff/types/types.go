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

package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// StateRoots holds the state roots required for generating a state diff
type StateRoots struct {
	OldStateRoot, NewStateRoot common.Hash
}

// StateObject is the final output structure from the builder
type StateObject struct {
	BlockNumber       *big.Int          `json:"blockNumber"     gencodec:"required"`
	BlockHash         common.Hash       `json:"blockHash"       gencodec:"required"`
	Nodes             []StateNode       `json:"nodes"           gencodec:"required"`
	CodeAndCodeHashes []CodeAndCodeHash `json:"codeMapping"`
}

// AccountMap is a mapping of hex encoded path => account wrapper
type AccountMap map[string]AccountWrapper

// AccountWrapper is used to temporary associate the unpacked node with its raw values
type AccountWrapper struct {
	Account   *types.StateAccount
	NodeType  NodeType
	Path      []byte
	NodeValue []byte
	LeafKey   []byte
}

// NodeType for explicitly setting type of node
type NodeType string

const (
	Unknown   NodeType = "Unknown"
	Branch    NodeType = "Branch"
	Extension NodeType = "Extension"
	Leaf      NodeType = "Leaf"
	Removed   NodeType = "Removed" // used to represent paths which have been emptied
)

func (n NodeType) Int() int {
	switch n {
	case Branch:
		return 0
	case Extension:
		return 1
	case Leaf:
		return 2
	case Removed:
		return 3
	default:
		return -1
	}
}

// StateNode holds the data for a single state diff node
type StateNode struct {
	NodeType     NodeType      `json:"nodeType"        gencodec:"required"`
	Path         []byte        `json:"path"            gencodec:"required"`
	NodeValue    []byte        `json:"value"           gencodec:"required"`
	StorageNodes []StorageNode `json:"storage"`
	LeafKey      []byte        `json:"leafKey"`
}

// StorageNode holds the data for a single storage diff node
type StorageNode struct {
	NodeType  NodeType `json:"nodeType"        gencodec:"required"`
	Path      []byte   `json:"path"            gencodec:"required"`
	NodeValue []byte   `json:"value"           gencodec:"required"`
	LeafKey   []byte   `json:"leafKey"`
}

// CodeAndCodeHash struct for holding codehash => code mappings
// we can't use an actual map because they are not rlp serializable
type CodeAndCodeHash struct {
	Hash common.Hash `json:"codeHash"`
	Code []byte      `json:"code"`
}

type StateNodeSink func(StateNode) error
type StorageNodeSink func(StorageNode) error
type CodeSink func(CodeAndCodeHash) error

// OperationType for type of WatchAddress operation
type OperationType string

const (
	Add    OperationType = "add"
	Remove OperationType = "remove"
	Set    OperationType = "set"
	Clear  OperationType = "clear"
)

// WatchAddressArg is a arg type for WatchAddress API
type WatchAddressArg struct {
	// Address represents common.Address
	Address   string
	CreatedAt uint64
}
