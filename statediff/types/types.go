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
	BlockNumber *big.Int        `json:"blockNumber"     gencodec:"required"`
	BlockHash   common.Hash     `json:"blockHash"       gencodec:"required"`
	Nodes       []StateLeafNode `json:"nodes"           gencodec:"required"`
	IPLDs       []IPLD          `json:"iplds"`
}

// AccountMap is a mapping of hex encoded path => account wrapper
type AccountMap map[string]AccountWrapper

// AccountWrapper is used to temporarily associate the unpacked node with its raw values
type AccountWrapper struct {
	Account *types.StateAccount
	LeafKey []byte
	CID     string
}

// StateLeafNode holds the data for a single state diff leaf node
type StateLeafNode struct {
	Removed        bool
	AccountWrapper AccountWrapper
	StorageDiff    []StorageLeafNode
}

// StorageLeafNode holds the data for a single storage diff node leaf node
type StorageLeafNode struct {
	Removed bool
	Value   []byte
	LeafKey []byte
	CID     string
}

// IPLD holds a cid:content pair, e.g. for codehash to code mappings or for intermediate node IPLD objects
type IPLD struct {
	CID     string
	Content []byte
}

// CodeAndCodeHash struct to hold codehash => code mappings
type CodeAndCodeHash struct {
	Hash common.Hash
	Code []byte
}

type StateNodeSink func(node StateLeafNode) error
type StorageNodeSink func(node StorageLeafNode) error
type IPLDSink func(IPLD) error

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
