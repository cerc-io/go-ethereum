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

package builder

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type StateDiff struct {
	BlockNumber     int64                                     `json:"blockNumber"      gencodec:"required"`
	BlockHash       common.Hash                               `json:"blockHash"        gencodec:"required"`
	CreatedAccounts map[common.Address]AccountDiffEventual    `json:"createdAccounts"  gencodec:"required"`
	DeletedAccounts map[common.Address]AccountDiffEventual    `json:"deletedAccounts"  gencodec:"required"`
	UpdatedAccounts map[common.Address]AccountDiffIncremental `json:"updatedAccounts"  gencodec:"required"`

	encoded []byte
	err     error
}

func (self *StateDiff) ensureEncoded() {
	if self.encoded == nil && self.err == nil {
		self.encoded, self.err = json.Marshal(self)
	}
}

// Implement Encoder interface for StateDiff
func (sd *StateDiff) Length() int {
	sd.ensureEncoded()
	return len(sd.encoded)
}

// Implement Encoder interface for StateDiff
func (sd *StateDiff) Encode() ([]byte, error) {
	sd.ensureEncoded()
	return sd.encoded, sd.err
}

type AccountDiffEventual struct {
	Nonce        DiffUint64            `json:"nonce"         gencodec:"required"`
	Balance      DiffBigInt            `json:"balance"       gencodec:"required"`
	Code         []byte                `json:"code"          gencodec:"required"`
	CodeHash     string                `json:"codeHash"      gencodec:"required"`
	ContractRoot DiffString            `json:"contractRoot"  gencodec:"required"`
	Storage      map[string]DiffString `json:"storage"       gencodec:"required"`
}

type AccountDiffIncremental struct {
	Nonce        DiffUint64            `json:"nonce"         gencodec:"required"`
	Balance      DiffBigInt            `json:"balance"       gencodec:"required"`
	CodeHash     string                `json:"codeHash"      gencodec:"required"`
	ContractRoot DiffString            `json:"contractRoot"  gencodec:"required"`
	Storage      map[string]DiffString `json:"storage"       gencodec:"required"`
}

type DiffString struct {
	NewValue *string `json:"newValue"  gencodec:"optional"`
	OldValue *string `json:"oldValue"  gencodec:"optional"`
}
type DiffUint64 struct {
	NewValue *uint64 `json:"newValue"  gencodec:"optional"`
	OldValue *uint64 `json:"oldValue"  gencodec:"optional"`
}
type DiffBigInt struct {
	NewValue *big.Int `json:"newValue"  gencodec:"optional"`
	OldValue *big.Int `json:"oldValue"  gencodec:"optional"`
}
