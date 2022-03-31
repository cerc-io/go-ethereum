// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package models

import "github.com/lib/pq"

// IPLDModel is the db model for public.blocks
type IPLDModel struct {
	BlockNumber string `db:"block_number"`
	Key         string `db:"key"`
	Data        []byte `db:"data"`
}

// HeaderModel is the db model for eth.header_cids
type HeaderModel struct {
	BlockNumber     string `db:"block_number"`
	BlockHash       string `db:"block_hash"`
	ParentHash      string `db:"parent_hash"`
	CID             string `db:"cid"`
	MhKey           string `db:"mh_key"`
	TotalDifficulty string `db:"td"`
	NodeID          string `db:"node_id"`
	Reward          string `db:"reward"`
	StateRoot       string `db:"state_root"`
	UncleRoot       string `db:"uncle_root"`
	TxRoot          string `db:"tx_root"`
	RctRoot         string `db:"receipt_root"`
	Bloom           []byte `db:"bloom"`
	Timestamp       uint64 `db:"timestamp"`
	TimesValidated  int64  `db:"times_validated"`
	Coinbase        string `db:"coinbase"`
}

// UncleModel is the db model for eth.uncle_cids
type UncleModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	BlockHash   string `db:"block_hash"`
	ParentHash  string `db:"parent_hash"`
	CID         string `db:"cid"`
	MhKey       string `db:"mh_key"`
	Reward      string `db:"reward"`
}

// TxModel is the db model for eth.transaction_cids
type TxModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	Index       int64  `db:"index"`
	TxHash      string `db:"tx_hash"`
	CID         string `db:"cid"`
	MhKey       string `db:"mh_key"`
	Dst         string `db:"dst"`
	Src         string `db:"src"`
	Data        []byte `db:"tx_data"`
	Type        uint8  `db:"tx_type"`
	Value       string `db:"value"`
}

// AccessListElementModel is the db model for eth.access_list_entry
type AccessListElementModel struct {
	BlockNumber string         `db:"block_number"`
	Index       int64          `db:"index"`
	TxID        string         `db:"tx_id"`
	Address     string         `db:"address"`
	StorageKeys pq.StringArray `db:"storage_keys"`
}

// ReceiptModel is the db model for eth.receipt_cids
type ReceiptModel struct {
	BlockNumber  string `db:"block_number"`
	TxID         string `db:"tx_id"`
	LeafCID      string `db:"leaf_cid"`
	LeafMhKey    string `db:"leaf_mh_key"`
	PostStatus   uint64 `db:"post_status"`
	PostState    string `db:"post_state"`
	Contract     string `db:"contract"`
	ContractHash string `db:"contract_hash"`
	LogRoot      string `db:"log_root"`
}

// StateNodeModel is the db model for eth.state_cids
type StateNodeModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	Path        []byte `db:"state_path"`
	StateKey    string `db:"state_leaf_key"`
	NodeType    int    `db:"node_type"`
	CID         string `db:"cid"`
	MhKey       string `db:"mh_key"`
	Diff        bool   `db:"diff"`
}

// StorageNodeModel is the db model for eth.storage_cids
type StorageNodeModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StatePath   []byte `db:"state_path"`
	Path        []byte `db:"storage_path"`
	StorageKey  string `db:"storage_leaf_key"`
	NodeType    int    `db:"node_type"`
	CID         string `db:"cid"`
	MhKey       string `db:"mh_key"`
	Diff        bool   `db:"diff"`
}

// StorageNodeWithStateKeyModel is a db model for eth.storage_cids + eth.state_cids.state_key
type StorageNodeWithStateKeyModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StatePath   []byte `db:"state_path"`
	Path        []byte `db:"storage_path"`
	StateKey    string `db:"state_leaf_key"`
	StorageKey  string `db:"storage_leaf_key"`
	NodeType    int    `db:"node_type"`
	CID         string `db:"cid"`
	MhKey       string `db:"mh_key"`
	Diff        bool   `db:"diff"`
}

// StateAccountModel is a db model for an eth state account (decoded value of state leaf node)
type StateAccountModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StatePath   []byte `db:"state_path"`
	Balance     string `db:"balance"`
	Nonce       uint64 `db:"nonce"`
	CodeHash    []byte `db:"code_hash"`
	StorageRoot string `db:"storage_root"`
}

// LogsModel is the db model for eth.logs
type LogsModel struct {
	BlockNumber string `db:"block_number"`
	ReceiptID   string `db:"rct_id"`
	LeafCID     string `db:"leaf_cid"`
	LeafMhKey   string `db:"leaf_mh_key"`
	Address     string `db:"address"`
	Index       int64  `db:"index"`
	Data        []byte `db:"log_data"`
	Topic0      string `db:"topic0"`
	Topic1      string `db:"topic1"`
	Topic2      string `db:"topic2"`
	Topic3      string `db:"topic3"`
}

// KnownGaps is the data structure for eth_meta.known_gaps
type KnownGapsModel struct {
	StartingBlockNumber string `db:"starting_block_number"`
	EndingBlockNumber   string `db:"ending_block_number"`
	CheckedOut          bool   `db:"checked_out"`
	ProcessingKey       int64  `db:"processing_key"`
}

// KnownGaps is the data structure for eth_meta.known_gaps
type KnownGapsModel struct {
	StartingBlockNumber string `db:"starting_block_number"`
	EndingBlockNumber   string `db:"ending_block_number"`
	CheckedOut          bool   `db:"checked_out"`
	ProcessingKey       int64  `db:"processing_key"`
}
