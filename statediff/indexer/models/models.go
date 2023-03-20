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

// IPLDModel is the db model for ipld.blocks
type IPLDModel struct {
	BlockNumber string `db:"block_number"`
	Key         string `db:"key"`
	Data        []byte `db:"data"`
}

// HeaderModel is the db model for eth.header_cids
type HeaderModel struct {
	BlockNumber     string         `db:"block_number"`
	BlockHash       string         `db:"block_hash"`
	ParentHash      string         `db:"parent_hash"`
	CID             string         `db:"cid"`
	TotalDifficulty string         `db:"td"`
	NodeIDs         pq.StringArray `db:"node_ids"`
	Reward          string         `db:"reward"`
	StateRoot       string         `db:"state_root"`
	UnclesHash      string         `db:"uncles_hash"`
	TxRoot          string         `db:"tx_root"`
	RctRoot         string         `db:"receipt_root"`
	Bloom           []byte         `db:"bloom"`
	Timestamp       uint64         `db:"timestamp"`
	Coinbase        string         `db:"coinbase"`
}

// UncleModel is the db model for eth.uncle_cids
type UncleModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	BlockHash   string `db:"block_hash"`
	ParentHash  string `db:"parent_hash"`
	CID         string `db:"cid"`
	Reward      string `db:"reward"`
	Index       int64  `db:"index"`
}

// TxModel is the db model for eth.transaction_cids
type TxModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	Index       int64  `db:"index"`
	TxHash      string `db:"tx_hash"`
	CID         string `db:"cid"`
	Dst         string `db:"dst"`
	Src         string `db:"src"`
	Type        uint8  `db:"tx_type"`
	Value       string `db:"value"`
}

// ReceiptModel is the db model for eth.receipt_cids
type ReceiptModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	TxID        string `db:"tx_id"`
	CID         string `db:"cid"`
	PostStatus  uint64 `db:"post_status"`
	PostState   string `db:"post_state"`
	Contract    string `db:"contract"`
}

// StateNodeModel is the db model for eth.state_cids
type StateNodeModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StateKey    string `db:"state_leaf_key"`
	Removed     bool   `db:"removed"`
	CID         string `db:"cid"`
	Diff        bool   `db:"diff"`
	Balance     string `db:"balance"`
	Nonce       uint64 `db:"nonce"`
	CodeHash    string `db:"code_hash"`
	StorageRoot string `db:"storage_root"`
}

// StorageNodeModel is the db model for eth.storage_cids
type StorageNodeModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	StateKey    string `db:"state_leaf_key"`
	StorageKey  string `db:"storage_leaf_key"`
	Removed     bool   `db:"removed"`
	CID         string `db:"cid"`
	Diff        bool   `db:"diff"`
	Value       []byte `db:"val"`
}

// LogsModel is the db model for eth.logs
type LogsModel struct {
	BlockNumber string `db:"block_number"`
	HeaderID    string `db:"header_id"`
	ReceiptID   string `db:"rct_id"`
	CID         string `db:"cid"`
	Address     string `db:"address"`
	Index       int64  `db:"index"`
	Topic0      string `db:"topic0"`
	Topic1      string `db:"topic1"`
	Topic2      string `db:"topic2"`
	Topic3      string `db:"topic3"`
}
