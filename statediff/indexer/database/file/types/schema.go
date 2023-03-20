// Copyright 2022 The go-ethereum Authors
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

var TableIPLDBlock = Table{
	`ipld.blocks`,
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "key", dbType: text},
		{name: "data", dbType: bytea},
	},
}

var TableNodeInfo = Table{
	Name: `public.nodes`,
	Columns: []column{
		{name: "genesis_block", dbType: varchar},
		{name: "network_id", dbType: varchar},
		{name: "node_id", dbType: varchar},
		{name: "client_name", dbType: varchar},
		{name: "chain_id", dbType: integer},
	},
}

var TableHeader = Table{
	"eth.header_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "block_hash", dbType: varchar},
		{name: "parent_hash", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "td", dbType: numeric},
		{name: "node_ids", dbType: varchar, isArray: true},
		{name: "reward", dbType: numeric},
		{name: "state_root", dbType: varchar},
		{name: "tx_root", dbType: varchar},
		{name: "receipt_root", dbType: varchar},
		{name: "uncles_hash", dbType: varchar},
		{name: "bloom", dbType: bytea},
		{name: "timestamp", dbType: numeric},
		{name: "coinbase", dbType: varchar},
	},
}

var TableStateNode = Table{
	"eth.state_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "state_leaf_key", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "removed", dbType: boolean},
		{name: "diff", dbType: boolean},
		{name: "balance", dbType: numeric},
		{name: "nonce", dbType: bigint},
		{name: "code_hash", dbType: varchar},
		{name: "storage_root", dbType: varchar},
	},
}

var TableStorageNode = Table{
	"eth.storage_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "state_leaf_key", dbType: varchar},
		{name: "storage_leaf_key", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "removed", dbType: boolean},
		{name: "diff", dbType: boolean},
		{name: "val", dbType: bytea},
	},
}

var TableUncle = Table{
	"eth.uncle_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "block_hash", dbType: varchar},
		{name: "header_id", dbType: varchar},
		{name: "parent_hash", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "reward", dbType: numeric},
		{name: "index", dbType: integer},
	},
}

var TableTransaction = Table{
	"eth.transaction_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "tx_hash", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "dst", dbType: varchar},
		{name: "src", dbType: varchar},
		{name: "index", dbType: integer},
		{name: "tx_type", dbType: integer},
		{name: "value", dbType: numeric},
	},
}

var TableReceipt = Table{
	"eth.receipt_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "tx_id", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "contract", dbType: varchar},
		{name: "post_state", dbType: varchar},
		{name: "post_status", dbType: integer},
	},
}

var TableLog = Table{
	"eth.log_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "rct_id", dbType: varchar},
		{name: "address", dbType: varchar},
		{name: "index", dbType: integer},
		{name: "topic0", dbType: varchar},
		{name: "topic1", dbType: varchar},
		{name: "topic2", dbType: varchar},
		{name: "topic3", dbType: varchar},
	},
}

var TableWatchedAddresses = Table{
	"eth_meta.watched_addresses",
	[]column{
		{name: "address", dbType: varchar},
		{name: "created_at", dbType: bigint},
		{name: "watched_at", dbType: bigint},
		{name: "last_filled_at", dbType: bigint},
	},
}
