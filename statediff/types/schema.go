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
	`public.blocks`,
	[]column{
		{name: "block_number", typ: bigint},
		{name: "key", typ: text},
		{name: "data", typ: bytea},
	},
}

var TableNodeInfo = Table{
	Name: `public.nodes`,
	Columns: []column{
		{name: "genesis_block", typ: varchar},
		{name: "network_id", typ: varchar},
		{name: "node_id", typ: varchar},
		{name: "client_name", typ: varchar},
		{name: "chain_id", typ: integer},
	},
}

var TableHeader = Table{
	"eth.header_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "block_hash", typ: varchar},
		{name: "parent_hash", typ: varchar},
		{name: "cid", typ: text},
		{name: "td", typ: numeric},
		{name: "node_id", typ: varchar},
		{name: "reward", typ: numeric},
		{name: "state_root", typ: varchar},
		{name: "tx_root", typ: varchar},
		{name: "receipt_root", typ: varchar},
		{name: "uncle_root", typ: varchar},
		{name: "bloom", typ: bytea},
		{name: "timestamp", typ: numeric},
		{name: "mh_key", typ: text},
		{name: "times_validated", typ: integer},
		{name: "coinbase", typ: varchar},
	},
}

var TableStateNode = Table{
	"eth.state_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "header_id", typ: varchar},
		{name: "state_leaf_key", typ: varchar},
		{name: "cid", typ: text},
		{name: "state_path", typ: bytea},
		{name: "node_type", typ: integer},
		{name: "diff", typ: boolean},
		{name: "mh_key", typ: text},
	},
}

var TableStorageNode = Table{
	"eth.storage_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "header_id", typ: varchar},
		{name: "state_path", typ: bytea},
		{name: "storage_leaf_key", typ: varchar},
		{name: "cid", typ: text},
		{name: "storage_path", typ: bytea},
		{name: "node_type", typ: integer},
		{name: "diff", typ: boolean},
		{name: "mh_key", typ: text},
	},
}

var TableUncle = Table{
	"eth.uncle_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "block_hash", typ: varchar},
		{name: "header_id", typ: varchar},
		{name: "parent_hash", typ: varchar},
		{name: "cid", typ: text},
		{name: "reward", typ: numeric},
		{name: "mh_key", typ: text},
	},
}

var TableTransaction = Table{
	"eth.transaction_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "header_id", typ: varchar},
		{name: "tx_hash", typ: varchar},
		{name: "cid", typ: text},
		{name: "dst", typ: varchar},
		{name: "src", typ: varchar},
		{name: "index", typ: integer},
		{name: "mh_key", typ: text},
		{name: "tx_data", typ: bytea},
		{name: "tx_type", typ: integer},
		{name: "value", typ: numeric},
	},
}

var TableAccessListElement = Table{
	"eth.access_list_elements",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "tx_id", typ: varchar},
		{name: "index", typ: integer},
		{name: "address", typ: varchar},
		{name: "storage_keys", typ: varchar, isArray: true},
	},
}

var TableReceipt = Table{
	"eth.receipt_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "tx_id", typ: varchar},
		{name: "leaf_cid", typ: text},
		{name: "contract", typ: varchar},
		{name: "contract_hash", typ: varchar},
		{name: "leaf_mh_key", typ: text},
		{name: "post_state", typ: varchar},
		{name: "post_status", typ: integer},
		{name: "log_root", typ: varchar},
	},
}

var TableLog = Table{
	"eth.log_cids",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "leaf_cid", typ: text},
		{name: "leaf_mh_key", typ: text},
		{name: "rct_id", typ: varchar},
		{name: "address", typ: varchar},
		{name: "index", typ: integer},
		{name: "topic0", typ: varchar},
		{name: "topic1", typ: varchar},
		{name: "topic2", typ: varchar},
		{name: "topic3", typ: varchar},
		{name: "log_data", typ: bytea},
	},
}

var TableStateAccount = Table{
	"eth.state_accounts",
	[]column{
		{name: "block_number", typ: bigint},
		{name: "header_id", typ: varchar},
		{name: "state_path", typ: bytea},
		{name: "balance", typ: numeric},
		{name: "nonce", typ: bigint},
		{name: "code_hash", typ: bytea},
		{name: "storage_root", typ: varchar},
	},
}

var TableWatchedAddresses = Table{
	"eth_meta.watched_addresses",
	[]column{
		{name: "address", typ: varchar},
		{name: "created_at", typ: bigint},
		{name: "watched_at", typ: bigint},
		{name: "last_filled_at", typ: bigint},
	},
}
