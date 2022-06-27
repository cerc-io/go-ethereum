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
		{name: "node_id", dbType: varchar},
		{name: "reward", dbType: numeric},
		{name: "state_root", dbType: varchar},
		{name: "tx_root", dbType: varchar},
		{name: "receipt_root", dbType: varchar},
		{name: "uncle_root", dbType: varchar},
		{name: "bloom", dbType: bytea},
		{name: "timestamp", dbType: numeric},
		{name: "mh_key", dbType: text},
		{name: "times_validated", dbType: integer},
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
		{name: "state_path", dbType: bytea},
		{name: "node_type", dbType: integer},
		{name: "diff", dbType: boolean},
		{name: "mh_key", dbType: text},
	},
}

var TableStorageNode = Table{
	"eth.storage_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "state_path", dbType: bytea},
		{name: "storage_leaf_key", dbType: varchar},
		{name: "cid", dbType: text},
		{name: "storage_path", dbType: bytea},
		{name: "node_type", dbType: integer},
		{name: "diff", dbType: boolean},
		{name: "mh_key", dbType: text},
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
		{name: "mh_key", dbType: text},
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
		{name: "mh_key", dbType: text},
		{name: "tx_data", dbType: bytea},
		{name: "tx_type", dbType: integer},
		{name: "value", dbType: numeric},
	},
}

var TableAccessListElement = Table{
	"eth.access_list_elements",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "tx_id", dbType: varchar},
		{name: "index", dbType: integer},
		{name: "address", dbType: varchar},
		{name: "storage_keys", dbType: varchar, isArray: true},
	},
}

var TableReceipt = Table{
	"eth.receipt_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "tx_id", dbType: varchar},
		{name: "leaf_cid", dbType: text},
		{name: "contract", dbType: varchar},
		{name: "contract_hash", dbType: varchar},
		{name: "leaf_mh_key", dbType: text},
		{name: "post_state", dbType: varchar},
		{name: "post_status", dbType: integer},
		{name: "log_root", dbType: varchar},
	},
}

var TableLog = Table{
	"eth.log_cids",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "leaf_cid", dbType: text},
		{name: "leaf_mh_key", dbType: text},
		{name: "rct_id", dbType: varchar},
		{name: "address", dbType: varchar},
		{name: "index", dbType: integer},
		{name: "topic0", dbType: varchar},
		{name: "topic1", dbType: varchar},
		{name: "topic2", dbType: varchar},
		{name: "topic3", dbType: varchar},
		{name: "log_data", dbType: bytea},
	},
}

var TableStateAccount = Table{
	"eth.state_accounts",
	[]column{
		{name: "block_number", dbType: bigint},
		{name: "header_id", dbType: varchar},
		{name: "state_path", dbType: bytea},
		{name: "balance", dbType: numeric},
		{name: "nonce", dbType: bigint},
		{name: "code_hash", dbType: bytea},
		{name: "storage_root", dbType: varchar},
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
