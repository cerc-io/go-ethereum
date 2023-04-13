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

package schema

var TableIPLDBlock = Table{
	Name: `ipld.blocks`,
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "key", Type: Dtext},
		{Name: "data", Type: Dbytea},
	},
	UpsertClause: OnConflict("block_number", "key"),
}

var TableNodeInfo = Table{
	Name: `public.nodes`,
	Columns: []Column{
		{Name: "genesis_block", Type: Dvarchar},
		{Name: "network_id", Type: Dvarchar},
		{Name: "node_id", Type: Dvarchar},
		{Name: "client_name", Type: Dvarchar},
		{Name: "chain_id", Type: Dinteger},
	},
}

var TableHeader = Table{
	Name: "eth.header_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "block_hash", Type: Dvarchar},
		{Name: "parent_hash", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "td", Type: Dnumeric},
		{Name: "node_ids", Type: Dvarchar, Array: true},
		{Name: "reward", Type: Dnumeric},
		{Name: "state_root", Type: Dvarchar},
		{Name: "tx_root", Type: Dvarchar},
		{Name: "receipt_root", Type: Dvarchar},
		{Name: "uncles_hash", Type: Dvarchar},
		{Name: "bloom", Type: Dbytea},
		{Name: "timestamp", Type: Dnumeric},
		{Name: "coinbase", Type: Dvarchar},
	},
	UpsertClause: OnConflict("block_number", "block_hash").Set(
		"parent_hash",
		"cid",
		"td",
		"node_ids",
		"reward",
		"state_root",
		"tx_root",
		"receipt_root",
		"uncles_hash",
		"bloom",
		"timestamp",
		"coinbase",
	)}

var TableStateNode = Table{
	Name: "eth.state_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "header_id", Type: Dvarchar},
		{Name: "state_leaf_key", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "diff", Type: Dboolean},
		{Name: "balance", Type: Dnumeric},
		{Name: "nonce", Type: Dbigint},
		{Name: "code_hash", Type: Dvarchar},
		{Name: "storage_root", Type: Dvarchar},
		{Name: "removed", Type: Dboolean},
	},
	UpsertClause: OnConflict("block_number", "header_id", "state_leaf_key"),
}

var TableStorageNode = Table{
	Name: "eth.storage_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "header_id", Type: Dvarchar},
		{Name: "state_leaf_key", Type: Dvarchar},
		{Name: "storage_leaf_key", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "diff", Type: Dboolean},
		{Name: "val", Type: Dbytea},
		{Name: "removed", Type: Dboolean},
	},
	UpsertClause: OnConflict("block_number", "header_id", "state_leaf_key", "storage_leaf_key"),
}

var TableUncle = Table{
	Name: "eth.uncle_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "block_hash", Type: Dvarchar},
		{Name: "header_id", Type: Dvarchar},
		{Name: "parent_hash", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "reward", Type: Dnumeric},
		{Name: "index", Type: Dinteger},
	},
	UpsertClause: OnConflict("block_number", "block_hash"),
}

var TableTransaction = Table{
	Name: "eth.transaction_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "header_id", Type: Dvarchar},
		{Name: "tx_hash", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "dst", Type: Dvarchar},
		{Name: "src", Type: Dvarchar},
		{Name: "index", Type: Dinteger},
		{Name: "tx_type", Type: Dinteger},
		{Name: "value", Type: Dnumeric},
	},
	UpsertClause: OnConflict("block_number", "header_id", "tx_hash"),
}

var TableReceipt = Table{
	Name: "eth.receipt_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "header_id", Type: Dvarchar},
		{Name: "tx_id", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "contract", Type: Dvarchar},
		{Name: "post_state", Type: Dvarchar},
		{Name: "post_status", Type: Dinteger},
	},
	UpsertClause: OnConflict("block_number", "header_id", "tx_id"),
}

var TableLog = Table{
	Name: "eth.log_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "header_id", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "rct_id", Type: Dvarchar},
		{Name: "address", Type: Dvarchar},
		{Name: "index", Type: Dinteger},
		{Name: "topic0", Type: Dvarchar},
		{Name: "topic1", Type: Dvarchar},
		{Name: "topic2", Type: Dvarchar},
		{Name: "topic3", Type: Dvarchar},
	},
	UpsertClause: OnConflict("block_number", "header_id", "rct_id", "index"),
}

var TableWatchedAddresses = Table{
	Name: "eth_meta.watched_addresses",
	Columns: []Column{
		{Name: "address", Type: Dvarchar},
		{Name: "created_at", Type: Dbigint},
		{Name: "watched_at", Type: Dbigint},
		{Name: "last_filled_at", Type: Dbigint},
	},
}
