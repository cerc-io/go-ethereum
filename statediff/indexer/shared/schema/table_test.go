package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/ethereum/go-ethereum/statediff/indexer/shared/schema"
)

var testHeaderTable = Table{
	Name: "eth.header_cids",
	Columns: []Column{
		{Name: "block_number", Type: Dbigint},
		{Name: "block_hash", Type: Dvarchar},
		{Name: "parent_hash", Type: Dvarchar},
		{Name: "cid", Type: Dtext},
		{Name: "td", Type: Dnumeric},
		{Name: "node_id", Type: Dvarchar},
		{Name: "reward", Type: Dnumeric},
		{Name: "state_root", Type: Dvarchar},
		{Name: "tx_root", Type: Dvarchar},
		{Name: "receipt_root", Type: Dvarchar},
		{Name: "uncle_root", Type: Dvarchar},
		{Name: "bloom", Type: Dbytea},
		{Name: "timestamp", Type: Dnumeric},
		{Name: "mh_key", Type: Dtext},
		{Name: "times_validated", Type: Dinteger},
		{Name: "coinbase", Type: Dvarchar},
	},
	UpsertClause: OnConflict("block_hash", "block_number").Set(
		"parent_hash",
		"cid",
		"td",
		"node_id",
		"reward",
		"state_root",
		"tx_root",
		"receipt_root",
		"uncle_root",
		"bloom",
		"timestamp",
		"mh_key",
		"times_validated",
		"coinbase",
	)}

func TestTable(t *testing.T) {
	headerUpsert := `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ON CONFLICT (block_hash, block_number) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
	headerNoUpsert := `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ON CONFLICT (block_hash, block_number) DO NOTHING`
	require.Equal(t, headerNoUpsert, testHeaderTable.ToInsertStatement(false))
	require.Equal(t, headerUpsert, testHeaderTable.ToInsertStatement(true))
}
