package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	db  sql.Database
	err error
	ind interfaces.StateDiffIndexer
)

func checkTxClosure(t *testing.T, idle, inUse, open int64) {
	require.Equal(t, idle, db.Stats().Idle())
	require.Equal(t, inUse, db.Stats().InUse())
	require.Equal(t, open, db.Stats().Open())
}

func tearDown(t *testing.T) {
	test_helpers.TearDownDB(t, db)
	require.NoError(t, ind.Close())
}
