// VulcanizeDB
// Copyright © 2022 Vulcanize

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

package file_test

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	db     sql.Database
	sqlxdb *sqlx.DB
	err    error
	ind    interfaces.StateDiffIndexer
)

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}

func resetDB(t *testing.T) {
	test_helpers.TearDownDB(t, db)

	connStr := postgres.DefaultConfig.DbConnectionString()
	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func testLoadEmptyWatchedAddresses(t *testing.T) {
	expectedData := []common.Address{}

	rows, err := ind.LoadWatchedAddresses()
	require.NoError(t, err)

	expectTrue(t, len(rows) == len(expectedData))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

type res struct {
	Address      string `db:"address"`
	CreatedAt    uint64 `db:"created_at"`
	WatchedAt    uint64 `db:"watched_at"`
	LastFilledAt uint64 `db:"last_filled_at"`
}

// func testInsertWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract1Address,
// 			CreatedAt: contract1CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 	}
// 	expectedData := []res{
// 		{
// 			Address:      contract1Address,
// 			CreatedAt:    contract1CreatedAt,
// 			WatchedAt:    watchedAt1,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract2Address,
// 			CreatedAt:    contract2CreatedAt,
// 			WatchedAt:    watchedAt1,
// 			LastFilledAt: lastFilledAt,
// 		},
// 	}

// 	err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt1)))
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testInsertAlreadyWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract3Address,
// 			CreatedAt: contract3CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 	}
// 	expectedData := []res{
// 		{
// 			Address:      contract1Address,
// 			CreatedAt:    contract1CreatedAt,
// 			WatchedAt:    watchedAt1,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract2Address,
// 			CreatedAt:    contract2CreatedAt,
// 			WatchedAt:    watchedAt1,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract3Address,
// 			CreatedAt:    contract3CreatedAt,
// 			WatchedAt:    watchedAt2,
// 			LastFilledAt: lastFilledAt,
// 		},
// 	}

// 	err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testRemoveWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract3Address,
// 			CreatedAt: contract3CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 	}
// 	expectedData := []res{
// 		{
// 			Address:      contract1Address,
// 			CreatedAt:    contract1CreatedAt,
// 			WatchedAt:    watchedAt1,
// 			LastFilledAt: lastFilledAt,
// 		},
// 	}

// 	err = ind.RemoveWatchedAddresses(args)
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testRemoveNonWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract1Address,
// 			CreatedAt: contract1CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 	}
// 	expectedData := []res{}

// 	err = ind.RemoveWatchedAddresses(args)
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testSetWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract1Address,
// 			CreatedAt: contract1CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 		{
// 			Address:   contract3Address,
// 			CreatedAt: contract3CreatedAt,
// 		},
// 	}
// 	expectedData := []res{
// 		{
// 			Address:      contract1Address,
// 			CreatedAt:    contract1CreatedAt,
// 			WatchedAt:    watchedAt2,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract2Address,
// 			CreatedAt:    contract2CreatedAt,
// 			WatchedAt:    watchedAt2,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract3Address,
// 			CreatedAt:    contract3CreatedAt,
// 			WatchedAt:    watchedAt2,
// 			LastFilledAt: lastFilledAt,
// 		},
// 	}

// 	err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testSetAlreadyWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	args := []sdtypes.WatchAddressArg{
// 		{
// 			Address:   contract4Address,
// 			CreatedAt: contract4CreatedAt,
// 		},
// 		{
// 			Address:   contract2Address,
// 			CreatedAt: contract2CreatedAt,
// 		},
// 		{
// 			Address:   contract3Address,
// 			CreatedAt: contract3CreatedAt,
// 		},
// 	}
// 	expectedData := []res{
// 		{
// 			Address:      contract4Address,
// 			CreatedAt:    contract4CreatedAt,
// 			WatchedAt:    watchedAt3,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract2Address,
// 			CreatedAt:    contract2CreatedAt,
// 			WatchedAt:    watchedAt3,
// 			LastFilledAt: lastFilledAt,
// 		},
// 		{
// 			Address:      contract3Address,
// 			CreatedAt:    contract3CreatedAt,
// 			WatchedAt:    watchedAt3,
// 			LastFilledAt: lastFilledAt,
// 		},
// 	}

// 	err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt3)))
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testLoadWatchedAddresses(t *testing.T) {
// 	expectedData := []common.Address{
// 		common.HexToAddress(contract4Address),
// 		common.HexToAddress(contract2Address),
// 		common.HexToAddress(contract3Address),
// 	}

// 	rows, err := ind.LoadWatchedAddresses()
// 	require.NoError(t, err)

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testClearWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	expectedData := []res{}

// 	err = ind.ClearWatchedAddresses()
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }

// func testClearEmptyWatchedAddresses(t *testing.T, resetAndDumpData func(*testing.T)) {
// 	expectedData := []res{}

// 	err = ind.ClearWatchedAddresses()
// 	require.NoError(t, err)
// 	resetAndDumpData(t)

// 	rows := []res{}
// 	err = sqlxdb.Select(&rows, watchedAddressesPgGet)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	expectTrue(t, len(rows) == len(expectedData))
// 	for idx, row := range rows {
// 		require.Equal(t, expectedData[idx], row)
// 	}
// }
