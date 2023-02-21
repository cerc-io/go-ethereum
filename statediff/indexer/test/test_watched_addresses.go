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

package test

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/stretchr/testify/require"
)

type res struct {
	Address      string `db:"address"`
	CreatedAt    uint64 `db:"created_at"`
	WatchedAt    uint64 `db:"watched_at"`
	LastFilledAt uint64 `db:"last_filled_at"`
}

func TestLoadEmptyWatchedAddresses(t *testing.T, ind interfaces.StateDiffIndexer) {
	expectedData := []common.Address{}

	rows, err := ind.LoadWatchedAddresses()
	require.NoError(t, err)

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestInsertWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{
		{
			Address:      mocks.Contract1Address,
			CreatedAt:    mocks.Contract1CreatedAt,
			WatchedAt:    mocks.WatchedAt1,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract2Address,
			CreatedAt:    mocks.Contract2CreatedAt,
			WatchedAt:    mocks.WatchedAt1,
			LastFilledAt: mocks.LastFilledAt,
		},
	}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestInsertAlreadyWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{
		{
			Address:      mocks.Contract1Address,
			CreatedAt:    mocks.Contract1CreatedAt,
			WatchedAt:    mocks.WatchedAt1,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract2Address,
			CreatedAt:    mocks.Contract2CreatedAt,
			WatchedAt:    mocks.WatchedAt1,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract3Address,
			CreatedAt:    mocks.Contract3CreatedAt,
			WatchedAt:    mocks.WatchedAt2,
			LastFilledAt: mocks.LastFilledAt,
		},
	}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestRemoveWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{
		{
			Address:      mocks.Contract1Address,
			CreatedAt:    mocks.Contract1CreatedAt,
			WatchedAt:    mocks.WatchedAt1,
			LastFilledAt: mocks.LastFilledAt,
		},
	}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestRemoveNonWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestSetWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{
		{
			Address:      mocks.Contract1Address,
			CreatedAt:    mocks.Contract1CreatedAt,
			WatchedAt:    mocks.WatchedAt2,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract2Address,
			CreatedAt:    mocks.Contract2CreatedAt,
			WatchedAt:    mocks.WatchedAt2,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract3Address,
			CreatedAt:    mocks.Contract3CreatedAt,
			WatchedAt:    mocks.WatchedAt2,
			LastFilledAt: mocks.LastFilledAt,
		},
	}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestSetAlreadyWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{
		{
			Address:      mocks.Contract4Address,
			CreatedAt:    mocks.Contract4CreatedAt,
			WatchedAt:    mocks.WatchedAt3,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract2Address,
			CreatedAt:    mocks.Contract2CreatedAt,
			WatchedAt:    mocks.WatchedAt3,
			LastFilledAt: mocks.LastFilledAt,
		},
		{
			Address:      mocks.Contract3Address,
			CreatedAt:    mocks.Contract3CreatedAt,
			WatchedAt:    mocks.WatchedAt3,
			LastFilledAt: mocks.LastFilledAt,
		},
	}

	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestLoadWatchedAddresses(t *testing.T, ind interfaces.StateDiffIndexer) {
	expectedData := []common.Address{
		common.HexToAddress(mocks.Contract4Address),
		common.HexToAddress(mocks.Contract2Address),
		common.HexToAddress(mocks.Contract3Address),
	}

	rows, err := ind.LoadWatchedAddresses()
	require.NoError(t, err)

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestClearWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{}
	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}

func TestClearEmptyWatchedAddresses(t *testing.T, db sql.Database) {
	expectedData := []res{}
	rows := []res{}
	err = db.Select(context.Background(), &rows, watchedAddressesPgGet)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, len(expectedData), len(rows))
	for idx, row := range rows {
		require.Equal(t, expectedData[idx], row)
	}
}
