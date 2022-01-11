// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package validator

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

type CustomDB struct {
	ethdb.Database
}

type Database struct {
	postgresdb ethdb.Database
	ethdb      ethdb.Database
}

func NewDB(edb ethdb.Database, postgresDB ethdb.Database) *Database {
	return &Database{
		postgresdb: postgresDB,
		ethdb:      edb,
	}
}

func NewDatabase(database ethdb.Database, postgresDB ethdb.Database) ethdb.Database {
	return &CustomDB{
		Database: NewDB(database, postgresDB),
	}
}

func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	return d.ethdb.NewIterator(prefix, start)
}

func (d *Database) Has(key []byte) (bool, error) {
	return d.ethdb.Has(key)
}

func (d *Database) Get(key []byte) ([]byte, error) {
	return d.postgresdb.Get(key)
}

func (d *Database) Put(key []byte, value []byte) error {
	return d.ethdb.Put(key, value)
}

func (d *Database) Delete(key []byte) error {
	return d.ethdb.Delete(key)
}

func (d *Database) Stat(property string) (string, error) {
	return d.ethdb.Stat(property)
}

func (d *Database) Compact(start []byte, limit []byte) error {
	return d.ethdb.Compact(start, limit)
}

// HasAncient returns an error as we don't have a backing chain freezer.
func (d *Database) HasAncient(kind string, number uint64) (bool, error) {
	return d.ethdb.HasAncient(kind, number)
}

// Ancient returns an error as we don't have a backing chain freezer.
func (d *Database) Ancient(kind string, number uint64) ([]byte, error) {
	return d.ethdb.Ancient(kind, number)
}

// AncientRange returns an error as we don't have a backing chain freezer.
func (d *Database) AncientRange(kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return d.ethdb.AncientRange(kind, start, max, maxByteSize)
}

// Ancients returns an error as we don't have a backing chain freezer.
func (d *Database) Ancients() (uint64, error) {
	return d.ethdb.Ancients()
}

// AncientSize returns an error as we don't have a backing chain freezer.
func (d *Database) AncientSize(kind string) (uint64, error) {
	return d.ethdb.AncientSize(kind)
}

// ModifyAncients is not supported.
func (d *Database) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return d.ethdb.ModifyAncients(fn)
}

func (d *Database) TruncateAncients(n uint64) error {
	return d.ethdb.TruncateAncients(n)
}

func (d *Database) Sync() error {
	return d.ethdb.Sync()
}

func (d *Database) NewBatch() ethdb.Batch {
	return d.ethdb.NewBatch()
}

func (d *Database) ReadAncients(fn func(ethdb.AncientReader) error) (err error) {
	return d.ethdb.ReadAncients(fn)
}

func (d *Database) Close() error {

	if err := d.postgresdb.Close(); err != nil {
		return err
	}
	return d.ethdb.Close()
}
