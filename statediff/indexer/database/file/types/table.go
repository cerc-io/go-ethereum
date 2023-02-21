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

import (
	"fmt"
	"strings"

	"github.com/thoas/go-funk"
)

type colType int

const (
	integer colType = iota
	boolean
	bigint
	numeric
	bytea
	varchar
	text
)

type column struct {
	name    string
	dbType  colType
	isArray bool
}
type Table struct {
	Name    string
	Columns []column
}

func (tbl *Table) ToCsvRow(args ...interface{}) []string {
	var row []string
	for i, col := range tbl.Columns {
		value := col.dbType.formatter()(args[i])

		if col.isArray {
			valueList := funk.Map(args[i], col.dbType.formatter()).([]string)
			value = fmt.Sprintf("{%s}", strings.Join(valueList, ","))
		}

		row = append(row, value)
	}
	return row
}

func (tbl *Table) VarcharColumns() []string {
	columns := funk.Filter(tbl.Columns, func(col column) bool {
		return col.dbType == varchar
	}).([]column)

	columnNames := funk.Map(columns, func(col column) string {
		return col.name
	}).([]string)

	return columnNames
}

type colfmt = func(interface{}) string

func sprintf(f string) colfmt {
	return func(x interface{}) string { return fmt.Sprintf(f, x) }
}

func (typ colType) formatter() colfmt {
	switch typ {
	case integer:
		return sprintf("%d")
	case boolean:
		return func(x interface{}) string {
			if x.(bool) {
				return "t"
			}
			return "f"
		}
	case bigint:
		return sprintf("%s")
	case numeric:
		return sprintf("%s")
	case bytea:
		return sprintf(`\x%x`)
	case varchar:
		return sprintf("%s")
	case text:
		return sprintf("%s")
	}
	panic("unreachable")
}
