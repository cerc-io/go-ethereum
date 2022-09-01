// VulcanizeDB
// Copyright © 2021 Vulcanize

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

package shared

import (
	"fmt"
	"strings"
)

// DBType to explicitly type the kind of DB
type DBType string

const (
	POSTGRES DBType = "Postgres"
	DUMP     DBType = "Dump"
	FILE     DBType = "File"
	UNKNOWN  DBType = "Unknown"
)

// ResolveDBType resolves a DBType from a provided string
func ResolveDBType(str string) (DBType, error) {
	switch strings.ToLower(str) {
	case "postgres", "pg":
		return POSTGRES, nil
	case "dump", "d":
		return DUMP, nil
	case "file", "f", "fs":
		return FILE, nil
	default:
		return UNKNOWN, fmt.Errorf("unrecognized db type string: %s", str)
	}
}
