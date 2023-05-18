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

package postgres

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

// DriverType to explicitly type the kind of sql driver we are using
type DriverType string

const (
	PGX     DriverType = "PGX"
	SQLX    DriverType = "SQLX"
	Unknown DriverType = "Unknown"
)

// Env variables
const (
	DATABASE_NAME     = "DATABASE_NAME"
	DATABASE_HOSTNAME = "DATABASE_HOSTNAME"
	DATABASE_PORT     = "DATABASE_PORT"
	DATABASE_USER     = "DATABASE_USER"
	DATABASE_PASSWORD = "DATABASE_PASSWORD"
)

// ResolveDriverType resolves a DriverType from a provided string
func ResolveDriverType(str string) (DriverType, error) {
	switch strings.ToLower(str) {
	case "pgx", "pgxpool":
		return PGX, nil
	case "sqlx":
		return SQLX, nil
	default:
		return Unknown, fmt.Errorf("unrecognized driver type string: %s", str)
	}
}

// TestConfig specifies default parameters for connecting to a testing DB
var TestConfig = Config{
	Hostname:     "localhost",
	Port:         8077,
	DatabaseName: "cerc_testing",
	Username:     "vdbm",
	Password:     "password",
	Driver:       SQLX,
}

// Config holds params for a Postgres db
type Config struct {
	// conn string params
	Hostname     string
	Port         int
	DatabaseName string
	Username     string
	Password     string

	// conn settings
	MaxConns        int
	MaxIdle         int
	MinConns        int
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
	ConnTimeout     time.Duration
	LogStatements   bool

	// node info params
	ID         string
	ClientName string

	// driver type
	Driver DriverType

	// toggle on/off upserts
	Upsert bool

	// toggle on/off CopyFrom
	CopyFrom bool
}

// Type satisfies interfaces.Config
func (c Config) Type() shared.DBType {
	return shared.POSTGRES
}

// DbConnectionString constructs and returns the connection string from the config
func (c Config) DbConnectionString() string {
	if len(c.Username) > 0 && len(c.Password) > 0 {
		return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
			c.Username, c.Password, c.Hostname, c.Port, c.DatabaseName)
	}
	if len(c.Username) > 0 && len(c.Password) == 0 {
		return fmt.Sprintf("postgresql://%s@%s:%d/%s?sslmode=disable",
			c.Username, c.Hostname, c.Port, c.DatabaseName)
	}
	return fmt.Sprintf("postgresql://%s:%d/%s?sslmode=disable", c.Hostname, c.Port, c.DatabaseName)
}

func (c Config) WithEnv() (Config, error) {
	if val := os.Getenv(DATABASE_NAME); val != "" {
		c.DatabaseName = val
	}
	if val := os.Getenv(DATABASE_HOSTNAME); val != "" {
		c.Hostname = val
	}
	if val := os.Getenv(DATABASE_PORT); val != "" {
		port, err := strconv.Atoi(val)
		if err != nil {
			return c, err
		}
		c.Port = port
	}
	if val := os.Getenv(DATABASE_USER); val != "" {
		c.Username = val
	}
	if val := os.Getenv(DATABASE_PASSWORD); val != "" {
		c.Password = val
	}
	return c, nil
}
