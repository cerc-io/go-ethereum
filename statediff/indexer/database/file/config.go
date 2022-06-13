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

package file

import (
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

// Config holds params for writing sql statements out to a file
type Config struct {
	FilePath                 string
	WatchedAddressesFilePath string
	NodeInfo                 node.Info
}

// Type satisfies interfaces.Config
func (c Config) Type() shared.DBType {
	return shared.FILE
}

// TestConfig config for unit tests
var TestConfig = Config{
	FilePath:                 "./statediffing_test_file.sql",
	WatchedAddressesFilePath: "./statediffing_watched_addresses_test_file.sql",
	NodeInfo: node.Info{
		GenesisBlock: "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
		NetworkID:    "1",
		ChainID:      1,
		ID:           "1",
		ClientName:   "go-ethereum",
	},
}
