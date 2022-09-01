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

package statediff

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/params"
)

// LoadConfig loads chain config from json file
func LoadConfig(chainConfigPath string) (*params.ChainConfig, error) {
	file, err := os.Open(chainConfigPath)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to read chain config file: %v", err))

		return nil, err
	}
	defer file.Close()

	chainConfig := new(params.ChainConfig)
	if err := json.NewDecoder(file).Decode(chainConfig); err != nil {
		log.Error(fmt.Sprintf("invalid chain config file: %v", err))

		return nil, err
	}

	log.Info(fmt.Sprintf("Using chain config from %s file. Content %+v", chainConfigPath, chainConfig))

	return chainConfig, nil
}

// ChainConfig returns the appropriate ethereum chain config for the provided chain id
func ChainConfig(chainID uint64) (*params.ChainConfig, error) {
	switch chainID {
	case 1:
		return params.MainnetChainConfig, nil
	case 3:
		return params.RopstenChainConfig, nil
	case 4:
		return params.RinkebyChainConfig, nil
	case 5:
		return params.GoerliChainConfig, nil
	default:
		return nil, fmt.Errorf("chain config for chainid %d not available", chainID)
	}
}
