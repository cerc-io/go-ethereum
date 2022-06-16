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

package test_helpers

import (
	"math/big"

	"github.com/ethereum/go-ethereum/params"
)

var (
	BalanceChange1000     = int64(1000)
	BalanceChange10000    = int64(10000)
	BalanceChange1Ether   = int64(params.Ether)
	Block1Account1Balance = big.NewInt(BalanceChange10000)
	Block2Account2Balance = big.NewInt(21000000000000)
	GasFees               = int64(params.GWei) * int64(params.TxGas)
	ContractGasLimit      = uint64(1000000)
)
