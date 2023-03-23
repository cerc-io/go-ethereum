// VulcanizeDB
// Copyright © 2019 Vulcanize

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

package ipld

import (
	"github.com/ethereum/go-ethereum/core/types"
)

// FromBlockAndReceipts takes a block and processes it
// to return it a set of IPLD nodes for further processing.
func FromBlockAndReceipts(block *types.Block, receipts []*types.Receipt) (*EthHeader, []*EthTx, []*EthReceipt, [][]*EthLog, error) {
	// Process the header
	headerNode, err := NewEthHeader(block.Header())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Process the txs
	txNodes, err := processTransactions(block.Transactions())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Process the receipts and logs
	rctNodes, logNodes, err := processReceiptsAndLogs(receipts)

	return headerNode, txNodes, rctNodes, logNodes, err
}

// processTransactions will take the found transactions in a parsed block body
// to return IPLD node slices for eth-tx
func processTransactions(txs []*types.Transaction) ([]*EthTx, error) {
	var ethTxNodes []*EthTx
	for _, tx := range txs {
		ethTx, err := NewEthTx(tx)
		if err != nil {
			return nil, err
		}
		ethTxNodes = append(ethTxNodes, ethTx)
	}

	return ethTxNodes, nil
}

// processReceiptsAndLogs will take in receipts
// to return IPLD node slices for eth-rct and eth-log
func processReceiptsAndLogs(rcts []*types.Receipt) ([]*EthReceipt, [][]*EthLog, error) {
	// Pre allocating memory.
	ethRctNodes := make([]*EthReceipt, len(rcts))
	ethLogNodes := make([][]*EthLog, len(rcts))

	for idx, rct := range rcts {
		logNodes, err := processLogs(rct.Logs)
		if err != nil {
			return nil, nil, err
		}

		ethRct, err := NewReceipt(rct)
		if err != nil {
			return nil, nil, err
		}

		ethRctNodes[idx] = ethRct
		ethLogNodes[idx] = logNodes
	}

	return ethRctNodes, ethLogNodes, nil
}

func processLogs(logs []*types.Log) ([]*EthLog, error) {
	logNodes := make([]*EthLog, len(logs))
	for idx, log := range logs {
		logNode, err := NewLog(log)
		if err != nil {
			return nil, err
		}
		logNodes[idx] = logNode
	}
	return logNodes, nil
}
