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
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// FromBlockRLP takes an RLP message representing
// an ethereum block header or body (header, ommers and txs)
// to return it as a set of IPLD nodes for further processing.
func FromBlockRLP(r io.Reader) (*EthHeader, []*EthTx, error) {
	// We may want to use this stream several times
	rawdata, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	// Let's try to decode the received element as a block body
	var decodedBlock types.Block
	err = rlp.Decode(bytes.NewBuffer(rawdata), &decodedBlock)
	if err != nil {
		if err.Error()[:41] != "rlp: expected input list for types.Header" {
			return nil, nil, err
		}

		// Maybe it is just a header... (body sans ommers and txs)
		var decodedHeader types.Header
		err := rlp.Decode(bytes.NewBuffer(rawdata), &decodedHeader)
		if err != nil {
			return nil, nil, err
		}

		c, err := RawdataToCid(MEthHeader, rawdata, multihash.KECCAK_256)
		if err != nil {
			return nil, nil, err
		}
		// It was a header
		return &EthHeader{
			Header:  &decodedHeader,
			cid:     c,
			rawdata: rawdata,
		}, nil, nil
	}

	// This is a block body (header + ommers + txs)
	// We'll extract the header bits here
	headerRawData := getRLP(decodedBlock.Header())
	c, err := RawdataToCid(MEthHeader, headerRawData, multihash.KECCAK_256)
	if err != nil {
		return nil, nil, err
	}
	ethBlock := &EthHeader{
		Header:  decodedBlock.Header(),
		cid:     c,
		rawdata: headerRawData,
	}

	// Process the found eth-tx objects
	ethTxNodes, err := processTransactions(decodedBlock.Transactions())
	if err != nil {
		return nil, nil, err
	}

	return ethBlock, ethTxNodes, nil
}

// FromBlockJSON takes the output of an ethereum client JSON API
// (i.e. parity or geth) and returns a set of IPLD nodes.
func FromBlockJSON(r io.Reader) (*EthHeader, []*EthTx, error) {
	var obj objJSONHeader
	dec := json.NewDecoder(r)
	err := dec.Decode(&obj)
	if err != nil {
		return nil, nil, err
	}

	headerRawData := getRLP(&obj.Result.Header)
	c, err := RawdataToCid(MEthHeader, headerRawData, multihash.KECCAK_256)
	if err != nil {
		return nil, nil, err
	}
	ethBlock := &EthHeader{
		Header:  &obj.Result.Header,
		cid:     c,
		rawdata: headerRawData,
	}

	// Process the found eth-tx objects
	ethTxNodes, err := processTransactions(obj.Result.Transactions)
	if err != nil {
		return nil, nil, err
	}

	return ethBlock, ethTxNodes, nil
}

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
	rctNodes, logNodes, err := processReceiptsAndLogs(receipts, block.Header().ReceiptHash[:])

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
func processReceiptsAndLogs(rcts []*types.Receipt, expectedRctRoot []byte) ([]*EthReceipt, [][]*EthLog, error) {
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
