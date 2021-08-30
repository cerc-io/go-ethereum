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
	"fmt"
	"io"
	"io/ioutil"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// FromBlockRLP takes an RLP message representing
// an ethereum block header or body (header, ommers and txs)
// to return it as a set of IPLD nodes for further processing.
func FromBlockRLP(r io.Reader) (*EthHeader, []*EthTx, []*EthTxTrie, error) {
	// We may want to use this stream several times
	rawdata, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, nil, nil, err
	}

	// Let's try to decode the received element as a block body
	var decodedBlock types.Block
	err = rlp.Decode(bytes.NewBuffer(rawdata), &decodedBlock)
	if err != nil {
		if err.Error()[:41] != "rlp: expected input list for types.Header" {
			return nil, nil, nil, err
		}

		// Maybe it is just a header... (body sans ommers and txs)
		var decodedHeader types.Header
		err := rlp.Decode(bytes.NewBuffer(rawdata), &decodedHeader)
		if err != nil {
			return nil, nil, nil, err
		}

		c, err := RawdataToCid(MEthHeader, rawdata, multihash.KECCAK_256)
		if err != nil {
			return nil, nil, nil, err
		}
		// It was a header
		return &EthHeader{
			Header:  &decodedHeader,
			cid:     c,
			rawdata: rawdata,
		}, nil, nil, nil
	}

	// This is a block body (header + ommers + txs)
	// We'll extract the header bits here
	headerRawData := getRLP(decodedBlock.Header())
	c, err := RawdataToCid(MEthHeader, headerRawData, multihash.KECCAK_256)
	if err != nil {
		return nil, nil, nil, err
	}
	ethBlock := &EthHeader{
		Header:  decodedBlock.Header(),
		cid:     c,
		rawdata: headerRawData,
	}

	// Process the found eth-tx objects
	ethTxNodes, ethTxTrieNodes, err := processTransactions(decodedBlock.Transactions(),
		decodedBlock.Header().TxHash[:])
	if err != nil {
		return nil, nil, nil, err
	}

	return ethBlock, ethTxNodes, ethTxTrieNodes, nil
}

// FromBlockJSON takes the output of an ethereum client JSON API
// (i.e. parity or geth) and returns a set of IPLD nodes.
func FromBlockJSON(r io.Reader) (*EthHeader, []*EthTx, []*EthTxTrie, error) {
	var obj objJSONHeader
	dec := json.NewDecoder(r)
	err := dec.Decode(&obj)
	if err != nil {
		return nil, nil, nil, err
	}

	headerRawData := getRLP(obj.Result.Header)
	c, err := RawdataToCid(MEthHeader, headerRawData, multihash.KECCAK_256)
	if err != nil {
		return nil, nil, nil, err
	}
	ethBlock := &EthHeader{
		Header:  &obj.Result.Header,
		cid:     c,
		rawdata: headerRawData,
	}

	// Process the found eth-tx objects
	ethTxNodes, ethTxTrieNodes, err := processTransactions(obj.Result.Transactions,
		obj.Result.Header.TxHash[:])
	if err != nil {
		return nil, nil, nil, err
	}

	return ethBlock, ethTxNodes, ethTxTrieNodes, nil
}

// FromBlockAndReceipts takes a block and processes it
// to return it a set of IPLD nodes for further processing.
func FromBlockAndReceipts(block *types.Block, receipts []*types.Receipt) (*EthHeader, []*EthHeader, []*EthTx, []*EthTxTrie, []*EthReceipt, []*EthRctTrie, [][]*EthLogTrie, [][]cid.Cid, error) {
	// Process the header
	headerNode, err := NewEthHeader(block.Header())
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, err
	}

	// Process the uncles
	uncleNodes := make([]*EthHeader, len(block.Uncles()))
	for i, uncle := range block.Uncles() {
		uncleNode, err := NewEthHeader(uncle)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, err
		}
		uncleNodes[i] = uncleNode
	}

	// Process the txs
	txNodes, txTrieNodes, err := processTransactions(block.Transactions(),
		block.Header().TxHash[:])
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, err
	}

	// Process the receipts and logs
	rctNodes, tctTrieNodes, logTrieNodes, logLeafNodeCIDs, err := processReceiptsAndLogs(receipts,
		block.Header().ReceiptHash[:])

	return headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, tctTrieNodes, logTrieNodes, logLeafNodeCIDs, err
}

// processTransactions will take the found transactions in a parsed block body
// to return IPLD node slices for eth-tx and eth-tx-trie
func processTransactions(txs []*types.Transaction, expectedTxRoot []byte) ([]*EthTx, []*EthTxTrie, error) {
	var ethTxNodes []*EthTx
	transactionTrie := newTxTrie()

	for idx, tx := range txs {
		ethTx, err := NewEthTx(tx)
		if err != nil {
			return nil, nil, err
		}
		ethTxNodes = append(ethTxNodes, ethTx)
		if err := transactionTrie.add(idx, ethTx.RawData()); err != nil {
			return nil, nil, err
		}
	}

	if !bytes.Equal(transactionTrie.rootHash(), expectedTxRoot) {
		return nil, nil, fmt.Errorf("wrong transaction hash computed")
	}
	txTrieNodes, err := transactionTrie.getNodes()
	return ethTxNodes, txTrieNodes, err
}

// processReceiptsAndLogs will take in receipts
// to return IPLD node slices for eth-rct, eth-rct-trie, eth-log, eth-log-trie
func processReceiptsAndLogs(rcts []*types.Receipt, expectedRctRoot []byte) ([]*EthReceipt, []*EthRctTrie, [][]*EthLogTrie, [][]cid.Cid, error) {
	// Pre allocating memory.
	ethRctNodes := make([]*EthReceipt, 0, len(rcts))
	ethLogleafNodeCids := make([][]cid.Cid, 0, len(rcts))
	ethLogTrieNodes := make([][]*EthLogTrie, 0, len(rcts))

	receiptTrie := newRctTrie()

	for idx, rct := range rcts {
		// Process logs for each receipt.
		logTrieNodes, leafNodeCids, logTrieHash, err := processLogs(rct.Logs)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		rct.LogRoot = logTrieHash
		ethLogTrieNodes = append(ethLogTrieNodes, logTrieNodes)
		ethLogleafNodeCids = append(ethLogleafNodeCids, leafNodeCids)

		ethRct, err := NewReceipt(rct)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		ethRctNodes = append(ethRctNodes, ethRct)
		if err = receiptTrie.add(idx, ethRct.RawData()); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if !bytes.Equal(receiptTrie.rootHash(), expectedRctRoot) {
		return nil, nil, nil, nil, fmt.Errorf("wrong receipt hash computed")
	}
	rctTrieNodes, err := receiptTrie.getNodes()
	return ethRctNodes, rctTrieNodes, ethLogTrieNodes, ethLogleafNodeCids, err
}

func processLogs(logs []*types.Log) ([]*EthLogTrie, []cid.Cid, common.Hash, error) {
	logTr := newLogTrie()
	for idx, log := range logs {
		ethLog, err := NewLog(log)
		if err != nil {
			return nil, nil, common.Hash{}, err
		}
		if err = logTr.add(idx, ethLog.RawData()); err != nil {
			return nil, nil, common.Hash{}, err
		}
	}

	logTrieNodes, err := logTr.getNodes()
	if err != nil {
		return nil, nil, common.Hash{}, err
	}

	leafNodes, keys, err := logTr.getLeafNodes()
	if err != nil {
		return nil, nil, common.Hash{}, err
	}

	leafNodeCids := make([]cid.Cid, len(leafNodes))
	for i, ln := range leafNodes {
		var idx uint

		r := bytes.NewReader(keys[i].trieKey)
		err = rlp.Decode(r, &idx)
		if err != nil {
			return nil, nil, common.Hash{}, err
		}
		leafNodeCids[idx] = ln.Cid()
	}

	return logTrieNodes, leafNodeCids, common.BytesToHash(logTr.rootHash()), err
}
