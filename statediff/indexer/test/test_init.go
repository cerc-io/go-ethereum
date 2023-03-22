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

package test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

var (
	err       error
	ipfsPgGet = `SELECT data FROM ipld.blocks
					WHERE key = $1 AND block_number = $2`
	watchedAddressesPgGet = `SELECT *
					FROM eth_meta.watched_addresses`
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5            []byte
	nonCanonicalBlockRct1, nonCanonicalBlockRct2                     []byte
	nonCanonicalBlock2Rct1, nonCanonicalBlock2Rct2                   []byte
	mockBlock, mockNonCanonicalBlock, mockNonCanonicalBlock2         *types.Block
	headerCID, mockNonCanonicalHeaderCID, mockNonCanonicalHeader2CID cid.Cid
	trx1CID, trx2CID, trx3CID, trx4CID, trx5CID                      cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                      cid.Cid
	nonCanonicalBlockRct1CID, nonCanonicalBlockRct2CID               cid.Cid
	nonCanonicalBlock2Rct1CID, nonCanonicalBlock2Rct2CID             cid.Cid
	state1CID, state2CID, storageCID                                 cid.Cid
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}

	// canonical block at LondonBlock height
	mockBlock = mocks.MockBlock
	txs, rcts := mocks.MockBlock.Transactions(), mocks.MockReceipts

	// non-canonical block at LondonBlock height
	mockNonCanonicalBlock = mocks.MockNonCanonicalBlock
	nonCanonicalBlockRcts := mocks.MockNonCanonicalBlockReceipts

	// non-canonical block at LondonBlock height + 1
	mockNonCanonicalBlock2 = mocks.MockNonCanonicalBlock2
	nonCanonicalBlock2Rcts := mocks.MockNonCanonicalBlock2Receipts

	// encode mock receipts
	buf := new(bytes.Buffer)
	txs.EncodeIndex(0, buf)
	tx1 = make([]byte, buf.Len())
	copy(tx1, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(1, buf)
	tx2 = make([]byte, buf.Len())
	copy(tx2, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(2, buf)
	tx3 = make([]byte, buf.Len())
	copy(tx3, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(3, buf)
	tx4 = make([]byte, buf.Len())
	copy(tx4, buf.Bytes())
	buf.Reset()

	txs.EncodeIndex(4, buf)
	tx5 = make([]byte, buf.Len())
	copy(tx5, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(0, buf)
	rct1 = make([]byte, buf.Len())
	copy(rct1, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(1, buf)
	rct2 = make([]byte, buf.Len())
	copy(rct2, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(2, buf)
	rct3 = make([]byte, buf.Len())
	copy(rct3, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(3, buf)
	rct4 = make([]byte, buf.Len())
	copy(rct4, buf.Bytes())
	buf.Reset()

	rcts.EncodeIndex(4, buf)
	rct5 = make([]byte, buf.Len())
	copy(rct5, buf.Bytes())
	buf.Reset()

	// encode mock receipts for non-canonical blocks
	nonCanonicalBlockRcts.EncodeIndex(0, buf)
	nonCanonicalBlockRct1 = make([]byte, buf.Len())
	copy(nonCanonicalBlockRct1, buf.Bytes())
	buf.Reset()

	nonCanonicalBlockRcts.EncodeIndex(1, buf)
	nonCanonicalBlockRct2 = make([]byte, buf.Len())
	copy(nonCanonicalBlockRct2, buf.Bytes())
	buf.Reset()

	nonCanonicalBlock2Rcts.EncodeIndex(0, buf)
	nonCanonicalBlock2Rct1 = make([]byte, buf.Len())
	copy(nonCanonicalBlock2Rct1, buf.Bytes())
	buf.Reset()

	nonCanonicalBlock2Rcts.EncodeIndex(1, buf)
	nonCanonicalBlock2Rct2 = make([]byte, buf.Len())
	copy(nonCanonicalBlock2Rct2, buf.Bytes())
	buf.Reset()

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
	mockNonCanonicalHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockNonCanonicalHeaderRlp, multihash.KECCAK_256)
	mockNonCanonicalHeader2CID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockNonCanonicalHeader2Rlp, multihash.KECCAK_256)
	trx1CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx1, multihash.KECCAK_256)
	trx2CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx2, multihash.KECCAK_256)
	trx3CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx3, multihash.KECCAK_256)
	trx4CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx4, multihash.KECCAK_256)
	trx5CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)
	rct1CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct1, multihash.KECCAK_256)
	rct2CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct2, multihash.KECCAK_256)
	rct3CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct3, multihash.KECCAK_256)
	rct4CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct4, multihash.KECCAK_256)
	rct5CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, rct5, multihash.KECCAK_256)

	// create raw receipts for non-canonical blocks
	nonCanonicalBlockRct1CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, nonCanonicalBlockRct1, multihash.KECCAK_256)
	nonCanonicalBlockRct2CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, nonCanonicalBlockRct2, multihash.KECCAK_256)

	nonCanonicalBlock2Rct1CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, nonCanonicalBlock2Rct1, multihash.KECCAK_256)
	nonCanonicalBlock2Rct2CID, _ = ipld.RawdataToCid(ipld.MEthTxReceipt, nonCanonicalBlock2Rct2, multihash.KECCAK_256)
}

// createRctModel creates a models.ReceiptModel object from a given ethereum receipt
func createRctModel(rct *types.Receipt, cid cid.Cid, blockNumber string) models.ReceiptModel {
	rctModel := models.ReceiptModel{
		BlockNumber: blockNumber,
		HeaderID:    rct.BlockHash.String(),
		TxID:        rct.TxHash.String(),
		CID:         cid.String(),
	}

	contract := shared.HandleZeroAddr(rct.ContractAddress)
	rctModel.Contract = contract

	if len(rct.PostState) == 0 {
		rctModel.PostStatus = rct.Status
	} else {
		rctModel.PostState = common.BytesToHash(rct.PostState).String()
	}

	return rctModel
}

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}
