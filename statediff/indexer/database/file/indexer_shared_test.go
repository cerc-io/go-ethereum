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

package file_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
)

var (
	legacyData      = mocks.NewLegacyData()
	mockLegacyBlock *types.Block
	legacyHeaderCID cid.Cid

	sqlxdb    *sqlx.DB
	err       error
	ind       interfaces.StateDiffIndexer
	ipfsPgGet = `SELECT data FROM public.blocks
					WHERE key = $1 AND block_number = $2`
	tx1, tx2, tx3, tx4, tx5, rct1, rct2, rct3, rct4, rct5                          []byte
	mockBlock                                                                      *types.Block
	headerCID, trx1CID, trx2CID, trx3CID, trx4CID, trx5CID                         cid.Cid
	rct1CID, rct2CID, rct3CID, rct4CID, rct5CID                                    cid.Cid
	rctLeaf1, rctLeaf2, rctLeaf3, rctLeaf4, rctLeaf5                               []byte
	state1CID, state2CID, storageCID                                               cid.Cid
	contract1Address, contract2Address, contract3Address, contract4Address         string
	contract1CreatedAt, contract2CreatedAt, contract3CreatedAt, contract4CreatedAt uint64
	lastFilledAt, watchedAt1, watchedAt2, watchedAt3                               uint64
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}

	mockBlock = mocks.MockBlock
	txs, rcts := mocks.MockBlock.Transactions(), mocks.MockReceipts

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

	headerCID, _ = ipld.RawdataToCid(ipld.MEthHeader, mocks.MockHeaderRlp, multihash.KECCAK_256)
	trx1CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx1, multihash.KECCAK_256)
	trx2CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx2, multihash.KECCAK_256)
	trx3CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx3, multihash.KECCAK_256)
	trx4CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx4, multihash.KECCAK_256)
	trx5CID, _ = ipld.RawdataToCid(ipld.MEthTx, tx5, multihash.KECCAK_256)
	state1CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.ContractLeafNode, multihash.KECCAK_256)
	state2CID, _ = ipld.RawdataToCid(ipld.MEthStateTrie, mocks.AccountLeafNode, multihash.KECCAK_256)
	storageCID, _ = ipld.RawdataToCid(ipld.MEthStorageTrie, mocks.StorageLeafNode, multihash.KECCAK_256)

	receiptTrie := ipld.NewRctTrie()

	receiptTrie.Add(0, rct1)
	receiptTrie.Add(1, rct2)
	receiptTrie.Add(2, rct3)
	receiptTrie.Add(3, rct4)
	receiptTrie.Add(4, rct5)

	rctLeafNodes, keys, _ := receiptTrie.GetLeafNodes()

	rctleafNodeCids := make([]cid.Cid, len(rctLeafNodes))
	orderedRctLeafNodes := make([][]byte, len(rctLeafNodes))
	for i, rln := range rctLeafNodes {
		var idx uint

		r := bytes.NewReader(keys[i].TrieKey)
		rlp.Decode(r, &idx)
		rctleafNodeCids[idx] = rln.Cid()
		orderedRctLeafNodes[idx] = rln.RawData()
	}

	rct1CID = rctleafNodeCids[0]
	rct2CID = rctleafNodeCids[1]
	rct3CID = rctleafNodeCids[2]
	rct4CID = rctleafNodeCids[3]
	rct5CID = rctleafNodeCids[4]

	rctLeaf1 = orderedRctLeafNodes[0]
	rctLeaf2 = orderedRctLeafNodes[1]
	rctLeaf3 = orderedRctLeafNodes[2]
	rctLeaf4 = orderedRctLeafNodes[3]
	rctLeaf5 = orderedRctLeafNodes[4]

	contract1Address = "0x5d663F5269090bD2A7DC2390c911dF6083D7b28F"
	contract2Address = "0x6Eb7e5C66DB8af2E96159AC440cbc8CDB7fbD26B"
	contract3Address = "0xcfeB164C328CA13EFd3C77E1980d94975aDfedfc"
	contract4Address = "0x0Edf0c4f393a628DE4828B228C48175b3EA297fc"
	contract1CreatedAt = uint64(1)
	contract2CreatedAt = uint64(2)
	contract3CreatedAt = uint64(3)
	contract4CreatedAt = uint64(4)

	lastFilledAt = uint64(0)
	watchedAt1 = uint64(10)
	watchedAt2 = uint64(15)
	watchedAt3 = uint64(20)
}

func expectTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Assertion failed")
	}
}

func resetDB(t *testing.T) {
	file.TearDownDB(t, sqlxdb)

	connStr := postgres.DefaultConfig.DbConnectionString()
	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}
