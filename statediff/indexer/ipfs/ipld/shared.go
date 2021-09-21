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
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	sdtrie "github.com/ethereum/go-ethereum/statediff/trie"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// IPLD Codecs for Ethereum
// See the authoritative document:
// https://github.com/multiformats/multicodec/blob/master/table.csv
const (
	RawBinary           = 0x55
	MEthHeader          = 0x90
	MEthHeaderList      = 0x91
	MEthTxTrie          = 0x92
	MEthTx              = 0x93
	MEthTxReceiptTrie   = 0x94
	MEthTxReceipt       = 0x95
	MEthStateTrie       = 0x96
	MEthAccountSnapshot = 0x97
	MEthStorageTrie     = 0x98
	MEthLogTrie         = 0x99
	MEthLog             = 0x9a
)

var (
	nullHashBytes  = common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")
	ErrInvalidLink = errors.New("no such link")
)

// RawdataToCid takes the desired codec and a slice of bytes
// and returns the proper cid of the object.
func RawdataToCid(codec uint64, rawdata []byte, multiHash uint64) (cid.Cid, error) {
	c, err := cid.Prefix{
		Codec:    codec,
		Version:  1,
		MhType:   multiHash,
		MhLength: -1,
	}.Sum(rawdata)
	if err != nil {
		return cid.Cid{}, err
	}
	return c, nil
}

// keccak256ToCid takes a keccak256 hash and returns its cid based on
// the codec given.
func keccak256ToCid(codec uint64, h []byte) cid.Cid {
	buf, err := mh.Encode(h, mh.KECCAK_256)
	if err != nil {
		panic(err)
	}

	return cid.NewCidV1(codec, mh.Multihash(buf))
}

// commonHashToCid takes a go-ethereum common.Hash and returns its
// cid based on the codec given,
func commonHashToCid(codec uint64, h common.Hash) cid.Cid {
	mhash, err := mh.Encode(h[:], mh.KECCAK_256)
	if err != nil {
		panic(err)
	}

	return cid.NewCidV1(codec, mhash)
}

// localTrie wraps a go-ethereum trie and its underlying memory db.
// It contributes to the creation of the trie node objects.
type localTrie struct {
	db     ethdb.Database
	trieDB *trie.Database
	trie   *trie.Trie
}

// newLocalTrie initializes and returns a localTrie object
func newLocalTrie() *localTrie {
	var err error
	lt := &localTrie{}
	lt.db = rawdb.NewMemoryDatabase()
	lt.trieDB = trie.NewDatabase(lt.db)
	lt.trie, err = trie.New(common.Hash{}, lt.trieDB)
	if err != nil {
		panic(err)
	}
	return lt
}

// Add receives the index of an object and its rawdata value
// and includes it into the localTrie
func (lt *localTrie) Add(idx int, rawdata []byte) error {
	key, err := rlp.EncodeToBytes(uint(idx))
	if err != nil {
		panic(err)
	}
	return lt.trie.TryUpdate(key, rawdata)
}

// rootHash returns the computed trie root.
// Useful for sanity checks on parsed data.
func (lt *localTrie) rootHash() []byte {
	return lt.trie.Hash().Bytes()
}

func (lt *localTrie) commit() error {
	// commit trie nodes to trieDB
	var err error
	_, err = lt.trie.Commit(nil)
	if err != nil {
		return err
	}
	// commit trieDB to the underlying ethdb.Database
	if err := lt.trieDB.Commit(lt.trie.Hash(), false, nil); err != nil {
		return err
	}
	return nil
}

// getKeys returns the stored keys of the memory database
// of the localTrie for further processing.
func (lt *localTrie) getKeys() ([][]byte, error) {
	if err := lt.commit(); err != nil {
		return nil, err
	}

	// collect all of the node keys
	it := lt.trie.NodeIterator([]byte{})
	keyBytes := make([][]byte, 0)
	for it.Next(true) {
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		keyBytes = append(keyBytes, it.Hash().Bytes())
	}
	return keyBytes, nil
}

type nodeKey struct {
	dbKey   []byte
	TrieKey []byte
}

// getLeafKeys returns the stored leaf keys from the memory database
// of the localTrie for further processing.
func (lt *localTrie) getLeafKeys() ([]*nodeKey, error) {
	if err := lt.commit(); err != nil {
		return nil, err
	}

	it := lt.trie.NodeIterator([]byte{})
	leafKeys := make([]*nodeKey, 0)
	for it.Next(true) {
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}

		node, nodeElements, err := sdtrie.ResolveNode(it, lt.trieDB)
		if err != nil {
			return nil, err
		}

		if node.NodeType != sdtypes.Leaf {
			continue
		}

		partialPath := trie.CompactToHex(nodeElements[0].([]byte))
		valueNodePath := append(node.Path, partialPath...)
		encodedPath := trie.HexToCompact(valueNodePath)
		leafKey := encodedPath[1:]

		leafKeys = append(leafKeys, &nodeKey{dbKey: it.Hash().Bytes(), TrieKey: leafKey})
	}
	return leafKeys, nil
}

// getRLP encodes the given object to RLP returning its bytes.
func getRLP(object interface{}) []byte {
	buf := new(bytes.Buffer)
	if err := rlp.Encode(buf, object); err != nil {
		panic(err)
	}

	return buf.Bytes()
}
