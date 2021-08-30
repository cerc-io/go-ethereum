package ipld

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// EthLogTrie (eth-tx-trie codec 0x9p) represents
// a node from the transaction trie in ethereum.
type EthLogTrie struct {
	*TrieNode
}

/*
  OUTPUT
*/

// DecodeEthLogTrie returns an EthLogTrie object from its cid and rawdata.
func DecodeEthLogTrie(c cid.Cid, b []byte) (*EthLogTrie, error) {
	tn, err := decodeTrieNode(c, b, decodeEthLogTrieLeaf)
	if err != nil {
		return nil, err
	}
	return &EthLogTrie{TrieNode: tn}, nil
}

// decodeEthLogTrieLeaf parses a eth-log-trie leaf
// from decoded RLP elements
func decodeEthLogTrieLeaf(i []interface{}) ([]interface{}, error) {
	l := new(types.Log)
	if err := rlp.DecodeBytes(i[1].([]byte), l); err != nil {
		return nil, err
	}
	c, err := RawdataToCid(MEthLogTrie, i[1].([]byte), multihash.KECCAK_256)
	if err != nil {
		return nil, err
	}

	return []interface{}{
		i[0].([]byte),
		&EthLog{
			Log:     l,
			cid:     c,
			rawData: i[1].([]byte),
		},
	}, nil
}

/*
  Block INTERFACE
*/

// RawData returns the binary of the RLP encode of the transaction.
func (t *EthLogTrie) RawData() []byte {
	return t.rawdata
}

// Cid returns the cid of the transaction.
func (t *EthLogTrie) Cid() cid.Cid {
	return t.cid
}

// String is a helper for output
func (t *EthLogTrie) String() string {
	return fmt.Sprintf("<EthereumLogTrie %s>", t.cid)
}

// Loggable returns in a map the type of IPLD Link.
func (t *EthLogTrie) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "eth-log-trie",
	}
}

// logTrie wraps a localTrie for use on the receipt trie.
type logTrie struct {
	*localTrie
}

// newLogTrie initializes and returns a logTrie.
func newLogTrie() *logTrie {
	return &logTrie{
		localTrie: newLocalTrie(),
	}
}

// getNodes invokes the localTrie, which computes the root hash of the
// log trie and returns its database keys, to return a slice
// of EthLogTrie nodes.
func (rt *logTrie) getNodes() ([]*EthLogTrie, error) {
	keys, err := rt.getKeys()
	if err != nil {
		return nil, err
	}

	out := make([]*EthLogTrie, 0, len(keys))
	for _, k := range keys {
		n, err := rt.getNodeFromDB(k)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}

	return out, nil
}

func (rt *logTrie) getNodeFromDB(key []byte) (*EthLogTrie, error) {
	rawdata, err := rt.db.Get(key)
	if err != nil {
		return nil, err
	}

	c, err := RawdataToCid(MEthLogTrie, rawdata, multihash.KECCAK_256)
	if err != nil {
		return nil, err
	}

	tn := &TrieNode{
		cid:     c,
		rawdata: rawdata,
	}
	return &EthLogTrie{TrieNode: tn}, nil
}

// getLeafNodes invokes the localTrie, which returns a slice
// of EthLogTrie leaf nodes.
func (rt *logTrie) getLeafNodes() ([]*EthLogTrie, []*nodeKey, error) {
	keys, err := rt.getLeafKeys()
	if err != nil {
		return nil, nil, err
	}

	out := make([]*EthLogTrie, 0, len(keys))
	for _, k := range keys {
		n, err := rt.getNodeFromDB(k.dbKey)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, n)
	}

	return out, keys, nil
}
