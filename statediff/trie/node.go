package trie

import (
	"fmt"

	"github.com/ethereum/go-ethereum/rlp"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

// CheckKeyType checks what type of key we have
func CheckKeyType(elements []interface{}) (sdtypes.NodeType, error) {
	if len(elements) > 2 {
		return sdtypes.Branch, nil
	}
	if len(elements) < 2 {
		return sdtypes.Unknown, fmt.Errorf("node cannot be less than two elements in length")
	}
	switch elements[0].([]byte)[0] / 16 {
	case '\x00':
		return sdtypes.Extension, nil
	case '\x01':
		return sdtypes.Extension, nil
	case '\x02':
		return sdtypes.Leaf, nil
	case '\x03':
		return sdtypes.Leaf, nil
	default:
		return sdtypes.Unknown, fmt.Errorf("unknown hex prefix")
	}
}

// ResolveNode return the state diff node pointed by the iterator.
func ResolveNode(it trie.NodeIterator, trieDB *trie.Database) (sdtypes.StateNode, []interface{}, error) {
	nodePath := make([]byte, len(it.Path()))
	copy(nodePath, it.Path())
	node, err := trieDB.Node(it.Hash())
	if err != nil {
		return sdtypes.StateNode{}, nil, err
	}
	var nodeElements []interface{}
	if err = rlp.DecodeBytes(node, &nodeElements); err != nil {
		return sdtypes.StateNode{}, nil, err
	}
	ty, err := CheckKeyType(nodeElements)
	if err != nil {
		return sdtypes.StateNode{}, nil, err
	}
	return sdtypes.StateNode{
		NodeType:  ty,
		Path:      nodePath,
		NodeValue: node,
	}, nodeElements, nil
}
