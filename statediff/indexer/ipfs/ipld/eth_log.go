package ipld

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// EthLog (eth-log, codec 0x9a), represents an ethereum block header
type EthLog struct {
	*types.Log

	rawData []byte
	cid     cid.Cid
}

// Static (compile time) check that EthLog satisfies the node.Node interface.
var _ node.Node = (*EthLog)(nil)

// NewLog create a new EthLog IPLD node
func NewLog(log *types.Log) (*EthLog, error) {
	logRaw, err := rlp.EncodeToBytes(log)
	if err != nil {
		return nil, err
	}
	c, err := RawdataToCid(MEthLog, logRaw, mh.KECCAK_256)
	if err != nil {
		return nil, err
	}
	return &EthLog{
		Log:     log,
		cid:     c,
		rawData: logRaw,
	}, nil
}

// DecodeEthLogs takes a cid and its raw binary data
func DecodeEthLogs(c cid.Cid, b []byte) (*EthLog, error) {
	l := new(types.Log)
	if err := rlp.DecodeBytes(b, l); err != nil {
		return nil, err
	}
	return &EthLog{
		Log:     l,
		cid:     c,
		rawData: b,
	}, nil
}

/*
  Block INTERFACE
*/

// RawData returns the binary of the RLP encode of the log.
func (l *EthLog) RawData() []byte {
	return l.rawData
}

// Cid returns the cid of the receipt log.
func (l *EthLog) Cid() cid.Cid {
	return l.cid
}

// String is a helper for output
func (l *EthLog) String() string {
	return fmt.Sprintf("<EthereumLog %s>", l.cid)
}

// Loggable returns in a map the type of IPLD Link.
func (l *EthLog) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "eth-log",
	}
}

// Resolve resolves a path through this node, stopping at any link boundary
// and returning the object found as well as the remaining path to traverse
func (l *EthLog) Resolve(p []string) (interface{}, []string, error) {
	if len(p) == 0 {
		return l, nil, nil
	}

	if len(p) > 1 {
		return nil, nil, fmt.Errorf("unexpected path elements past %s", p[0])
	}

	switch p[0] {
	case "address":
		return l.Address, nil, nil
	case "data":
		// This is a []byte. By default they are marshalled into Base64.
		return fmt.Sprintf("0x%x", l.Data), nil, nil
	case "topics":
		return l.Topics, nil, nil
	case "logIndex":
		return l.Index, nil, nil
	case "removed":
		return l.Removed, nil, nil
	default:
		return nil, nil, ErrInvalidLink
	}
}

// Tree lists all paths within the object under 'path', and up to the given depth.
// To list the entire object (similar to `find .`) pass "" and -1
func (l *EthLog) Tree(p string, depth int) []string {
	if p != "" || depth == 0 {
		return nil
	}

	return []string{
		"address",
		"data",
		"topics",
		"logIndex",
		"removed",
	}
}

// ResolveLink is a helper function that calls resolve and asserts the
// output is a link
func (l *EthLog) ResolveLink(p []string) (*node.Link, []string, error) {
	obj, rest, err := l.Resolve(p)
	if err != nil {
		return nil, nil, err
	}

	if lnk, ok := obj.(*node.Link); ok {
		return lnk, rest, nil
	}

	return nil, nil, fmt.Errorf("resolved item was not a link")
}

// Copy will go away. It is here to comply with the Node interface.
func (l *EthLog) Copy() node.Node {
	panic("implement me")
}

// Links is a helper function that returns all links within this object
func (l *EthLog) Links() []*node.Link {
	return nil
}

// Stat will go away. It is here to comply with the interface.
func (l *EthLog) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

// Size will go away. It is here to comply with the interface.
func (l *EthLog) Size() (uint64, error) {
	return 0, nil
}
