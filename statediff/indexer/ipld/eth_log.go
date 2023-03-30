package ipld

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// EthLog (eth-log, codec 0x9a), represents an ethereum block header
type EthLog struct {
	rawData []byte
	cid     cid.Cid
}

// Static (compile time) check that EthLog satisfies the node.Node interface.
var _ IPLD = (*EthLog)(nil)

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
		cid:     c,
		rawData: logRaw,
	}, nil
}

// RawData returns the binary of the RLP encode of the log.
func (l *EthLog) RawData() []byte {
	return l.rawData
}

// Cid returns the cid of the receipt log.
func (l *EthLog) Cid() cid.Cid {
	return l.cid
}
