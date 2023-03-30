package ipld

import "github.com/ipfs/go-cid"

type IPLD interface {
	Cid() cid.Cid
	RawData() []byte
}
