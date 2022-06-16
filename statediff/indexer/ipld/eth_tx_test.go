package ipld

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
)

/*
  EthBlock
  INPUT
*/

func TestTxInBlockBodyRlpParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-body-rlp-999999")
	checkError(err, t)

	_, output, _, err := FromBlockRLP(fi)
	checkError(err, t)

	if len(output) != 11 {
		t.Fatalf("Wrong number of parsed txs\r\nexpected %d\r\ngot %d", 11, len(output))
	}

	// Oh, let's just grab the last element and one from the middle
	testTx05Fields(output[5], t)
	testTx10Fields(output[10], t)
}

func TestTxInBlockHeaderRlpParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-header-rlp-999999")
	checkError(err, t)

	_, output, _, err := FromBlockRLP(fi)
	checkError(err, t)

	if len(output) != 0 {
		t.Fatalf("Wrong number of txs\r\nexpected %d\r\ngot %d", 0, len(output))
	}
}

func TestTxInBlockBodyJsonParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-body-json-999999")
	checkError(err, t)

	_, output, _, err := FromBlockJSON(fi)
	checkError(err, t)

	if len(output) != 11 {
		t.Fatalf("Wrong number of parsed txs\r\nexpected %d\r\ngot %d", 11, len(output))
	}

	testTx05Fields(output[5], t)
	testTx10Fields(output[10], t)
}

/*
  OUTPUT
*/

func TestDecodeTransaction(t *testing.T) {
	// Prepare the "fetched transaction".
	// This one is supposed to be in the datastore already,
	// and given away by github.com/ipfs/go-ipfs/merkledag
	rawTransactionString :=
		"f86c34850df84758008252089432be343b94f860124dc4fee278fdcbd38c102d88880f25" +
			"8512af0d4000801ba0e9a25c929c26d1a95232ba75aef419a91b470651eb77614695e16c" +
			"5ba023e383a0679fb2fc0d0b0f3549967c0894ee7d947f07d238a83ef745bc3ced5143a4af36"
	rawTransaction, err := hex.DecodeString(rawTransactionString)
	checkError(err, t)
	c, err := RawdataToCid(MEthTx, rawTransaction, multihash.KECCAK_256)
	checkError(err, t)

	// Just to clarify: This `block` is an IPFS block
	storedTransaction, err := block.NewBlockWithCid(rawTransaction, c)
	checkError(err, t)

	// Now the proper test
	ethTransaction, err := DecodeEthTx(storedTransaction.Cid(), storedTransaction.RawData())
	checkError(err, t)

	testTx05Fields(ethTransaction, t)
}

/*
  Block INTERFACE
*/

func TestEthTxLoggable(t *testing.T) {
	txs := prepareParsedTxs(t)

	l := txs[0].Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-tx" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-tx", l["type"])
	}
}

/*
  Node INTERFACE
*/

func TestEthTxResolve(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	// Empty path
	obj, rest, err := tx.Resolve([]string{})
	rtx, ok := obj.(*EthTx)
	if !ok {
		t.Fatal("Wrong type of returned object")
	}
	if rtx.Cid() != tx.Cid() {
		t.Fatalf("Wrong CID\r\nexpected %s\r\ngot %s", tx.Cid().String(), rtx.Cid().String())
	}
	if rest != nil {
		t.Fatal("est should be nil")
	}
	if err != nil {
		t.Fatal("err should be nil")
	}

	// len(p) > 1
	badCases := [][]string{
		{"two", "elements"},
		{"here", "three", "elements"},
		{"and", "here", "four", "elements"},
	}

	for _, bc := range badCases {
		obj, rest, err = tx.Resolve(bc)
		if obj != nil {
			t.Fatal("obj should be nil")
		}
		if rest != nil {
			t.Fatal("rest should be nil")
		}
		if err.Error() != fmt.Sprintf("unexpected path elements past %s", bc[0]) {
			t.Fatalf("wrong error\r\nexpected %s\r\ngot %s", fmt.Sprintf("unexpected path elements past %s", bc[0]), err.Error())
		}
	}

	moreBadCases := []string{
		"i",
		"am",
		"not",
		"a",
		"tx",
		"field",
	}
	for _, mbc := range moreBadCases {
		obj, rest, err = tx.Resolve([]string{mbc})
		if obj != nil {
			t.Fatal("obj should be nil")
		}
		if rest != nil {
			t.Fatal("rest should be nil")
		}

		if err != ErrInvalidLink {
			t.Fatalf("wrong error\r\nexpected %s\r\ngot %s", ErrInvalidLink, err)
		}
	}

	goodCases := []string{
		"gas",
		"gasPrice",
		"input",
		"nonce",
		"r",
		"s",
		"toAddress",
		"v",
		"value",
	}
	for _, gc := range goodCases {
		_, _, err = tx.Resolve([]string{gc})
		if err != nil {
			t.Fatalf("error should be nil %v", gc)
		}
	}

}

func TestEthTxTree(t *testing.T) {
	tx := prepareParsedTxs(t)[0]
	_ = tx

	// Bad cases
	tree := tx.Tree("non-empty-string", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = tx.Tree("non-empty-string", 1)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = tx.Tree("", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	// Good cases
	tree = tx.Tree("", 1)
	lookupElements := map[string]interface{}{
		"type":      nil,
		"gas":       nil,
		"gasPrice":  nil,
		"input":     nil,
		"nonce":     nil,
		"r":         nil,
		"s":         nil,
		"toAddress": nil,
		"v":         nil,
		"value":     nil,
	}

	if len(tree) != len(lookupElements) {
		t.Fatalf("Wrong number of elements\r\nexpected %d\r\ngot %d", len(lookupElements), len(tree))
	}

	for _, te := range tree {
		if _, ok := lookupElements[te]; !ok {
			t.Fatalf("Unexpected Element: %v", te)
		}
	}
}

func TestEthTxResolveLink(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	// bad case
	obj, rest, err := tx.ResolveLink([]string{"supercalifragilist"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err != ErrInvalidLink {
		t.Fatalf("Wrong error\r\nexpected %s\r\ngot %s", ErrInvalidLink, err.Error())
	}

	// good case
	obj, rest, err = tx.ResolveLink([]string{"nonce"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err.Error() != "resolved item was not a link" {
		t.Fatalf("Wrong error\r\nexpected %s\r\ngot %s", "resolved item was not a link", err.Error())
	}
}

func TestEthTxCopy(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Expected panic")
		}
		if r != "implement me" {
			t.Fatalf("Wrong panic message\r\nexpected %s\r\ngot %s", "'implement me'", r)
		}
	}()

	_ = tx.Copy()
}

func TestEthTxLinks(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	if tx.Links() != nil {
		t.Fatal("Links() expected to return nil")
	}
}

func TestEthTxStat(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	obj, err := tx.Stat()
	if obj == nil {
		t.Fatal("Expected a not null object node.NodeStat")
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

func TestEthTxSize(t *testing.T) {
	tx := prepareParsedTxs(t)[0]

	size, err := tx.Size()
	checkError(err, t)

	spl := strings.Split(tx.Transaction.Size().String(), " ")
	expectedSize, units := spl[0], spl[1]
	floatSize, err := strconv.ParseFloat(expectedSize, 64)
	checkError(err, t)

	var byteSize uint64
	switch units {
	case "B":
		byteSize = uint64(floatSize)
	case "KB":
		byteSize = uint64(floatSize * 1000)
	case "MB":
		byteSize = uint64(floatSize * 1000000)
	case "GB":
		byteSize = uint64(floatSize * 1000000000)
	case "TB":
		byteSize = uint64(floatSize * 1000000000000)
	default:
		t.Fatal("Unexpected size units")
	}
	if size != byteSize {
		t.Fatalf("Wrong size\r\nexpected %d\r\ngot %d", byteSize, size)
	}
}

/*
  AUXILIARS
*/

// prepareParsedTxs is a convenienve method
func prepareParsedTxs(t *testing.T) []*EthTx {
	fi, err := os.Open("test_data/eth-block-body-rlp-999999")
	checkError(err, t)

	_, output, _, err := FromBlockRLP(fi)
	checkError(err, t)

	return output
}

func testTx05Fields(ethTx *EthTx, t *testing.T) {
	// Was the cid calculated?
	if ethTx.Cid().String() != "bagjqcgzawhfnvdnpmpcfoug7d3tz53k2ht3cidr45pnw3y7snpd46azbpp2a" {
		t.Fatalf("Wrong cid\r\nexpected %s\r\ngot %s\r\n", "bagjqcgzawhfnvdnpmpcfoug7d3tz53k2ht3cidr45pnw3y7snpd46azbpp2a", ethTx.Cid().String())
	}

	// Do we have the rawdata available?
	if fmt.Sprintf("%x", ethTx.RawData()[:10]) != "f86c34850df847580082" {
		t.Fatalf("Wrong Rawdata\r\nexpected %s\r\ngot %s", "f86c34850df847580082", fmt.Sprintf("%x", ethTx.RawData()[:10]))
	}

	// Proper Fields of types.Transaction
	if fmt.Sprintf("%x", ethTx.To()) != "32be343b94f860124dc4fee278fdcbd38c102d88" {
		t.Fatalf("Wrong Recipient\r\nexpected %s\r\ngot %s", "32be343b94f860124dc4fee278fdcbd38c102d88", fmt.Sprintf("%x", ethTx.To()))
	}
	if len(ethTx.Data()) != 0 {
		t.Fatalf("Wrong len of Data\r\nexpected %d\r\ngot %d", 0, len(ethTx.Data()))
	}
	if fmt.Sprintf("%v", ethTx.Gas()) != "21000" {
		t.Fatalf("Wrong Gas\r\nexpected %s\r\ngot %s", "21000", fmt.Sprintf("%v", ethTx.Gas()))
	}
	if fmt.Sprintf("%v", ethTx.Value()) != "1091424800000000000" {
		t.Fatalf("Wrong Value\r\nexpected %s\r\ngot %s", "1091424800000000000", fmt.Sprintf("%v", ethTx.Value()))
	}
	if fmt.Sprintf("%v", ethTx.Nonce()) != "52" {
		t.Fatalf("Wrong Nonce\r\nexpected %s\r\ngot %s", "52", fmt.Sprintf("%v", ethTx.Nonce()))
	}
	if fmt.Sprintf("%v", ethTx.GasPrice()) != "60000000000" {
		t.Fatalf("Wrong Gas Price\r\nexpected %s\r\ngot %s", "60000000000", fmt.Sprintf("%v", ethTx.GasPrice()))
	}
}

func testTx10Fields(ethTx *EthTx, t *testing.T) {
	// Was the cid calculated?
	if ethTx.Cid().String() != "bagjqcgzaykakwayoec6j55zmq62cbvmplgf5u5j67affge3ksi4ermgitjoa" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "bagjqcgzaykakwayoec6j55zmq62cbvmplgf5u5j67affge3ksi4ermgitjoa", ethTx.Cid().String())
	}

	// Do we have the rawdata available?
	if fmt.Sprintf("%x", ethTx.RawData()[:10]) != "f8708302a120850ba43b" {
		t.Fatalf("Wrong Rawdata\r\nexpected %s\r\ngot %s", "f8708302a120850ba43b", fmt.Sprintf("%x", ethTx.RawData()[:10]))
	}

	// Proper Fields of types.Transaction
	if fmt.Sprintf("%x", ethTx.To()) != "1c51bf013add0857c5d9cf2f71a7f15ca93d4816" {
		t.Fatalf("Wrong Recipient\r\nexpected %s\r\ngot %s", "1c51bf013add0857c5d9cf2f71a7f15ca93d4816", fmt.Sprintf("%x", ethTx.To()))
	}
	if len(ethTx.Data()) != 0 {
		t.Fatalf("Wrong len of Data\r\nexpected %d\r\ngot %d", 0, len(ethTx.Data()))
	}
	if fmt.Sprintf("%v", ethTx.Gas()) != "90000" {
		t.Fatalf("Wrong Gas\r\nexpected %s\r\ngot %s", "90000", fmt.Sprintf("%v", ethTx.Gas()))
	}
	if fmt.Sprintf("%v", ethTx.Value()) != "1049756850000000000" {
		t.Fatalf("Wrong Value\r\nexpected %s\r\ngot %s", "1049756850000000000", fmt.Sprintf("%v", ethTx.Value()))
	}
	if fmt.Sprintf("%v", ethTx.Nonce()) != "172320" {
		t.Fatalf("Wrong Nonce\r\nexpected %s\r\ngot %s", "172320", fmt.Sprintf("%v", ethTx.Nonce()))
	}
	if fmt.Sprintf("%v", ethTx.GasPrice()) != "50000000000" {
		t.Fatalf("Wrong Gas Price\r\nexpected %s\r\ngot %s", "50000000000", fmt.Sprintf("%v", ethTx.GasPrice()))
	}
}
