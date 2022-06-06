package ipld

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"testing"

	block "github.com/ipfs/go-block-format"
	node "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/core/types"
)

func TestBlockBodyRlpParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-body-rlp-999999")
	checkError(err, t)

	output, _, _, err := FromBlockRLP(fi)
	checkError(err, t)

	testEthBlockFields(output, t)
}

func TestBlockHeaderRlpParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-header-rlp-999999")
	checkError(err, t)

	output, _, _, err := FromBlockRLP(fi)
	checkError(err, t)

	testEthBlockFields(output, t)
}

func TestBlockBodyJsonParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-block-body-json-999999")
	checkError(err, t)

	output, _, _, err := FromBlockJSON(fi)
	checkError(err, t)

	testEthBlockFields(output, t)
}

func TestEthBlockProcessTransactionsError(t *testing.T) {
	// Let's just change one byte in a field of one of these transactions.
	fi, err := os.Open("test_data/error-tx-eth-block-body-json-999999")
	checkError(err, t)

	_, _, _, err = FromBlockJSON(fi)
	if err == nil {
		t.Fatal("Expected an error")
	}
}

// TestDecodeBlockHeader should work for both inputs (block header and block body)
// as what we are storing is just the block header
func TestDecodeBlockHeader(t *testing.T) {
	storedEthBlock := prepareStoredEthBlock("test_data/eth-block-header-rlp-999999", t)

	ethBlock, err := DecodeEthHeader(storedEthBlock.Cid(), storedEthBlock.RawData())
	checkError(err, t)

	testEthBlockFields(ethBlock, t)
}

func TestEthBlockString(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)
	if ethBlock.String() != "<EthHeader bagiacgzawt5236hkiuvrhfyy4jya3qitlt6icfcqgheew6vsptlraokppm4a>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthHeader bagiacgzawt5236hkiuvrhfyy4jya3qitlt6icfcqgheew6vsptlraokppm4a>", ethBlock.String())
	}
}

func TestEthBlockLoggable(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	l := ethBlock.Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-header" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-header", l["type"])
	}
}

func TestEthBlockJSONMarshal(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	jsonOutput, err := ethBlock.MarshalJSON()
	checkError(err, t)

	var data map[string]interface{}
	err = json.Unmarshal(jsonOutput, &data)
	checkError(err, t)

	// Testing all fields is boring, but can help us to avoid
	// that dreaded regression
	if data["bloom"].(string)[:10] != "0x00000000" {
		t.Fatalf("Wrong Bloom\r\nexpected %s\r\ngot %s", "0x00000000", data["bloom"].(string)[:10])
		t.Fatal("Wrong Bloom")
	}
	if data["coinbase"] != "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5" {
		t.Fatalf("Wrong coinbase\r\nexpected %s\r\ngot %s", "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5", data["coinbase"])
	}
	if parseFloat(data["difficulty"]) != "12555463106190" {
		t.Fatalf("Wrong Difficulty\r\nexpected %s\r\ngot %s", "12555463106190", parseFloat(data["difficulty"]))
	}
	if data["extra"] != "0xd783010303844765746887676f312e342e32856c696e7578" {
		t.Fatalf("Wrong Extra\r\nexpected %s\r\ngot %s", "0xd783010303844765746887676f312e342e32856c696e7578", data["extra"])
	}
	if parseFloat(data["gaslimit"]) != "3141592" {
		t.Fatalf("Wrong Gas limit\r\nexpected %s\r\ngot %s", "3141592", parseFloat(data["gaslimit"]))
	}
	if parseFloat(data["gasused"]) != "231000" {
		t.Fatalf("Wrong Gas used\r\nexpected %s\r\ngot %s", "231000", parseFloat(data["gasused"]))
	}
	if data["mixdigest"] != "0x5b10f4a08a6c209d426f6158bd24b574f4f7b7aa0099c67c14a1f693b4dd04d0" {
		t.Fatalf("Wrong Mix digest\r\nexpected %s\r\ngot %s", "0x5b10f4a08a6c209d426f6158bd24b574f4f7b7aa0099c67c14a1f693b4dd04d0", data["mixdigest"])
	}
	if data["nonce"] != "0xf491f46b60fe04b3" {
		t.Fatalf("Wrong nonce\r\nexpected %s\r\ngot %s", "0xf491f46b60fe04b3", data["nonce"])
	}
	if parseFloat(data["number"]) != "999999" {
		t.Fatalf("Wrong block number\r\nexpected %s\r\ngot %s", "999999", parseFloat(data["number"]))
	}
	if parseMapElement(data["parent"]) != "bagiacgza2m6j3xu774hlvjxhd2fsnuv5ufom6ei4ply3mm3jrleeozt7b62a" {
		t.Fatalf("Wrong Parent cid\r\nexpected %s\r\ngot %s", "bagiacgza2m6j3xu774hlvjxhd2fsnuv5ufom6ei4ply3mm3jrleeozt7b62a", parseMapElement(data["parent"]))
	}
	if parseMapElement(data["receipts"]) != "bagkacgzap6qpnsrkagbdecgybaa63ljx4pr2aa5vlsetdg2f5mpzpbrk2iuq" {
		t.Fatalf("Wrong Receipt root cid\r\nexpected %s\r\ngot %s", "bagkacgzap6qpnsrkagbdecgybaa63ljx4pr2aa5vlsetdg2f5mpzpbrk2iuq", parseMapElement(data["receipts"]))
	}
	if parseMapElement(data["root"]) != "baglacgza5wmkus23dhec7m2tmtyikcfobjw6yzs7uv3ghxfjjroxavkm3yia" {
		t.Fatalf("Wrong root hash cid\r\nexpected %s\r\ngot %s", "baglacgza5wmkus23dhec7m2tmtyikcfobjw6yzs7uv3ghxfjjroxavkm3yia", parseMapElement(data["root"]))
	}
	if parseFloat(data["time"]) != "1455404037" {
		t.Fatalf("Wrong Time\r\nexpected %s\r\ngot %s", "1455404037", parseFloat(data["time"]))
	}
	if parseMapElement(data["tx"]) != "bagjacgzair6l3dci6smknejlccbrzx7vtr737s56onoksked2t5anxgxvzka" {
		t.Fatalf("Wrong Tx root cid\r\nexpected %s\r\ngot %s", "bagjacgzair6l3dci6smknejlccbrzx7vtr737s56onoksked2t5anxgxvzka", parseMapElement(data["tx"]))
	}
	if parseMapElement(data["uncles"]) != "bagiqcgzadxge32g6y5oxvk4fwvt3ntgudljreri3ssfhie7qufbp2qgusndq" {
		t.Fatalf("Wrong Uncle hash cid\r\nexpected %s\r\ngot %s", "bagiqcgzadxge32g6y5oxvk4fwvt3ntgudljreri3ssfhie7qufbp2qgusndq", parseMapElement(data["uncles"]))
	}
}

func TestEthBlockLinks(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	links := ethBlock.Links()
	if links[0].Cid.String() != "bagiacgza2m6j3xu774hlvjxhd2fsnuv5ufom6ei4ply3mm3jrleeozt7b62a" {
		t.Fatalf("Wrong cid for parent link\r\nexpected: %s\r\ngot %s", "bagiacgza2m6j3xu774hlvjxhd2fsnuv5ufom6ei4ply3mm3jrleeozt7b62a", links[0].Cid.String())
	}
	if links[1].Cid.String() != "bagkacgzap6qpnsrkagbdecgybaa63ljx4pr2aa5vlsetdg2f5mpzpbrk2iuq" {
		t.Fatalf("Wrong cid for receipt root link\r\nexpected: %s\r\ngot %s", "bagkacgzap6qpnsrkagbdecgybaa63ljx4pr2aa5vlsetdg2f5mpzpbrk2iuq", links[1].Cid.String())
	}
	if links[2].Cid.String() != "baglacgza5wmkus23dhec7m2tmtyikcfobjw6yzs7uv3ghxfjjroxavkm3yia" {
		t.Fatalf("Wrong cid for state root link\r\nexpected: %s\r\ngot %s", "baglacgza5wmkus23dhec7m2tmtyikcfobjw6yzs7uv3ghxfjjroxavkm3yia", links[2].Cid.String())
	}
	if links[3].Cid.String() != "bagjacgzair6l3dci6smknejlccbrzx7vtr737s56onoksked2t5anxgxvzka" {
		t.Fatalf("Wrong cid for tx root link\r\nexpected: %s\r\ngot %s", "bagjacgzair6l3dci6smknejlccbrzx7vtr737s56onoksked2t5anxgxvzka", links[3].Cid.String())
	}
	if links[4].Cid.String() != "bagiqcgzadxge32g6y5oxvk4fwvt3ntgudljreri3ssfhie7qufbp2qgusndq" {
		t.Fatalf("Wrong cid for uncles root link\r\nexpected: %s\r\ngot %s", "bagiqcgzadxge32g6y5oxvk4fwvt3ntgudljreri3ssfhie7qufbp2qgusndq", links[4].Cid.String())
	}
}

func TestEthBlockResolveEmptyPath(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, rest, err := ethBlock.Resolve([]string{})
	checkError(err, t)

	if ethBlock != obj.(*EthHeader) {
		t.Fatal("Should have returned the same eth-block object")
	}

	if len(rest) != 0 {
		t.Fatalf("Wrong len of rest of the path returned\r\nexpected %d\r\ngot %d", 0, len(rest))
	}
}

func TestEthBlockResolveNoSuchLink(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	_, _, err := ethBlock.Resolve([]string{"wewonthavethisfieldever"})
	if err == nil {
		t.Fatal("Should have failed with unknown field")
	}

	if err != ErrInvalidLink {
		t.Fatalf("Wrong error message\r\nexpected %s\r\ngot %s", ErrInvalidLink, err.Error())
	}
}

func TestEthBlockResolveBloom(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, rest, err := ethBlock.Resolve([]string{"bloom"})
	checkError(err, t)

	// The marshaler of types.Bloom should output it as 0x
	bloomInText := fmt.Sprintf("%x", obj.(types.Bloom))
	if bloomInText[:10] != "0000000000" {
		t.Fatalf("Wrong Bloom\r\nexpected %s\r\ngot %s", "0000000000", bloomInText[:10])
	}

	if len(rest) != 0 {
		t.Fatalf("Wrong len of rest of the path returned\r\nexpected %d\r\ngot %d", 0, len(rest))
	}
}

func TestEthBlockResolveBloomExtraPathElements(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, rest, err := ethBlock.Resolve([]string{"bloom", "unexpected", "extra", "elements"})
	if obj != nil {
		t.Fatal("Returned obj should be nil")
	}

	if rest != nil {
		t.Fatal("Returned rest should be nil")
	}

	if err.Error() != "unexpected path elements past bloom" {
		t.Fatalf("Wrong error\r\nexpected %s\r\ngot %s", "unexpected path elements past bloom", err.Error())
	}
}

func TestEthBlockResolveNonLinkFields(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	testCases := map[string][]string{
		"coinbase":   {"%x", "52bc44d5378309ee2abf1539bf71de1b7d7be3b5"},
		"difficulty": {"%s", "12555463106190"},
		"extra":      {"%s", "0xd783010303844765746887676f312e342e32856c696e7578"},
		"gaslimit":   {"%d", "3141592"},
		"gasused":    {"%d", "231000"},
		"mixdigest":  {"%x", "5b10f4a08a6c209d426f6158bd24b574f4f7b7aa0099c67c14a1f693b4dd04d0"},
		"nonce":      {"%x", "f491f46b60fe04b3"},
		"number":     {"%s", "999999"},
		"time":       {"%d", "1455404037"},
	}

	for field, value := range testCases {
		obj, rest, err := ethBlock.Resolve([]string{field})
		checkError(err, t)

		format := value[0]
		result := value[1]
		if fmt.Sprintf(format, obj) != result {
			t.Fatalf("Wrong %v\r\nexpected %v\r\ngot %s", field, result, fmt.Sprintf(format, obj))
		}

		if len(rest) != 0 {
			t.Fatalf("Wrong len of rest of the path returned\r\nexpected %d\r\ngot %d", 0, len(rest))
		}
	}
}

func TestEthBlockResolveNonLinkFieldsExtraPathElements(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	testCases := []string{
		"coinbase",
		"difficulty",
		"extra",
		"gaslimit",
		"gasused",
		"mixdigest",
		"nonce",
		"number",
		"time",
	}

	for _, field := range testCases {
		obj, rest, err := ethBlock.Resolve([]string{field, "unexpected", "extra", "elements"})
		if obj != nil {
			t.Fatal("Returned obj should be nil")
		}

		if rest != nil {
			t.Fatal("Returned rest should be nil")
		}

		if err.Error() != "unexpected path elements past "+field {
			t.Fatalf("Wrong error\r\nexpected %s\r\ngot %s", "unexpected path elements past "+field, err.Error())
		}

	}
}

func TestEthBlockResolveLinkFields(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	testCases := map[string]string{
		"parent":   "bagiacgza2m6j3xu774hlvjxhd2fsnuv5ufom6ei4ply3mm3jrleeozt7b62a",
		"receipts": "bagkacgzap6qpnsrkagbdecgybaa63ljx4pr2aa5vlsetdg2f5mpzpbrk2iuq",
		"root":     "baglacgza5wmkus23dhec7m2tmtyikcfobjw6yzs7uv3ghxfjjroxavkm3yia",
		"tx":       "bagjacgzair6l3dci6smknejlccbrzx7vtr737s56onoksked2t5anxgxvzka",
		"uncles":   "bagiqcgzadxge32g6y5oxvk4fwvt3ntgudljreri3ssfhie7qufbp2qgusndq",
	}

	for field, result := range testCases {
		obj, rest, err := ethBlock.Resolve([]string{field, "anything", "goes", "here"})
		checkError(err, t)

		lnk, ok := obj.(*node.Link)
		if !ok {
			t.Fatal("Returned object is not a link")
		}

		if lnk.Cid.String() != result {
			t.Fatalf("Wrong %s cid\r\nexpected %v\r\ngot %v", field, result, lnk.Cid.String())
		}

		for i, p := range []string{"anything", "goes", "here"} {
			if rest[i] != p {
				t.Fatalf("Wrong rest of the path returned\r\nexpected %s\r\ngot %s", p, rest[i])
			}
		}
	}
}

func TestEthBlockTreeBadParams(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	tree := ethBlock.Tree("non-empty-string", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = ethBlock.Tree("non-empty-string", 1)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = ethBlock.Tree("", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}
}

func TestEThBlockTree(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	tree := ethBlock.Tree("", 1)
	lookupElements := map[string]interface{}{
		"bloom":      nil,
		"coinbase":   nil,
		"difficulty": nil,
		"extra":      nil,
		"gaslimit":   nil,
		"gasused":    nil,
		"mixdigest":  nil,
		"nonce":      nil,
		"number":     nil,
		"parent":     nil,
		"receipts":   nil,
		"root":       nil,
		"time":       nil,
		"tx":         nil,
		"uncles":     nil,
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

/*
  The two functions above: TestEthBlockResolveNonLinkFields and
  TestEthBlockResolveLinkFields did all the heavy lifting. Then, we will
  just test two use cases.
*/
func TestEthBlockResolveLinksBadLink(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, rest, err := ethBlock.ResolveLink([]string{"supercalifragilist"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}

	if err != ErrInvalidLink {
		t.Fatalf("Expected error\r\nexpected %s\r\ngot %s", ErrInvalidLink, err)
	}
}

func TestEthBlockResolveLinksGoodLink(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, rest, err := ethBlock.ResolveLink([]string{"tx", "0", "0", "0"})
	if obj == nil {
		t.Fatalf("Expected valid *node.Link obj to be returned")
	}

	if rest == nil {
		t.Fatal("Expected rest to be returned")
	}
	for i, p := range []string{"0", "0", "0"} {
		if rest[i] != p {
			t.Fatalf("Wrong rest of the path returned\r\nexpected %s\r\ngot %s", p, rest[i])
		}
	}

	if err != nil {
		t.Fatal("Non error expected")
	}
}

/*
  These functions below should go away
  We are working on test coverage anyways...
*/
func TestEthBlockCopy(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	defer func() {
		r := recover()
		if r == (any)(nil) {
			t.Fatal("Expected panic")
		}
		if r != (any)("implement me") {
			t.Fatalf("Wrong panic message\r\nexpected %s\r\ngot %s", "'implement me'", r)
		}
	}()

	_ = ethBlock.Copy()
}

func TestEthBlockStat(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	obj, err := ethBlock.Stat()
	if obj == nil {
		t.Fatal("Expected a not null object node.NodeStat")
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

func TestEthBlockSize(t *testing.T) {
	ethBlock := prepareDecodedEthBlock("test_data/eth-block-header-rlp-999999", t)

	size, err := ethBlock.Size()
	if size != 0 {
		t.Fatalf("Wrong size\r\nexpected %d\r\ngot %d", 0, size)
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

/*
  AUXILIARS
*/

// checkError makes 3 lines into 1.
func checkError(err error, t *testing.T) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		t.Fatalf("[%v:%v] %v", fn, line, err)
	}
}

// parseFloat is a convenience function to test json output
func parseFloat(v interface{}) string {
	return strconv.FormatFloat(v.(float64), 'f', 0, 64)
}

// parseMapElement is a convenience function to tets json output
func parseMapElement(v interface{}) string {
	return v.(map[string]interface{})["/"].(string)
}

// prepareStoredEthBlock reads the block from a file source to get its rawdata
// and computes its cid, for then, feeding it into a new IPLD block function.
// So we can pretend that we got this block from the datastore
func prepareStoredEthBlock(filepath string, t *testing.T) *block.BasicBlock {
	// Prepare the "fetched block". This one is supposed to be in the datastore
	// and given away by github.com/ipfs/go-ipfs/merkledag
	fi, err := os.Open(filepath)
	checkError(err, t)

	b, err := ioutil.ReadAll(fi)
	checkError(err, t)

	c, err := RawdataToCid(MEthHeader, b, multihash.KECCAK_256)
	checkError(err, t)

	// It's good to clarify that this one below is an IPLD block
	storedEthBlock, err := block.NewBlockWithCid(b, c)
	checkError(err, t)

	return storedEthBlock
}

// prepareDecodedEthBlock is more complex than function above, as it stores a
// basic block and RLP-decodes it
func prepareDecodedEthBlock(filepath string, t *testing.T) *EthHeader {
	// Get the block from the datastore and decode it.
	storedEthBlock := prepareStoredEthBlock("test_data/eth-block-header-rlp-999999", t)
	ethBlock, err := DecodeEthHeader(storedEthBlock.Cid(), storedEthBlock.RawData())
	checkError(err, t)

	return ethBlock
}

// testEthBlockFields checks the fields of EthBlock one by one.
func testEthBlockFields(ethBlock *EthHeader, t *testing.T) {
	// Was the cid calculated?
	if ethBlock.Cid().String() != "bagiacgzawt5236hkiuvrhfyy4jya3qitlt6icfcqgheew6vsptlraokppm4a" {
		t.Fatalf("Wrong cid\r\nexpected %s\r\ngot %s", "bagiacgzawt5236hkiuvrhfyy4jya3qitlt6icfcqgheew6vsptlraokppm4a", ethBlock.Cid().String())
	}

	// Do we have the rawdata available?
	if fmt.Sprintf("%x", ethBlock.RawData()[:10]) != "f90218a0d33c9dde9fff" {
		t.Fatalf("Wrong Rawdata\r\nexpected %s\r\ngot %s", "f90218a0d33c9dde9fff", fmt.Sprintf("%x", ethBlock.RawData()[:10]))
	}

	// Proper Fields of types.Header
	if fmt.Sprintf("%x", ethBlock.ParentHash) != "d33c9dde9fff0ebaa6e71e8b26d2bda15ccf111c7af1b633698ac847667f0fb4" {
		t.Fatalf("Wrong ParentHash\r\nexpected %s\r\ngot %s", "d33c9dde9fff0ebaa6e71e8b26d2bda15ccf111c7af1b633698ac847667f0fb4", fmt.Sprintf("%x", ethBlock.ParentHash))
	}
	if fmt.Sprintf("%x", ethBlock.UncleHash) != "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347" {
		t.Fatalf("Wrong UncleHash field\r\nexpected %s\r\ngot %s", "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", fmt.Sprintf("%x", ethBlock.UncleHash))
	}
	if fmt.Sprintf("%x", ethBlock.Coinbase) != "52bc44d5378309ee2abf1539bf71de1b7d7be3b5" {
		t.Fatalf("Wrong Coinbase\r\nexpected %s\r\ngot %s", "52bc44d5378309ee2abf1539bf71de1b7d7be3b5", fmt.Sprintf("%x", ethBlock.Coinbase))
	}
	if fmt.Sprintf("%x", ethBlock.Root) != "ed98aa4b5b19c82fb35364f08508ae0a6dec665fa57663dca94c5d70554cde10" {
		t.Fatalf("Wrong Root\r\nexpected %s\r\ngot %s", "ed98aa4b5b19c82fb35364f08508ae0a6dec665fa57663dca94c5d70554cde10", fmt.Sprintf("%x", ethBlock.Root))
	}
	if fmt.Sprintf("%x", ethBlock.TxHash) != "447cbd8c48f498a6912b10831cdff59c7fbfcbbe735ca92883d4fa06dcd7ae54" {
		t.Fatalf("Wrong TxHash\r\nexpected %s\r\ngot %s", "447cbd8c48f498a6912b10831cdff59c7fbfcbbe735ca92883d4fa06dcd7ae54", fmt.Sprintf("%x", ethBlock.TxHash))
	}
	if fmt.Sprintf("%x", ethBlock.ReceiptHash) != "7fa0f6ca2a01823208d80801edad37e3e3a003b55c89319b45eb1f97862ad229" {
		t.Fatalf("Wrong ReceiptHash\r\nexpected %s\r\ngot %s", "7fa0f6ca2a01823208d80801edad37e3e3a003b55c89319b45eb1f97862ad229", fmt.Sprintf("%x", ethBlock.ReceiptHash))
	}
	if len(ethBlock.Bloom) != 256 {
		t.Fatalf("Wrong Bloom Length\r\nexpected %d\r\ngot %d", 256, len(ethBlock.Bloom))
	}
	if fmt.Sprintf("%x", ethBlock.Bloom[71:76]) != "0000000000" { // You wouldn't want me to print out the whole bloom field?
		t.Fatalf("Wrong Bloom\r\nexpected %s\r\ngot %s", "0000000000", fmt.Sprintf("%x", ethBlock.Bloom[71:76]))
	}
	if ethBlock.Difficulty.String() != "12555463106190" {
		t.Fatalf("Wrong Difficulty\r\nexpected %s\r\ngot %s", "12555463106190", ethBlock.Difficulty.String())
	}
	if ethBlock.Number.String() != "999999" {
		t.Fatalf("Wrong Block Number\r\nexpected %s\r\ngot %s", "999999", ethBlock.Number.String())
	}
	if ethBlock.GasLimit != uint64(3141592) {
		t.Fatalf("Wrong Gas Limit\r\nexpected %d\r\ngot %d", 3141592, ethBlock.GasLimit)
	}
	if ethBlock.GasUsed != uint64(231000) {
		t.Fatalf("Wrong Gas Used\r\nexpected %d\r\ngot %d", 231000, ethBlock.GasUsed)
	}
	if ethBlock.Time != uint64(1455404037) {
		t.Fatalf("Wrong Time\r\nexpected %d\r\ngot %d", 1455404037, ethBlock.Time)
	}
	if fmt.Sprintf("%x", ethBlock.Extra) != "d783010303844765746887676f312e342e32856c696e7578" {
		t.Fatalf("Wrong Extra\r\nexpected %s\r\ngot %s", "d783010303844765746887676f312e342e32856c696e7578", fmt.Sprintf("%x", ethBlock.Extra))
	}
	if fmt.Sprintf("%x", ethBlock.Nonce) != "f491f46b60fe04b3" {
		t.Fatalf("Wrong Nonce\r\nexpected %s\r\ngot %s", "f491f46b60fe04b3", fmt.Sprintf("%x", ethBlock.Nonce))
	}
	if fmt.Sprintf("%x", ethBlock.MixDigest) != "5b10f4a08a6c209d426f6158bd24b574f4f7b7aa0099c67c14a1f693b4dd04d0" {
		t.Fatalf("Wrong MixDigest\r\nexpected %s\r\ngot %s", "5b10f4a08a6c209d426f6158bd24b574f4f7b7aa0099c67c14a1f693b4dd04d0", fmt.Sprintf("%x", ethBlock.MixDigest))
	}
}
