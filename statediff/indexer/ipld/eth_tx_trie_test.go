package ipld

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

/*
  EthBlock
*/

func TestTxTriesInBlockBodyJSONParsing(t *testing.T) {
	// HINT: 306 txs
	// cat test_data/eth-block-body-json-4139497 | jsontool | grep transactionIndex | wc -l
	// or, https://etherscan.io/block/4139497
	fi, err := os.Open("test_data/eth-block-body-json-4139497")
	checkError(err, t)

	_, _, output, err := FromBlockJSON(fi)
	checkError(err, t)
	if len(output) != 331 {
		t.Fatalf("Wrong number of obtained tx trie nodes\r\nexpected %d\r\n got %d", 331, len(output))
	}
}

/*
  OUTPUT
*/

func TestTxTrieDecodeExtension(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	if ethTxTrie.nodeKind != "extension" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "extension", ethTxTrie.nodeKind)
	}

	if len(ethTxTrie.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(ethTxTrie.elements))
	}

	if fmt.Sprintf("%x", ethTxTrie.elements[0].([]byte)) != "0001" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "0001", fmt.Sprintf("%x", ethTxTrie.elements[0].([]byte)))
	}

	if ethTxTrie.elements[1].(cid.Cid).String() !=
		"bagjacgzak6wdjvshdtb7lrvlteweyd7f5qjr3dmzmh7g2xpi4xrwoujsio2a" {
		t.Fatalf("Wrong CID\r\nexpected %s\r\ngot %s", "bagjacgzak6wdjvshdtb7lrvlteweyd7f5qjr3dmzmh7g2xpi4xrwoujsio2a", ethTxTrie.elements[1].(cid.Cid).String())
	}
}

func TestTxTrieDecodeLeaf(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieLeaf(t)

	if ethTxTrie.nodeKind != "leaf" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "leaf", ethTxTrie.nodeKind)
	}

	if len(ethTxTrie.elements) != 2 {
		t.Fatalf("Wrong number of elements for a leaf node\r\nexpected %d\r\ngot %d", 2, len(ethTxTrie.elements))
	}

	if fmt.Sprintf("%x", ethTxTrie.elements[0].([]byte)) != "" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "", fmt.Sprintf("%x", ethTxTrie.elements[0].([]byte)))
	}

	if _, ok := ethTxTrie.elements[1].(*EthTx); !ok {
		t.Fatal("Expected element to be an EthTx")
	}

	if ethTxTrie.elements[1].(*EthTx).String() !=
		"<EthereumTx bagjqcgzaqsbvff5xrqh5lobxmhuharvkqdc4jmsqfalsu2xs4pbyix7dvfzq>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumTx bagjqcgzaqsbvff5xrqh5lobxmhuharvkqdc4jmsqfalsu2xs4pbyix7dvfzq>", ethTxTrie.elements[1].(*EthTx).String())
	}
}

func TestTxTrieDecodeBranch(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	if ethTxTrie.nodeKind != "branch" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "branch", ethTxTrie.nodeKind)
	}

	if len(ethTxTrie.elements) != 17 {
		t.Fatalf("Wrong number of elements for a branch node\r\nexpected %d\r\ngot %d", 17, len(ethTxTrie.elements))
	}

	for i, element := range ethTxTrie.elements {
		switch {
		case i < 9:
			if _, ok := element.(cid.Cid); !ok {
				t.Fatal("Expected element to be a cid")
			}
			continue
		default:
			if element != nil {
				t.Fatal("Expected element to be a nil")
			}
		}
	}
}

/*
  Block INTERFACE
*/

func TestEthTxTrieBlockElements(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	if fmt.Sprintf("%x", ethTxTrie.RawData())[:10] != "e4820001a0" {
		t.Fatalf("Wrong Data\r\nexpected %s\r\ngot %s", "e4820001a0", fmt.Sprintf("%x", ethTxTrie.RawData())[:10])
	}

	if ethTxTrie.Cid().String() !=
		"bagjacgzaw6ccgrfc3qnrl6joodbjjiet4haufnt2xww725luwgfhijnmg36q" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "bagjacgzaw6ccgrfc3qnrl6joodbjjiet4haufnt2xww725luwgfhijnmg36q", ethTxTrie.Cid().String())
	}
}

func TestEthTxTrieString(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	if ethTxTrie.String() != "<EthereumTxTrie bagjacgzaw6ccgrfc3qnrl6joodbjjiet4haufnt2xww725luwgfhijnmg36q>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumTxTrie bagjacgzaw6ccgrfc3qnrl6joodbjjiet4haufnt2xww725luwgfhijnmg36q>", ethTxTrie.String())
	}
}

func TestEthTxTrieLoggable(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)
	l := ethTxTrie.Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-tx-trie" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-tx-trie", l["type"])
	}
}

/*
  Node INTERFACE
*/

func TestTxTrieResolveExtension(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	_ = ethTxTrie
}

func TestTxTrieResolveLeaf(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieLeaf(t)

	_ = ethTxTrie
}

func TestTxTrieResolveBranch(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	indexes := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

	for j, index := range indexes {
		obj, rest, err := ethTxTrie.Resolve([]string{index, "nonce"})

		switch {
		case j < 9:
			_, ok := obj.(*node.Link)
			if !ok {
				t.Fatalf("Returned object is not a link (index: %d)", j)
			}

			if rest[0] != "nonce" {
				t.Fatalf("Wrong rest of the path returned\r\nexpected %s\r\ngot %s", "nonce", rest[0])
			}

			if err != nil {
				t.Fatal("Error should be nil")
			}

		default:
			if obj != nil {
				t.Fatalf("Returned object should have been nil")
			}

			if rest != nil {
				t.Fatalf("Rest of the path returned should be nil")
			}

			if err.Error() != "no such link in this branch" {
				t.Fatalf("Wrong error")
			}
		}
	}

	otherSuccessCases := [][]string{
		{"0", "1", "banana"},
		{"1", "banana"},
		{"7bc", "def"},
		{"bc", "def"},
	}

	for i := 0; i < len(otherSuccessCases); i = i + 2 {
		osc := otherSuccessCases[i]
		expectedRest := otherSuccessCases[i+1]

		obj, rest, err := ethTxTrie.Resolve(osc)
		_, ok := obj.(*node.Link)
		if !ok {
			t.Fatalf("Returned object is not a link")
		}

		for j := range expectedRest {
			if rest[j] != expectedRest[j] {
				t.Fatalf("Wrong rest of the path returned\r\nexpected %s\r\ngot %s", expectedRest[j], rest[j])
			}
		}

		if err != nil {
			t.Fatal("Error should be nil")
		}

	}
}

func TestTraverseTxTrieWithResolve(t *testing.T) {
	var err error

	txMap := prepareTxTrieMap(t)

	// This is the cid of the tx root at the block 4,139,497
	currentNode := txMap["bagjacgzaqolvvlyflkdiylijcu4ts6myxczkb2y3ewxmln5oyrsrkfc4v7ua"]

	// This is the path we want to traverse
	// the transaction id 256, which is RLP encoded to 820100
	var traversePath []string
	for _, s := range "820100" {
		traversePath = append(traversePath, string(s))
	}
	traversePath = append(traversePath, "value")

	var obj interface{}
	for {
		obj, traversePath, err = currentNode.Resolve(traversePath)
		link, ok := obj.(*node.Link)
		if !ok {
			break
		}
		if err != nil {
			t.Fatal("Error should be nil")
		}

		currentNode = txMap[link.Cid.String()]
		if currentNode == nil {
			t.Fatal("transaction trie node not found in memory map")
		}
	}

	if fmt.Sprintf("%v", obj) != "0xc495a958603400" {
		t.Fatalf("Wrong value\r\nexpected %s\r\ngot %s", "0xc495a958603400", fmt.Sprintf("%v", obj))
	}
}

func TestTxTrieTreeBadParams(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	tree := ethTxTrie.Tree("non-empty-string", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = ethTxTrie.Tree("non-empty-string", 1)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = ethTxTrie.Tree("", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}
}

func TestTxTrieTreeExtension(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	tree := ethTxTrie.Tree("", -1)

	if len(tree) != 1 {
		t.Fatalf("An extension should have one element")
	}

	if tree[0] != "01" {
		t.Fatalf("Wrong trie element\r\nexpected %s\r\ngot %s", "01", tree[0])
	}
}

func TestTxTrieTreeBranch(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	tree := ethTxTrie.Tree("", -1)

	lookupElements := map[string]interface{}{
		"0": nil,
		"1": nil,
		"2": nil,
		"3": nil,
		"4": nil,
		"5": nil,
		"6": nil,
		"7": nil,
		"8": nil,
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

func TestTxTrieLinksBranch(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	desiredValues := []string{
		"bagjacgzakhtcfpja453ydiaqxgidqmxhh7jwmxujib663deebwfs3m2n3hoa",
		"bagjacgza2p2fuqh4vumknq6x5w7i47usvtu5ixqins6qjjtcks4zge3vx3qq",
		"bagjacgza4fkhn7et3ra66yjkzbtvbxjefuketda6jctlut6it7gfahxhywga",
		"bagjacgzacnryeybs52xryrka5uxi4eg4hi2mh66esaghu7cetzu6fsukrynq",
		"bagjacgzastu5tc7lwz4ap3gznjwkyyepswquub7gvhags5mgdyfynnwbi43a",
		"bagjacgza5qgp76ovvorkydni2lchew6ieu5wb55w6hdliiu6vft7zlxtdhjq",
		"bagjacgzafnssc4yvln6zxmks5roskw4ckngta5n4yfy2skhlu435ve4b575a",
		"bagjacgzagkuei7qxfxefufme2d3xizxokkq4ad3rzl2x4dq2uao6dcr4va2a",
		"bagjacgzaxpaehtananrdxjghwukh2wwkkzcqwveppf6xclkrtd26rm27kqwq",
	}

	links := ethTxTrie.Links()

	for i, v := range desiredValues {
		if links[i].Cid.String() != v {
			t.Fatalf("Wrong cid for link %d\r\nexpected %s\r\ngot %s", i, v, links[i].Cid.String())
		}
	}
}

/*
  EthTxTrie Functions
*/

func TestTxTrieJSONMarshalExtension(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieExtension(t)

	jsonOutput, err := ethTxTrie.MarshalJSON()
	checkError(err, t)

	var data map[string]interface{}
	err = json.Unmarshal(jsonOutput, &data)
	checkError(err, t)

	if parseMapElement(data["01"]) !=
		"bagjacgzak6wdjvshdtb7lrvlteweyd7f5qjr3dmzmh7g2xpi4xrwoujsio2a" {
		t.Fatalf("Wrong Marshaled Value\r\nexpected %s\r\ngot %s", "bagjacgzak6wdjvshdtb7lrvlteweyd7f5qjr3dmzmh7g2xpi4xrwoujsio2a", parseMapElement(data["01"]))
	}

	if data["type"] != "extension" {
		t.Fatalf("Wrong node type\r\nexpected %s\r\ngot %s", "extension", data["type"])
	}
}

func TestTxTrieJSONMarshalLeaf(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieLeaf(t)

	jsonOutput, err := ethTxTrie.MarshalJSON()
	checkError(err, t)

	var data map[string]interface{}
	err = json.Unmarshal(jsonOutput, &data)
	checkError(err, t)

	if data["type"] != "leaf" {
		t.Fatalf("Wrong node type\r\nexpected %s\r\ngot %s", "leaf", data["type"])
	}

	if fmt.Sprintf("%v", data[""].(map[string]interface{})["nonce"]) !=
		"40243" {
		t.Fatalf("Wrong nonce value\r\nexepcted %s\r\ngot %s", "40243", fmt.Sprintf("%v", data[""].(map[string]interface{})["nonce"]))
	}
}

func TestTxTrieJSONMarshalBranch(t *testing.T) {
	ethTxTrie := prepareDecodedEthTxTrieBranch(t)

	jsonOutput, err := ethTxTrie.MarshalJSON()
	checkError(err, t)

	var data map[string]interface{}
	err = json.Unmarshal(jsonOutput, &data)
	checkError(err, t)

	desiredValues := map[string]string{
		"0": "bagjacgzakhtcfpja453ydiaqxgidqmxhh7jwmxujib663deebwfs3m2n3hoa",
		"1": "bagjacgza2p2fuqh4vumknq6x5w7i47usvtu5ixqins6qjjtcks4zge3vx3qq",
		"2": "bagjacgza4fkhn7et3ra66yjkzbtvbxjefuketda6jctlut6it7gfahxhywga",
		"3": "bagjacgzacnryeybs52xryrka5uxi4eg4hi2mh66esaghu7cetzu6fsukrynq",
		"4": "bagjacgzastu5tc7lwz4ap3gznjwkyyepswquub7gvhags5mgdyfynnwbi43a",
		"5": "bagjacgza5qgp76ovvorkydni2lchew6ieu5wb55w6hdliiu6vft7zlxtdhjq",
		"6": "bagjacgzafnssc4yvln6zxmks5roskw4ckngta5n4yfy2skhlu435ve4b575a",
		"7": "bagjacgzagkuei7qxfxefufme2d3xizxokkq4ad3rzl2x4dq2uao6dcr4va2a",
		"8": "bagjacgzaxpaehtananrdxjghwukh2wwkkzcqwveppf6xclkrtd26rm27kqwq",
	}

	for k, v := range desiredValues {
		if parseMapElement(data[k]) != v {
			t.Fatalf("Wrong Marshaled Value %s\r\nexpected %s\r\ngot %s", k, v, parseMapElement(data[k]))
		}
	}

	for _, v := range []string{"a", "b", "c", "d", "e", "f"} {
		if data[v] != nil {
			t.Fatal("Expected value to be nil")
		}
	}

	if data["type"] != "branch" {
		t.Fatalf("Wrong node type\r\nexpected %s\r\ngot %s", "branch", data["type"])
	}
}

/*
  AUXILIARS
*/

// prepareDecodedEthTxTrie simulates an IPLD block available in the datastore,
// checks the source RLP and tests for the absence of errors during the decoding fase.
func prepareDecodedEthTxTrie(branchDataRLP string, t *testing.T) *EthTxTrie {
	b, err := hex.DecodeString(branchDataRLP)
	checkError(err, t)

	c, err := RawdataToCid(MEthTxTrie, b, multihash.KECCAK_256)
	checkError(err, t)

	storedEthTxTrie, err := block.NewBlockWithCid(b, c)
	checkError(err, t)

	ethTxTrie, err := DecodeEthTxTrie(storedEthTxTrie.Cid(), storedEthTxTrie.RawData())
	checkError(err, t)

	return ethTxTrie
}

func prepareDecodedEthTxTrieExtension(t *testing.T) *EthTxTrie {
	extensionDataRLP :=
		"e4820001a057ac34d6471cc3f5c6ab992c4c0fe5ec131d8d9961fe6d5de8e5e367513243b4"
	return prepareDecodedEthTxTrie(extensionDataRLP, t)
}

func prepareDecodedEthTxTrieLeaf(t *testing.T) *EthTxTrie {
	leafDataRLP :=
		"f87220b86ff86d829d3384ee6b280083015f9094e0e6c781b8cba08bc840" +
			"7eac0101b668d1fa6f4987c495a9586034008026a0981b6223c9d3c31971" +
			"6da3cf057da84acf0fef897f4003d8a362d7bda42247dba066be134c4bc4" +
			"32125209b5056ef274b7423bcac7cc398cf60b83aaff7b95469f"
	return prepareDecodedEthTxTrie(leafDataRLP, t)
}

func prepareDecodedEthTxTrieBranch(t *testing.T) *EthTxTrie {
	branchDataRLP :=
		"f90131a051e622bd20e77781a010b9903832e73fd3665e89407ded8c840d8b2db34dd9" +
			"dca0d3f45a40fcad18a6c3d7edbe8e7e92ace9d45e086cbd04a66254b9931375bee1a0" +
			"e15476fc93dc41ef612ac86750dd242d14498c1e48a6ba4fc89fcc501ee7c58ca01363" +
			"826032eeaf1c4540ed2e8e10dc3a34c3fbc4900c7a7c449e69e2ca8a8e1ba094e9d98b" +
			"ebb67807ecd96a6cac608f95a14a07e6a9c06975861e0b86b6c14736a0ec0cfff9d5ab" +
			"a2ac0da8d2c4725bc8253b60f7b6f1c6b4229ea967fcaef319d3a02b652173155b7d9b" +
			"b152ec5d255b82534d3075bcc171a928eba737da9381effaa032a8447e172dc85a1584" +
			"d0f77466ee52a1c00f71caf57e0e1aa01de18a3ca834a0bbc043cc0d03623ba4c7b514" +
			"7d5aca56450b548f797d712d5198f5e8b35f542d8080808080808080"
	return prepareDecodedEthTxTrie(branchDataRLP, t)
}

func prepareTxTrieMap(t *testing.T) map[string]*EthTxTrie {
	fi, err := os.Open("test_data/eth-block-body-json-4139497")
	checkError(err, t)

	_, _, txTrieNodes, err := FromBlockJSON(fi)
	checkError(err, t)

	out := make(map[string]*EthTxTrie)

	for _, txTrieNode := range txTrieNodes {
		decodedNode, err := DecodeEthTxTrie(txTrieNode.Cid(), txTrieNode.RawData())
		checkError(err, t)
		out[txTrieNode.Cid().String()] = decodedNode
	}

	return out
}
