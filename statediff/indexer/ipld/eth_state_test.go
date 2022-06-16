package ipld

import (
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
)

/*
  INPUT
  OUTPUT
*/

func TestStateTrieNodeEvenExtensionParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-eb2f5f")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "extension" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "extension", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	if fmt.Sprintf("%x", output.elements[0]) != "0d08" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "0d08", fmt.Sprintf("%x", output.elements[0]))
	}

	if output.elements[1].(cid.Cid).String() !=
		"baglacgzalnzmhhnxudxtga6t3do2rctb6ycgyj6mjnycoamlnc733nnbkd6q" {
		t.Fatalf("Wrong CID\r\nexpected %s\r\ngot %s", "baglacgzalnzmhhnxudxtga6t3do2rctb6ycgyj6mjnycoamlnc733nnbkd6q", output.elements[1].(cid.Cid).String())
	}
}

func TestStateTrieNodeOddExtensionParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-56864f")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "extension" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "extension", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	if fmt.Sprintf("%x", output.elements[0]) != "02" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "02", fmt.Sprintf("%x", output.elements[0]))
	}

	if output.elements[1].(cid.Cid).String() !=
		"baglacgzaizf2czb7wztoox4lu23qkwkbfamqsdzcmejzr3rsszrvkaktpfeq" {
		t.Fatalf("Wrong CID\r\nexpected %s\r\ngot %s", "baglacgzaizf2czb7wztoox4lu23qkwkbfamqsdzcmejzr3rsszrvkaktpfeq", output.elements[1].(cid.Cid).String())
	}
}

func TestStateTrieNodeEvenLeafParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-0e8b34")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "leaf" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "leaf", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	// bd66f60e5b954e1af93ded1b02cb575ff0ed6d9241797eff7576b0bf0637
	if fmt.Sprintf("%x", output.elements[0].([]byte)[0:10]) != "0b0d06060f06000e050b" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "0b0d06060f06000e050b", fmt.Sprintf("%x", output.elements[0].([]byte)[0:10]))
	}

	if output.elements[1].(*EthAccountSnapshot).String() !=
		"<EthereumAccountSnapshot baglqcgzaf5tapdf2fwb6mo4ijtovqpoi4n3f4jv2yx6avvz6sjypp6vytfva>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumAccountSnapshot baglqcgzaf5tapdf2fwb6mo4ijtovqpoi4n3f4jv2yx6avvz6sjypp6vytfva>", output.elements[1].(*EthAccountSnapshot).String())
	}
}

func TestStateTrieNodeOddLeafParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-c9070d")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "leaf" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "leaf", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	// 6c9db9bb545a03425e300f3ee72bae098110336dd3eaf48c20a2e5b6865fc
	if fmt.Sprintf("%x", output.elements[0].([]byte)[0:10]) != "060c090d0b090b0b0504" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "060c090d0b090b0b0504", fmt.Sprintf("%x", output.elements[0].([]byte)[0:10]))
	}

	if output.elements[1].(*EthAccountSnapshot).String() !=
		"<EthereumAccountSnapshot baglqcgzasckx2alxk43cksshnztjvhfyvbbh6bkp376gtcndm5cg4fkrkhsa>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumAccountSnapshot baglqcgzasckx2alxk43cksshnztjvhfyvbbh6bkp376gtcndm5cg4fkrkhsa>", output.elements[1].(*EthAccountSnapshot).String())
	}
}

/*
  Block INTERFACE
*/
func TestStateTrieBlockElements(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-d7f897")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if fmt.Sprintf("%x", output.RawData())[:10] != "f90211a090" {
		t.Fatalf("Wrong Data\r\nexpected %s\r\ngot %s", "f90211a090", fmt.Sprintf("%x", output.RawData())[:10])
	}

	if output.Cid().String() !=
		"baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca", output.Cid().String())
	}
}

func TestStateTrieString(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-d7f897")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.String() !=
		"<EthereumStateTrie baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumStateTrie baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca>", output.String())
	}
}

func TestStateTrieLoggable(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-d7f897")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	l := output.Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-state-trie" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-state-trie", l["type"])
	}
}

/*
  TRIE NODE (Through EthStateTrie)
  Node INTERFACE
*/

func TestTraverseStateTrieWithResolve(t *testing.T) {
	var err error

	stMap := prepareStateTrieMap(t)

	// This is the cid of the root of the block 0
	// baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca
	currentNode := stMap["baglacgza274jot5vvr4ntlajtonnkaml5xbm4cts3liye6qxbhndawapavca"]

	// This is the path we want to traverse
	// The eth address is 0x5abfec25f74cd88437631a7731906932776356f9
	// Its keccak-256 is cdd3e25edec0a536a05f5e5ab90a5603624c0ed77453b2e8f955cf8b43d4d0fb
	// We use the keccak-256(addr) to traverse the state trie in ethereum.
	var traversePath []string
	for _, s := range "cdd3e25edec0a536a05f5e5ab90a5603624c0ed77453b2e8f955cf8b43d4d0fb" {
		traversePath = append(traversePath, string(s))
	}
	traversePath = append(traversePath, "balance")

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

		currentNode = stMap[link.Cid.String()]
		if currentNode == nil {
			t.Fatal("state trie node not found in memory map")
		}
	}

	if fmt.Sprintf("%v", obj) != "11901484239480000000000000" {
		t.Fatalf("Wrong balance value\r\nexpected %s\r\ngot %s", "11901484239480000000000000", fmt.Sprintf("%v", obj))
	}
}

func TestStateTrieResolveLinks(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-eb2f5f")
	checkError(err, t)

	stNode, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	// bad case
	obj, rest, err := stNode.ResolveLink([]string{"supercalifragilist"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err.Error() != "invalid path element" {
		t.Fatalf("Wrong error\r\nexpected %s\r\ngot %s", "invalid path element", err.Error())
	}

	// good case
	obj, rest, err = stNode.ResolveLink([]string{"d8"})
	if obj == nil {
		t.Fatalf("Expected a not nil obj to be returned")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err != nil {
		t.Fatal("Expected error to be nil")
	}
}

func TestStateTrieCopy(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-eb2f5f")
	checkError(err, t)

	stNode, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Expected panic")
		}
		if r != "implement me" {
			t.Fatalf("Wrong panic message\r\nexpected %s\r\ngot %s", "'implement me'", r)
		}
	}()

	_ = stNode.Copy()
}

func TestStateTrieStat(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-eb2f5f")
	checkError(err, t)

	stNode, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	obj, err := stNode.Stat()
	if obj == nil {
		t.Fatal("Expected a not null object node.NodeStat")
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

func TestStateTrieSize(t *testing.T) {
	fi, err := os.Open("test_data/eth-state-trie-rlp-eb2f5f")
	checkError(err, t)

	stNode, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	size, err := stNode.Size()
	if size != uint64(0) {
		t.Fatalf("Wrong size\r\nexpected %d\r\ngot %d", 0, size)
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

func prepareStateTrieMap(t *testing.T) map[string]*EthStateTrie {
	filepaths := []string{
		"test_data/eth-state-trie-rlp-0e8b34",
		"test_data/eth-state-trie-rlp-56864f",
		"test_data/eth-state-trie-rlp-6fc2d7",
		"test_data/eth-state-trie-rlp-727994",
		"test_data/eth-state-trie-rlp-c9070d",
		"test_data/eth-state-trie-rlp-d5be90",
		"test_data/eth-state-trie-rlp-d7f897",
		"test_data/eth-state-trie-rlp-eb2f5f",
	}

	out := make(map[string]*EthStateTrie)

	for _, fp := range filepaths {
		fi, err := os.Open(fp)
		checkError(err, t)

		stateTrieNode, err := FromStateTrieRLPFile(fi)
		checkError(err, t)

		out[stateTrieNode.Cid().String()] = stateTrieNode
	}

	return out
}
