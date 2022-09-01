package ipld

import (
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
)

/*
  INPUT
  OUTPUT
*/

func TestStorageTrieNodeExtensionParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-113049")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "extension" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "extension", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an extension node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	if fmt.Sprintf("%x", output.elements[0]) != "0a" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "0a", fmt.Sprintf("%x", output.elements[0]))
	}

	if output.elements[1].(cid.Cid).String() !=
		"baglacgzautxeutufae7owyrezfvwpan2vusocmxgzwqhzrhjbwprp2texgsq" {
		t.Fatalf("Wrong CID\r\nexpected %s\r\ngot %s", "baglacgzautxeutufae7owyrezfvwpan2vusocmxgzwqhzrhjbwprp2texgsq", output.elements[1].(cid.Cid).String())
	}
}

func TestStateTrieNodeLeafParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-ffbcad")
	checkError(err, t)

	output, err := FromStorageTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "leaf" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "leaf", output.nodeKind)
	}

	if len(output.elements) != 2 {
		t.Fatalf("Wrong number of elements for an leaf node\r\nexpected %d\r\ngot %d", 2, len(output.elements))
	}

	// 2ee1ae9c502e48e0ed528b7b39ac569cef69d7844b5606841a7f3fe898a2
	if fmt.Sprintf("%x", output.elements[0].([]byte)[:10]) != "020e0e010a0e090c0500" {
		t.Fatalf("Wrong key\r\nexpected %s\r\ngot %s", "020e0e010a0e090c0500", fmt.Sprintf("%x", output.elements[0].([]byte)[:10]))
	}

	if fmt.Sprintf("%x", output.elements[1]) != "89056c31f304b2530000" {
		t.Fatalf("Wrong Value\r\nexpected %s\r\ngot %s", "89056c31f304b2530000", fmt.Sprintf("%x", output.elements[1]))
	}
}

func TestStateTrieNodeBranchParsing(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-ffc25c")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	if output.nodeKind != "branch" {
		t.Fatalf("Wrong nodeKind\r\nexpected %s\r\ngot %s", "branch", output.nodeKind)
	}

	if len(output.elements) != 17 {
		t.Fatalf("Wrong number of elements for an branch node\r\nexpected %d\r\ngot %d", 17, len(output.elements))
	}

	if fmt.Sprintf("%s", output.elements[4]) !=
		"baglacgzadqhbmlxrxtw5hplcq5jn74p4dceryzw664w3237ra52dnghbjpva" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "baglacgzadqhbmlxrxtw5hplcq5jn74p4dceryzw664w3237ra52dnghbjpva", fmt.Sprintf("%s", output.elements[4]))
	}

	if fmt.Sprintf("%s", output.elements[10]) !=
		"baglacgza77d37i2v6uhtzeeq4vngragjbgbwq3lylpoc3lihenvzimybzxmq" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "baglacgza77d37i2v6uhtzeeq4vngragjbgbwq3lylpoc3lihenvzimybzxmq", fmt.Sprintf("%s", output.elements[10]))
	}
}

/*
  Block INTERFACE
*/
func TestStorageTrieBlockElements(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-ffbcad")
	checkError(err, t)

	output, err := FromStorageTrieRLPFile(fi)
	checkError(err, t)

	if fmt.Sprintf("%x", output.RawData())[:10] != "eb9f202ee1" {
		t.Fatalf("Wrong Data\r\nexpected %s\r\ngot %s", "eb9f202ee1", fmt.Sprintf("%x", output.RawData())[:10])
	}

	if output.Cid().String() !=
		"bagmacgza766k3oprj2qxn36eycw55pogmu3dwtfay6zdh6ajrhvw3b2nqg5a" {
		t.Fatalf("Wrong Cid\r\nexpected %s\r\ngot %s", "bagmacgza766k3oprj2qxn36eycw55pogmu3dwtfay6zdh6ajrhvw3b2nqg5a", output.Cid().String())
	}
}

func TestStorageTrieString(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-ffbcad")
	checkError(err, t)

	output, err := FromStorageTrieRLPFile(fi)
	checkError(err, t)

	if output.String() !=
		"<EthereumStorageTrie bagmacgza766k3oprj2qxn36eycw55pogmu3dwtfay6zdh6ajrhvw3b2nqg5a>" {
		t.Fatalf("Wrong String()\r\nexpected %s\r\ngot %s", "<EthereumStorageTrie bagmacgza766k3oprj2qxn36eycw55pogmu3dwtfay6zdh6ajrhvw3b2nqg5a>", output.String())
	}
}

func TestStorageTrieLoggable(t *testing.T) {
	fi, err := os.Open("test_data/eth-storage-trie-rlp-ffbcad")
	checkError(err, t)

	output, err := FromStorageTrieRLPFile(fi)
	checkError(err, t)

	l := output.Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-storage-trie" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-storage-trie", l["type"])
	}
}
