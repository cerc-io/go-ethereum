package ipld

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
)

/*
  Block INTERFACE
*/
func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
}

func TestAccountSnapshotBlockElements(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	if fmt.Sprintf("%x", eas.RawData())[:10] != "f84e808a03" {
		t.Fatal("Wrong Data")
	}

	if eas.Cid().String() !=
		"baglqcgzasckx2alxk43cksshnztjvhfyvbbh6bkp376gtcndm5cg4fkrkhsa" {
		t.Fatal("Wrong Cid")
	}
}

func TestAccountSnapshotString(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	if eas.String() !=
		"<EthereumAccountSnapshot baglqcgzasckx2alxk43cksshnztjvhfyvbbh6bkp376gtcndm5cg4fkrkhsa>" {
		t.Fatalf("Wrong String()")
	}
}

func TestAccountSnapshotLoggable(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	l := eas.Loggable()
	if _, ok := l["type"]; !ok {
		t.Fatal("Loggable map expected the field 'type'")
	}

	if l["type"] != "eth-account-snapshot" {
		t.Fatalf("Wrong Loggable 'type' value\r\nexpected %s\r\ngot %s", "eth-account-snapshot", l["type"])
	}
}

/*
  Node INTERFACE
*/
func TestAccountSnapshotResolve(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	// Empty path
	obj, rest, err := eas.Resolve([]string{})
	reas, ok := obj.(*EthAccountSnapshot)
	if !ok {
		t.Fatalf("Wrong type of returned object\r\nexpected %T\r\ngot %T", &EthAccountSnapshot{}, reas)
	}
	if reas.Cid() != eas.Cid() {
		t.Fatalf("wrong returned CID\r\nexpected %s\r\ngot %s", eas.Cid().String(), reas.Cid().String())
	}
	if rest != nil {
		t.Fatal("rest should be nil")
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
		obj, rest, err = eas.Resolve(bc)
		if obj != nil {
			t.Fatal("obj should be nil")
		}
		if rest != nil {
			t.Fatal("rest should be nil")
		}
		if err.Error() != fmt.Sprintf("unexpected path elements past %s", bc[0]) {
			t.Fatal("wrong error")
		}
	}

	moreBadCases := []string{
		"i",
		"am",
		"not",
		"an",
		"account",
		"field",
	}
	for _, mbc := range moreBadCases {
		obj, rest, err = eas.Resolve([]string{mbc})
		if obj != nil {
			t.Fatal("obj should be nil")
		}
		if rest != nil {
			t.Fatal("rest should be nil")
		}
		if err != ErrInvalidLink {
			t.Fatal("wrong error")
		}
	}

	goodCases := []string{
		"balance",
		"codeHash",
		"nonce",
		"root",
	}
	for _, gc := range goodCases {
		_, _, err = eas.Resolve([]string{gc})
		if err != nil {
			t.Fatalf("error should be nil %v", gc)
		}
	}

}

func TestAccountSnapshotTree(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	// Bad cases
	tree := eas.Tree("non-empty-string", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = eas.Tree("non-empty-string", 1)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	tree = eas.Tree("", 0)
	if tree != nil {
		t.Fatal("Expected nil to be returned")
	}

	// Good cases
	tree = eas.Tree("", 1)
	lookupElements := map[string]interface{}{
		"balance":  nil,
		"codeHash": nil,
		"nonce":    nil,
		"root":     nil,
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

func TestAccountSnapshotResolveLink(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	// bad case
	obj, rest, err := eas.ResolveLink([]string{"supercalifragilist"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err != ErrInvalidLink {
		t.Fatal("Wrong error")
	}

	// good case
	obj, rest, err = eas.ResolveLink([]string{"nonce"})
	if obj != nil {
		t.Fatalf("Expected obj to be nil")
	}
	if rest != nil {
		t.Fatal("Expected rest to be nil")
	}
	if err.Error() != "resolved item was not a link" {
		t.Fatal("Wrong error")
	}
}

func TestAccountSnapshotCopy(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Expected panic")
		}
		if r != "implement me" {
			t.Fatalf("Wrong panic message\r\n expected %s\r\ngot %s", "'implement me'", r)
		}
	}()

	_ = eas.Copy()
}

func TestAccountSnapshotLinks(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	if eas.Links() != nil {
		t.Fatal("Links() expected to return nil")
	}
}

func TestAccountSnapshotStat(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	obj, err := eas.Stat()
	if obj == nil {
		t.Fatal("Expected a not null object node.NodeStat")
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

func TestAccountSnapshotSize(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	size, err := eas.Size()
	if size != uint64(0) {
		t.Fatalf("Wrong size\r\nexpected %d\r\ngot %d", 0, size)
	}

	if err != nil {
		t.Fatal("Expected a nil error")
	}
}

/*
  EthAccountSnapshot functions
*/

func TestAccountSnapshotMarshalJSON(t *testing.T) {
	eas := prepareEthAccountSnapshot(t)

	jsonOutput, err := eas.MarshalJSON()
	checkError(err, t)

	var data map[string]interface{}
	err = json.Unmarshal(jsonOutput, &data)
	checkError(err, t)

	balanceExpression := regexp.MustCompile(`{"balance":16011846000000000000000,`)
	if !balanceExpression.MatchString(string(jsonOutput)) {
		t.Fatal("Balance expression not found")
	}

	code, _ := data["codeHash"].(map[string]interface{})
	if fmt.Sprintf("%s", code["/"]) !=
		"bafkrwigf2jdadbxxem6je7t5wlomoa6a4ualmu6kqittw6723acf3bneoa" {
		t.Fatalf("Wrong Marshaled Value\r\nexpected %s\r\ngot %s", "bafkrwigf2jdadbxxem6je7t5wlomoa6a4ualmu6kqittw6723acf3bneoa", fmt.Sprintf("%s", code["/"]))
	}

	if fmt.Sprintf("%v", data["nonce"]) != "0" {
		t.Fatalf("Wrong Marshaled Value\r\nexpected %s\r\ngot %s", "0", fmt.Sprintf("%v", data["nonce"]))
	}

	root, _ := data["root"].(map[string]interface{})
	if fmt.Sprintf("%s", root["/"]) !=
		"bagmacgzak3ub6fy3zrk2n74dixtjfqhynznurya3tfwk3qabmix3ly3dwqqq" {
		t.Fatalf("Wrong Marshaled Value\r\nexpected %s\r\ngot %s", "bagmacgzak3ub6fy3zrk2n74dixtjfqhynznurya3tfwk3qabmix3ly3dwqqq", fmt.Sprintf("%s", root["/"]))
	}
}

/*
  AUXILIARS
*/
func prepareEthAccountSnapshot(t *testing.T) *EthAccountSnapshot {
	fi, err := os.Open("test_data/eth-state-trie-rlp-c9070d")
	checkError(err, t)

	output, err := FromStateTrieRLPFile(fi)
	checkError(err, t)

	return output.elements[1].(*EthAccountSnapshot)
}
