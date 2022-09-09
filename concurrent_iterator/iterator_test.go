package iterator_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"

	iter "github.com/vulcanize/go-eth-state-node-iterator"
	fixt "github.com/vulcanize/go-eth-state-node-iterator/fixture"
)

func TestMakePaths(t *testing.T) {
	var prefix []byte
	for i := 0; i < 4; i++ {
		nbins := uint(1) << i
		paths := iter.MakePaths(prefix, nbins)
		if len(paths) != int(nbins) {
			t.Errorf("wrong number of paths; expected %d, have %d", nbins, len(paths))
		}
	}
}

func TestIterator(t *testing.T) {
	edb, err := rawdb.NewLevelDBDatabaseWithFreezer(
		fixt.ChainDataPath, 1024, 256, fixt.AncientDataPath,
		"eth-pg-ipfs-state-snapshot", false,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer edb.Close()

	height := uint64(1)
	hash := rawdb.ReadCanonicalHash(edb, height)
	header := rawdb.ReadHeader(edb, hash, height)
	if header == nil {
		t.Fatalf("unable to read canonical header at height %d", height)
	}
	sdb := state.NewDatabase(edb)
	tree, err := sdb.OpenTrie(header.Root)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("in bounds", func(t *testing.T) {
		type testCase struct {
			lower, upper []byte
		}
		cases := []testCase{
			{nil, []byte{0, 0}},
			{[]byte{1, 0}, []byte{2, 0}},
			{[]byte{3, 5}, []byte{4, 2, 0}},
			{[]byte{5, 6, 9, 0}, []byte{7, 0}},
			{[]byte{8, 0}, []byte{8, 0}},
			{[]byte{8, 0}, []byte{7, 0}},
		}

		runCase := func(t *testing.T, tc testCase) {
			it := iter.NewPrefixBoundIterator(tree.NodeIterator(iter.HexToKeyBytes(tc.lower)), tc.lower, tc.upper)
			for it.Next(true) {
				if bytes.Compare(it.Path(), tc.lower) < 0 {
					t.Fatalf("iterator outside lower bound: %v", it.Path())
				}
				if bytes.Compare(tc.upper, it.Path()) < 0 {
					t.Fatalf("iterator outside upper bound: %v <= %v", tc.upper, it.Path())
				}
			}
		}
		for _, tc := range cases {
			t.Run("case", func(t *testing.T) { runCase(t, tc) })
		}
	})

	t.Run("trie is covered", func(t *testing.T) {
		allPaths := fixt.Block1_Paths
		cases := []uint{1, 2, 4, 8, 16, 32}
		runCase := func(t *testing.T, nbins uint) {
			iters := iter.SubtrieIterators(tree, nbins)
			ix := 0
			for b := uint(0); b < nbins; b++ {
				for it := iters[b]; it.Next(true); ix++ {
					if !bytes.Equal(allPaths[ix], it.Path()) {
						t.Fatalf("wrong path value\nexpected:\t%v\nactual:\t\t%v",
							allPaths[ix], it.Path())
					}
				}
				// if the last node path was even-length, it will be duplicated
				if len(allPaths[ix-1])&0b1 == 0 {
					ix--
				}
			}
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%d bins", tc), func(t *testing.T) { runCase(t, tc) })
		}
	})
}
