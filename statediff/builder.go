// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package statediff

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	metrics2 "github.com/ethereum/go-ethereum/statediff/indexer/database/metrics"
	ipld2 "github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/ethereum/go-ethereum/statediff/trie_helpers"
	types2 "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	emptyNode, _      = rlp.EncodeToBytes(&[]byte{})
	emptyContractRoot = crypto.Keccak256Hash(emptyNode)
	nullCodeHash      = crypto.Keccak256Hash([]byte{}).Bytes()
	nullNodeHash      = common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")
)

// Builder interface exposes the method for building a state diff between two blocks
type Builder interface {
	BuildStateDiffObject(args Args, params Params) (types2.StateObject, error)
	WriteStateDiffObject(args Args, params Params, output types2.StateNodeSink, ipldOutput types2.IPLDSink) error
}

type StateDiffBuilder struct {
	StateCache state.Database
}

type IterPair struct {
	Older, Newer trie.NodeIterator
}

func StateNodeAppender(nodes *[]types2.StateLeafNode) types2.StateNodeSink {
	return func(node types2.StateLeafNode) error {
		*nodes = append(*nodes, node)
		return nil
	}
}
func StorageNodeAppender(nodes *[]types2.StorageLeafNode) types2.StorageNodeSink {
	return func(node types2.StorageLeafNode) error {
		*nodes = append(*nodes, node)
		return nil
	}
}
func IPLDMappingAppender(iplds *[]types2.IPLD) types2.IPLDSink {
	return func(c types2.IPLD) error {
		*iplds = append(*iplds, c)
		return nil
	}
}

// NewBuilder is used to create a statediff builder
func NewBuilder(stateCache state.Database) Builder {
	return &StateDiffBuilder{
		StateCache: stateCache, // state cache is safe for concurrent reads
	}
}

// BuildStateDiffObject builds a statediff object from two blocks and the provided parameters
func (sdb *StateDiffBuilder) BuildStateDiffObject(args Args, params Params) (types2.StateObject, error) {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildStateDiffObjectTimer)
	var stateNodes []types2.StateLeafNode
	var iplds []types2.IPLD
	err := sdb.WriteStateDiffObject(args, params, StateNodeAppender(&stateNodes), IPLDMappingAppender(&iplds))
	if err != nil {
		return types2.StateObject{}, err
	}
	return types2.StateObject{
		BlockHash:   args.BlockHash,
		BlockNumber: args.BlockNumber,
		Nodes:       stateNodes,
		IPLDs:       iplds,
	}, nil
}

// WriteStateDiffObject writes a statediff object to output sinks
func (sdb *StateDiffBuilder) WriteStateDiffObject(args Args, params Params, output types2.StateNodeSink,
	ipldOutput types2.IPLDSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.WriteStateDiffObjectTimer)
	// Load tries for old and new states
	oldTrie, err := sdb.StateCache.OpenTrie(args.OldStateRoot)
	if err != nil {
		return fmt.Errorf("error creating trie for oldStateRoot: %v", err)
	}
	newTrie, err := sdb.StateCache.OpenTrie(args.NewStateRoot)
	if err != nil {
		return fmt.Errorf("error creating trie for newStateRoot: %v", err)
	}

	// we do two state trie iterations:
	// 		one for new/updated nodes,
	// 		one for deleted/updated nodes;
	// prepare 2 iterator instances for each task
	iterPairs := []IterPair{
		{
			Older: oldTrie.NodeIterator([]byte{}),
			Newer: newTrie.NodeIterator([]byte{}),
		},
		{
			Older: oldTrie.NodeIterator([]byte{}),
			Newer: newTrie.NodeIterator([]byte{}),
		},
	}

	logger := log.New("hash", args.BlockHash.Hex(), "number", args.BlockNumber)
	return sdb.BuildStateDiffWithIntermediateStateNodes(iterPairs, params, output, ipldOutput, logger, nil)
}

func (sdb *StateDiffBuilder) BuildStateDiffWithIntermediateStateNodes(iterPairs []IterPair, params Params,
	output types2.StateNodeSink, ipldOutput types2.IPLDSink, logger log.Logger, prefixPath []byte) error {
	logger.Debug("statediff BEGIN BuildStateDiffWithIntermediateStateNodes")
	defer metrics2.ReportAndUpdateDuration("statediff END BuildStateDiffWithIntermediateStateNodes", time.Now(), logger, metrics2.IndexerMetrics.BuildStateDiffWithIntermediateStateNodesTimer)
	// collect a slice of all the nodes that were touched and exist at B (B-A)
	// a map of their leafkey to all the accounts that were touched and exist at B
	// and a slice of all the paths for the nodes in both of the above sets
	diffAccountsAtB, err := sdb.createdAndUpdatedState(
		iterPairs[0].Older, iterPairs[0].Newer, params.watchedAddressesLeafPaths, ipldOutput, logger, prefixPath)
	if err != nil {
		return fmt.Errorf("error collecting createdAndUpdatedNodes: %v", err)
	}

	// collect a slice of all the nodes that existed at a path in A that doesn't exist in B
	// a map of their leafkey to all the accounts that were touched and exist at A
	diffAccountsAtA, err := sdb.deletedOrUpdatedState(
		iterPairs[1].Older, iterPairs[1].Newer, diffAccountsAtB,
		params.watchedAddressesLeafPaths, output, logger, prefixPath)
	if err != nil {
		return fmt.Errorf("error collecting deletedOrUpdatedNodes: %v", err)
	}

	// collect and sort the leafkey keys for both account mappings into a slice
	t := time.Now()
	createKeys := trie_helpers.SortKeys(diffAccountsAtB)
	deleteKeys := trie_helpers.SortKeys(diffAccountsAtA)
	logger.Debug(fmt.Sprintf("statediff BuildStateDiffWithIntermediateStateNodes sort duration=%dms", time.Since(t).Milliseconds()))

	// and then find the intersection of these keys
	// these are the leafkeys for the accounts which exist at both A and B but are different
	// this also mutates the passed in createKeys and deleteKeys, removing the intersection keys
	// and leaving the truly created or deleted keys in place
	t = time.Now()
	updatedKeys := trie_helpers.FindIntersection(createKeys, deleteKeys)
	logger.Debug(fmt.Sprintf("statediff BuildStateDiffWithIntermediateStateNodes intersection count=%d duration=%dms",
		len(updatedKeys),
		time.Since(t).Milliseconds()))

	// build the diff nodes for the updated accounts using the mappings at both A and B as directed by the keys found as the intersection of the two
	err = sdb.buildAccountUpdates(diffAccountsAtB, diffAccountsAtA, updatedKeys, output, ipldOutput, logger)
	if err != nil {
		return fmt.Errorf("error building diff for updated accounts: %v", err)
	}
	// build the diff nodes for created accounts
	err = sdb.buildAccountCreations(diffAccountsAtB, output, ipldOutput, logger)
	if err != nil {
		return fmt.Errorf("error building diff for created accounts: %v", err)
	}
	return nil
}

// createdAndUpdatedState returns
// a slice of all the intermediate nodes that exist in a different state at B than A
// a mapping of their leafkeys to all the accounts that exist in a different state at B than A
// and a slice of the paths for all of the nodes included in both
func (sdb *StateDiffBuilder) createdAndUpdatedState(a, b trie.NodeIterator,
	watchedAddressesLeafPaths [][]byte, output types2.IPLDSink, logger log.Logger, prefixPath []byte) (types2.AccountMap, error) {
	logger.Debug("statediff BEGIN createdAndUpdatedState")
	defer metrics2.ReportAndUpdateDuration("statediff END createdAndUpdatedState", time.Now(), logger, metrics2.IndexerMetrics.CreatedAndUpdatedStateTimer)
	diffAccountsAtB := make(types2.AccountMap)
	watchingAddresses := len(watchedAddressesLeafPaths) > 0

	it, itCount := trie.NewDifferenceIterator(a, b)
	for it.Next(true) {
		// ignore node if it is not along paths of interest
		if watchingAddresses && !isValidPrefixPath(watchedAddressesLeafPaths, append(prefixPath, it.Path()...)) {
			continue
		}
		// index values by leaf key
		if it.Leaf() {
			// if it is a "value" node, we will index the value by leaf key
			accountW, err := sdb.processStateValueNode(it, watchedAddressesLeafPaths, prefixPath)
			if err != nil {
				return nil, err
			}
			if accountW == nil {
				continue
			}
			// for now, just add it to diffAccountsAtB
			// we will compare to diffAccountsAtA to determine which diffAccountsAtB
			// were creations and which were updates and also identify accounts that were removed going A->B
			diffAccountsAtB[common.Bytes2Hex(accountW.LeafKey)] = *accountW
		} else { // trie nodes will be written to blockstore only
			// reminder that this includes leaf nodes, since the geth iterator.Leaf() actually signifies a "value" node
			if bytes.Equal(it.Hash().Bytes(), nullNodeHash) {
				continue
			}
			nodeVal := make([]byte, len(it.NodeBlob()))
			copy(nodeVal, it.NodeBlob())
			if len(watchedAddressesLeafPaths) > 0 {
				var elements []interface{}
				if err := rlp.DecodeBytes(nodeVal, &elements); err != nil {
					return nil, err
				}
				ok, err := isLeaf(elements)
				if err != nil {
					return nil, err
				}
				if ok {
					nodePath := append(prefixPath, it.Path()...)
					partialPath := trie.CompactToHex(elements[0].([]byte))
					valueNodePath := append(nodePath, partialPath...)
					if !isWatchedAddress(watchedAddressesLeafPaths, valueNodePath) {
						continue
					}
				}
			}
			nodeHash := make([]byte, len(it.Hash().Bytes()))
			copy(nodeHash, it.Hash().Bytes())
			if err := output(types2.IPLD{
				CID:     ipld2.Keccak256ToCid(ipld2.MEthStateTrie, nodeHash).String(),
				Content: nodeVal,
			}); err != nil {
				return nil, err
			}
		}
	}
	logger.Debug("statediff COUNTS createdAndUpdatedStateWithIntermediateNodes", "it", itCount, "diffAccountsAtB", len(diffAccountsAtB))
	metrics2.IndexerMetrics.DifferenceIteratorCounter.Inc(int64(*itCount))
	return diffAccountsAtB, it.Error()
}

// reminder: it.Leaf() == true when the iterator is positioned at a "value node" which is not something that actually exists in an MMPT
func (sdb *StateDiffBuilder) processStateValueNode(it trie.NodeIterator, watchedAddressesLeafPaths [][]byte, prefixPath []byte) (*types2.AccountWrapper, error) {
	// skip if it is not a watched address
	// If we aren't watching any specific addresses, we are watching everything
	if len(watchedAddressesLeafPaths) > 0 && !isWatchedAddress(watchedAddressesLeafPaths, append(prefixPath, it.Path()...)) {
		return nil, nil
	}

	// since this is a "value node", we need to move up to the "parent" node which is the actual leaf node
	// it should be in the fastcache since it necessarily was recently accessed to reach the current node
	parentNodeRLP, err := sdb.StateCache.TrieDB().Node(it.Parent())
	if err != nil {
		return nil, err
	}
	var nodeElements []interface{}
	if err = rlp.DecodeBytes(parentNodeRLP, &nodeElements); err != nil {
		return nil, err
	}
	parentSubPath := make([]byte, len(it.ParentPath()))
	copy(parentSubPath, it.ParentPath())
	parentPath := append(prefixPath, parentSubPath...)
	partialPath := trie.CompactToHex(nodeElements[0].([]byte))
	valueNodePath := append(parentPath, partialPath...)
	encodedPath := trie.HexToCompact(valueNodePath)
	leafKey := encodedPath[1:]

	var account types.StateAccount
	accountRLP := make([]byte, len(it.LeafBlob()))
	copy(accountRLP, it.LeafBlob())
	if err := rlp.DecodeBytes(accountRLP, &account); err != nil {
		return nil, fmt.Errorf("error decoding account for leaf value at leaf key %x\nerror: %v", leafKey, err)
	}

	return &types2.AccountWrapper{
		LeafKey: leafKey,
		Account: &account,
		CID:     ipld2.Keccak256ToCid(ipld2.MEthStateTrie, crypto.Keccak256(parentNodeRLP)).String(),
	}, nil
}

// deletedOrUpdatedState returns a slice of all the pathes that are emptied at B
// and a mapping of their leafkeys to all the accounts that exist in a different state at A than B
func (sdb *StateDiffBuilder) deletedOrUpdatedState(a, b trie.NodeIterator, diffAccountsAtB types2.AccountMap,
	watchedAddressesLeafPaths [][]byte, output types2.StateNodeSink, logger log.Logger, prefixPath []byte) (types2.AccountMap, error) {
	logger.Debug("statediff BEGIN deletedOrUpdatedState")
	defer metrics2.ReportAndUpdateDuration("statediff END deletedOrUpdatedState", time.Now(), logger, metrics2.IndexerMetrics.DeletedOrUpdatedStateTimer)
	diffAccountAtA := make(types2.AccountMap)
	watchingAddresses := len(watchedAddressesLeafPaths) > 0

	it, _ := trie.NewDifferenceIterator(b, a)
	for it.Next(true) {
		// ignore node if it is not along paths of interest
		if watchingAddresses && !isValidPrefixPath(watchedAddressesLeafPaths, append(prefixPath, it.Path()...)) {
			continue
		}

		if it.Leaf() {
			accountW, err := sdb.processStateValueNode(it, watchedAddressesLeafPaths, prefixPath)
			if err != nil {
				return nil, err
			}
			if accountW == nil {
				continue
			}
			leafKey := common.Bytes2Hex(accountW.LeafKey)
			diffAccountAtA[leafKey] = *accountW
			// if this node's leaf key did not show up in diffAccountsAtB
			// that means the account was deleted
			// in that case, emit an empty "removed" diff state node
			// include empty "removed" diff storage nodes for all the storage slots
			if _, ok := diffAccountsAtB[leafKey]; !ok {
				diff := types2.StateLeafNode{
					AccountWrapper: types2.AccountWrapper{
						Account: nil,
						LeafKey: accountW.LeafKey,
						CID:     shared.RemovedNodeStateCID,
					},
					Removed: true,
				}

				storageDiff := make([]types2.StorageLeafNode, 0)
				err := sdb.buildRemovedAccountStorageNodes(accountW.Account.Root, StorageNodeAppender(&storageDiff))
				if err != nil {
					return nil, fmt.Errorf("failed building storage diffs for removed state account with key %x\r\nerror: %v", leafKey, err)
				}
				diff.StorageDiff = storageDiff
				if err := output(diff); err != nil {
					return nil, err
				}
			}
		}
	}
	return diffAccountAtA, it.Error()
}

// buildAccountUpdates uses the account diffs maps for A => B and B => A and the known intersection of their leafkeys
// to generate the statediff node objects for all of the accounts that existed at both A and B but in different states
// needs to be called before building account creations and deletions as this mutates
// those account maps to remove the accounts which were updated
func (sdb *StateDiffBuilder) buildAccountUpdates(creations, deletions types2.AccountMap, updatedKeys []string,
	output types2.StateNodeSink, ipldOutput types2.IPLDSink, logger log.Logger) error {
	logger.Debug("statediff BEGIN buildAccountUpdates", "creations", len(creations), "deletions", len(deletions), "updatedKeys", len(updatedKeys))
	defer metrics2.ReportAndUpdateDuration("statediff END buildAccountUpdates ", time.Now(), logger, metrics2.IndexerMetrics.BuildAccountUpdatesTimer)
	var err error
	for _, key := range updatedKeys {
		createdAcc := creations[key]
		deletedAcc := deletions[key]
		storageDiff := make([]types2.StorageLeafNode, 0)
		if deletedAcc.Account != nil && createdAcc.Account != nil {
			oldSR := deletedAcc.Account.Root
			newSR := createdAcc.Account.Root
			err = sdb.buildStorageNodesIncremental(
				oldSR, newSR, StorageNodeAppender(&storageDiff), ipldOutput)
			if err != nil {
				return fmt.Errorf("failed building incremental storage diffs for account with leafkey %s\r\nerror: %v", key, err)
			}
		}
		if err = output(types2.StateLeafNode{
			AccountWrapper: createdAcc,
			Removed:        false,
			StorageDiff:    storageDiff,
		}); err != nil {
			return err
		}
		delete(creations, key)
		delete(deletions, key)
	}

	return nil
}

// buildAccountCreations returns the statediff node objects for all the accounts that exist at B but not at A
// it also returns the code and codehash for created contract accounts
func (sdb *StateDiffBuilder) buildAccountCreations(accounts types2.AccountMap, output types2.StateNodeSink,
	ipldOutput types2.IPLDSink, logger log.Logger) error {
	logger.Debug("statediff BEGIN buildAccountCreations")
	defer metrics2.ReportAndUpdateDuration("statediff END buildAccountCreations", time.Now(), logger, metrics2.IndexerMetrics.BuildAccountCreationsTimer)
	for _, val := range accounts {
		diff := types2.StateLeafNode{
			AccountWrapper: val,
			Removed:        false,
		}
		if !bytes.Equal(val.Account.CodeHash, nullCodeHash) {
			// For contract creations, any storage node contained is a diff
			storageDiff := make([]types2.StorageLeafNode, 0)
			err := sdb.buildStorageNodesEventual(val.Account.Root, StorageNodeAppender(&storageDiff), ipldOutput)
			if err != nil {
				return fmt.Errorf("failed building eventual storage diffs for node with leaf key %x\r\nerror: %v", val.LeafKey, err)
			}
			diff.StorageDiff = storageDiff
			// emit codehash => code mappings for contract
			codeHash := common.BytesToHash(val.Account.CodeHash)
			code, err := sdb.StateCache.ContractCode(common.Hash{}, codeHash)
			if err != nil {
				return fmt.Errorf("failed to retrieve code for codehash %s\r\n error: %v", codeHash.String(), err)
			}
			if err := ipldOutput(types2.IPLD{
				CID:     ipld2.Keccak256ToCid(ipld2.RawBinary, codeHash.Bytes()).String(),
				Content: code,
			}); err != nil {
				return err
			}
		}
		if err := output(diff); err != nil {
			return err
		}
	}

	return nil
}

// buildStorageNodesEventual builds the storage diff node objects for a created account
// i.e. it returns all the storage nodes at this state, since there is no previous state
func (sdb *StateDiffBuilder) buildStorageNodesEventual(sr common.Hash, output types2.StorageNodeSink,
	ipldOutput types2.IPLDSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildStorageNodesEventualTimer)
	if bytes.Equal(sr.Bytes(), emptyContractRoot.Bytes()) {
		return nil
	}
	log.Debug("Storage Root For Eventual Diff", "root", sr.Hex())
	sTrie, err := sdb.StateCache.OpenTrie(sr)
	if err != nil {
		log.Info("error in build storage diff eventual", "error", err)
		return err
	}
	it := sTrie.NodeIterator(make([]byte, 0))
	err = sdb.buildStorageNodesFromTrie(it, output, ipldOutput)
	if err != nil {
		return err
	}
	return nil
}

// buildStorageNodesFromTrie returns all the storage diff node objects in the provided node interator
// including intermediate nodes can be turned on or off
func (sdb *StateDiffBuilder) buildStorageNodesFromTrie(it trie.NodeIterator, output types2.StorageNodeSink,
	ipldOutput types2.IPLDSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildStorageNodesFromTrieTimer)
	for it.Next(true) {
		if it.Leaf() {
			storageLeafNode, err := sdb.processStorageValueNode(it)
			if err != nil {
				return err
			}
			if err := output(storageLeafNode); err != nil {
				return err
			}
		} else {
			nodeVal := make([]byte, len(it.NodeBlob()))
			copy(nodeVal, it.NodeBlob())
			nodeHash := make([]byte, len(it.Hash().Bytes()))
			copy(nodeHash, it.Hash().Bytes())
			if err := ipldOutput(types2.IPLD{
				CID:     ipld2.Keccak256ToCid(ipld2.MEthStorageTrie, nodeHash).String(),
				Content: nodeVal,
			}); err != nil {
				return err
			}
		}
	}
	return it.Error()
}

// reminder: it.Leaf() == true when the iterator is positioned at a "value node" which is not something that actually exists in an MMPT
func (sdb *StateDiffBuilder) processStorageValueNode(it trie.NodeIterator) (types2.StorageLeafNode, error) {
	// skip if it is not a watched address
	leafKey := make([]byte, len(it.LeafKey()))
	copy(leafKey, it.LeafKey())
	value := make([]byte, len(it.LeafBlob()))
	copy(value, it.LeafBlob())

	// since this is a "value node", we need to move up to the "parent" node which is the actual leaf node
	// it should be in the fastcache since it necessarily was recently accessed to reach the current node
	parentNodeRLP, err := sdb.StateCache.TrieDB().Node(it.Parent())
	if err != nil {
		return types2.StorageLeafNode{}, err
	}

	return types2.StorageLeafNode{
		LeafKey: leafKey,
		Value:   value,
		CID:     ipld2.Keccak256ToCid(ipld2.MEthStorageTrie, crypto.Keccak256(parentNodeRLP)).String(),
	}, nil
}

// buildRemovedAccountStorageNodes builds the "removed" diffs for all the storage nodes for a destroyed account
func (sdb *StateDiffBuilder) buildRemovedAccountStorageNodes(sr common.Hash, output types2.StorageNodeSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildRemovedAccountStorageNodesTimer)
	if bytes.Equal(sr.Bytes(), emptyContractRoot.Bytes()) {
		return nil
	}
	log.Debug("Storage Root For Removed Diffs", "root", sr.Hex())
	sTrie, err := sdb.StateCache.OpenTrie(sr)
	if err != nil {
		log.Info("error in build removed account storage diffs", "error", err)
		return err
	}
	it := sTrie.NodeIterator(make([]byte, 0))
	err = sdb.buildRemovedStorageNodesFromTrie(it, output)
	if err != nil {
		return err
	}
	return nil
}

// buildRemovedStorageNodesFromTrie returns diffs for all the storage nodes in the provided node interator
func (sdb *StateDiffBuilder) buildRemovedStorageNodesFromTrie(it trie.NodeIterator, output types2.StorageNodeSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildRemovedStorageNodesFromTrieTimer)
	for it.Next(true) {
		if it.Leaf() { // only leaf values are indexed, don't need to demarcate removed intermediate nodes
			leafKey := make([]byte, len(it.LeafKey()))
			copy(leafKey, it.LeafKey())
			if err := output(types2.StorageLeafNode{
				CID:     shared.RemovedNodeStorageCID,
				Removed: true,
				LeafKey: leafKey,
				Value:   []byte{},
			}); err != nil {
				return err
			}
		}
	}
	return it.Error()
}

// buildStorageNodesIncremental builds the storage diff node objects for all nodes that exist in a different state at B than A
func (sdb *StateDiffBuilder) buildStorageNodesIncremental(oldSR common.Hash, newSR common.Hash, output types2.StorageNodeSink,
	ipldOutput types2.IPLDSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.BuildStorageNodesIncrementalTimer)
	if bytes.Equal(newSR.Bytes(), oldSR.Bytes()) {
		return nil
	}
	log.Trace("Storage Roots for Incremental Diff", "old", oldSR.Hex(), "new", newSR.Hex())
	oldTrie, err := sdb.StateCache.OpenTrie(oldSR)
	if err != nil {
		return err
	}
	newTrie, err := sdb.StateCache.OpenTrie(newSR)
	if err != nil {
		return err
	}

	diffSlotsAtB, err := sdb.createdAndUpdatedStorage(
		oldTrie.NodeIterator([]byte{}), newTrie.NodeIterator([]byte{}), output, ipldOutput)
	if err != nil {
		return err
	}
	err = sdb.deletedOrUpdatedStorage(oldTrie.NodeIterator([]byte{}), newTrie.NodeIterator([]byte{}),
		diffSlotsAtB, output)
	if err != nil {
		return err
	}
	return nil
}

func (sdb *StateDiffBuilder) createdAndUpdatedStorage(a, b trie.NodeIterator, output types2.StorageNodeSink,
	ipldOutput types2.IPLDSink) (map[string]bool, error) {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.CreatedAndUpdatedStorageTimer)
	diffSlotsAtB := make(map[string]bool)
	it, _ := trie.NewDifferenceIterator(a, b)
	for it.Next(true) {
		if it.Leaf() {
			storageLeafNode, err := sdb.processStorageValueNode(it)
			if err != nil {
				return nil, err
			}
			if err := output(storageLeafNode); err != nil {
				return nil, err
			}
			diffSlotsAtB[common.Bytes2Hex(storageLeafNode.LeafKey)] = true
		} else {
			if bytes.Equal(it.Hash().Bytes(), nullNodeHash) {
				continue
			}
			nodeVal := make([]byte, len(it.NodeBlob()))
			copy(nodeVal, it.NodeBlob())
			nodeHash := make([]byte, len(it.Hash().Bytes()))
			copy(nodeHash, it.Hash().Bytes())
			if err := ipldOutput(types2.IPLD{
				CID:     ipld2.Keccak256ToCid(ipld2.MEthStorageTrie, nodeHash).String(),
				Content: nodeVal,
			}); err != nil {
				return nil, err
			}
		}
	}
	return diffSlotsAtB, it.Error()
}

func (sdb *StateDiffBuilder) deletedOrUpdatedStorage(a, b trie.NodeIterator, diffSlotsAtB map[string]bool, output types2.StorageNodeSink) error {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.DeletedOrUpdatedStorageTimer)
	it, _ := trie.NewDifferenceIterator(b, a)
	for it.Next(true) {
		if it.Leaf() {
			leafKey := make([]byte, len(it.LeafKey()))
			copy(leafKey, it.LeafKey())
			// if this node's leaf key did not show up in diffSlotsAtB
			// that means the storage slot was vacated
			// in that case, emit an empty "removed" diff storage node
			if _, ok := diffSlotsAtB[common.Bytes2Hex(leafKey)]; !ok {
				if err := output(types2.StorageLeafNode{
					CID:     shared.RemovedNodeStorageCID,
					Removed: true,
					LeafKey: leafKey,
					Value:   []byte{},
				}); err != nil {
					return err
				}
			}
		}
	}
	return it.Error()
}

// isValidPrefixPath is used to check if a node at currentPath is a parent | ancestor to one of the addresses the builder is configured to watch
func isValidPrefixPath(watchedAddressesLeafPaths [][]byte, currentPath []byte) bool {
	for _, watchedAddressPath := range watchedAddressesLeafPaths {
		if bytes.HasPrefix(watchedAddressPath, currentPath) {
			return true
		}
	}

	return false
}

// isWatchedAddress is used to check if a state account corresponds to one of the addresses the builder is configured to watch
func isWatchedAddress(watchedAddressesLeafPaths [][]byte, valueNodePath []byte) bool {
	defer metrics2.UpdateDuration(time.Now(), metrics2.IndexerMetrics.IsWatchedAddressTimer)
	for _, watchedAddressPath := range watchedAddressesLeafPaths {
		if bytes.Equal(watchedAddressPath, valueNodePath) {
			return true
		}
	}

	return false
}

// isLeaf checks if the node we are at is a leaf
func isLeaf(elements []interface{}) (bool, error) {
	if len(elements) > 2 {
		return false, nil
	}
	if len(elements) < 2 {
		return false, fmt.Errorf("node cannot be less than two elements in length")
	}
	switch elements[0].([]byte)[0] / 16 {
	case '\x00':
		return false, nil
	case '\x01':
		return false, nil
	case '\x02':
		return true, nil
	case '\x03':
		return true, nil
	default:
		return false, fmt.Errorf("unknown hex prefix")
	}
}
