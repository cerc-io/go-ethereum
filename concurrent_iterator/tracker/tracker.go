package tracker

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
	log "github.com/sirupsen/logrus"

	iter "github.com/vulcanize/go-eth-state-node-iterator"
)

type Tracker struct {
	recoveryFile string

	startChan chan *Iterator
	stopChan  chan *Iterator
	started   map[*Iterator]struct{}
	stopped   []*Iterator
	running   bool
}

type Iterator struct {
	trie.NodeIterator
	tracker *Tracker

	SeekedPath []byte // latest path seeked from the tracked iterator
	EndPath    []byte // endPath for the tracked iterator
}

func New(file string, buf uint) Tracker {
	return Tracker{
		recoveryFile: file,
		startChan:    make(chan *Iterator, buf),
		stopChan:     make(chan *Iterator, buf),
		started:      map[*Iterator]struct{}{},
		running:      true,
	}
}

func (tr *Tracker) CaptureSignal(cancelCtx context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Errorf("Signal received (%v), stopping", sig)
		// cancel context on receiving a signal
		// on ctx cancellation, all the iterators complete processing of their current node before stopping
		cancelCtx()
	}()
}

// Wraps an iterator in a Iterator. This should not be called once halts are possible.
func (tr *Tracker) Tracked(it trie.NodeIterator, recoveredPath []byte) (ret *Iterator) {
	// create seeked path of max capacity (65)
	iterSeekedPath := make([]byte, 0, 65)
	// intially populate seeked path with the recovered path
	// to be used in trie traversal
	if recoveredPath != nil {
		iterSeekedPath = append(iterSeekedPath, recoveredPath...)
	}

	// if the iterator being tracked is a PrefixBoundIterator, capture it's end path
	// to be used in trie traversal
	var endPath []byte
	if boundedIter, ok := it.(*iter.PrefixBoundIterator); ok {
		endPath = boundedIter.EndPath
	}

	ret = &Iterator{it, tr, iterSeekedPath, endPath}
	tr.startChan <- ret
	return
}

// explicitly stops an iterator
func (tr *Tracker) StopIterator(it *Iterator) {
	tr.stopChan <- it
}

// dumps iterator path and bounds to a text file so it can be restored later
func (tr *Tracker) dump() error {
	log.Debugf("Dumping recovery state to: %s", tr.recoveryFile)
	var rows [][]string
	for it := range tr.started {
		var startPath []byte
		var endPath []byte
		if impl, ok := it.NodeIterator.(*iter.PrefixBoundIterator); ok {
			// if the iterator being tracked is a PrefixBoundIterator,
			// initialize start and end paths with its bounds
			startPath = impl.StartPath
			endPath = impl.EndPath
		}

		// if seeked path and iterator path are non-empty, use iterator's path as startpath
		if !bytes.Equal(it.SeekedPath, []byte{}) && !bytes.Equal(it.Path(), []byte{}) {
			startPath = it.Path()
		}

		rows = append(rows, []string{
			fmt.Sprintf("%x", startPath),
			fmt.Sprintf("%x", endPath),
			fmt.Sprintf("%x", it.SeekedPath),
		})
	}

	file, err := os.Create(tr.recoveryFile)
	if err != nil {
		return err
	}
	defer file.Close()
	out := csv.NewWriter(file)

	return out.WriteAll(rows)
}

// attempts to read iterator state from file
// if file doesn't exist, returns an empty slice with no error
func (tr *Tracker) Restore(tree state.Trie) ([]trie.NodeIterator, error) {
	file, err := os.Open(tr.recoveryFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	log.Debugf("Restoring recovery state from: %s", tr.recoveryFile)

	defer file.Close()
	in := csv.NewReader(file)
	in.FieldsPerRecord = 3
	rows, err := in.ReadAll()
	if err != nil {
		return nil, err
	}

	var ret []trie.NodeIterator
	for _, row := range rows {
		// pick up where each interval left off
		var startPath []byte
		var endPath []byte
		var recoveredPath []byte

		if len(row[0]) != 0 {
			if _, err = fmt.Sscanf(row[0], "%x", &startPath); err != nil {
				return nil, err
			}
		}
		if len(row[1]) != 0 {
			if _, err = fmt.Sscanf(row[1], "%x", &endPath); err != nil {
				return nil, err
			}
		}
		if len(row[2]) != 0 {
			if _, err = fmt.Sscanf(row[2], "%x", &recoveredPath); err != nil {
				return nil, err
			}
		}

		// force the lower bound path to an even length (required by geth API/HexToKeyBytes)
		if len(startPath)&0b1 == 1 {
			// decrement first to avoid skipped nodes
			decrementPath(startPath)
			startPath = append(startPath, 0)
		}
		it := iter.NewPrefixBoundIterator(tree.NodeIterator(iter.HexToKeyBytes(startPath)), startPath, endPath)
		ret = append(ret, tr.Tracked(it, recoveredPath))
	}

	log.Debugf("Restored %d iterators", len(ret))
	return ret, nil
}

func (tr *Tracker) HaltAndDump() error {
	tr.running = false

	// drain any pending iterators
	close(tr.startChan)
	for start := range tr.startChan {
		tr.started[start] = struct{}{}
	}
	close(tr.stopChan)
	for stop := range tr.stopChan {
		tr.stopped = append(tr.stopped, stop)
	}

	for _, stop := range tr.stopped {
		delete(tr.started, stop)
	}

	if len(tr.started) == 0 {
		// if the tracker state is empty, erase any existing recovery file
		err := os.Remove(tr.recoveryFile)
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}

	return tr.dump()
}

func (it *Iterator) Next(descend bool) bool {
	ret := it.NodeIterator.Next(descend)

	if !ret {
		if it.tracker.running {
			it.tracker.stopChan <- it
		} else {
			log.Errorf("iterator stopped after tracker halted: path=%x", it.Path())
		}
	}
	return ret
}

// Subtracts 1 from the last byte in a path slice, carrying if needed.
// Does nothing, returning false, for all-zero inputs.
func decrementPath(path []byte) bool {
	// check for all zeros
	allzero := true
	for i := 0; i < len(path); i++ {
		allzero = allzero && path[i] == 0
	}
	if allzero {
		return false
	}
	for i := len(path) - 1; i >= 0; i-- {
		val := path[i]
		path[i]--
		if val == 0 {
			path[i] = 0xf
		} else {
			return true
		}
	}
	return true
}
