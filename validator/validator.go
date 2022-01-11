package validator

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/rpc"
)

// serviceParams wraps constructor parameters
type serviceParams struct {
	blockNum, trailNum uint64
}

type service struct {
	params  *serviceParams
	backend *eth.Ethereum
	chain   *core.BlockChain
}

func newService(backend *eth.Ethereum, params *serviceParams) *service {
	s := &service{
		params:  params,
		backend: backend,
	}

	return s
}

// start is used to begin the service
func (sds *service) start() (uint64, error) {
	blockNum := sds.params.blockNum
	trailNum := sds.params.trailNum
	indexBlockNum := blockNum + 1
	ctx := context.Background()

	for {
		if blockNum+trailNum < indexBlockNum {
			break
		}

		rpcBlockNum := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(indexBlockNum))
		block, err := sds.backend.APIBackend.BlockByNumberOrHash(ctx, rpcBlockNum)
		if err != nil {
			return blockNum, err
		}

		totalTxn := len(block.Transactions())
		_, _, stateDB, err := sds.backend.APIBackend.StateAtTransaction(ctx, block, totalTxn+1, 0)
		if err != nil {
			return blockNum, err
		}

		blockStateRoot := block.Header().Root.String()
		dbStateRoot := stateDB.IntermediateRoot(true).String()
		// verify the state root
		if blockStateRoot != dbStateRoot {
			return blockNum, fmt.Errorf("failed to verify state root")
		}

		indexBlockNum++
	}

	return indexBlockNum, nil
}
