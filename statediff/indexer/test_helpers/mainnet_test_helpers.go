// VulcanizeDB
// Copyright © 2021 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package test_helpers

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	defaultBlockFilePath    = "../../../mainnet_data/block"
	defaultReceiptsFilePath = "../../../mainnet_data/receipts"
)

const (
	TEST_RAW_URL      = "TEST_RAW_URL"
	TEST_BLOCK_NUMBER = "TEST_BLOCK_NUMBER"
)

// ProblemBlocks list of known problem blocks, with funky edge cases
var ProblemBlocks = []int64{
	12600011,
	12619985,
	12625121,
	12655432,
	12579670,
	12914664,
}

// TestConfig holds configuration params for mainnet tests
type TestConfig struct {
	RawURL      string
	BlockNumber *big.Int
	LocalCache  bool
}

// DefaultTestConfig is the default TestConfig
var DefaultTestConfig = TestConfig{
	RawURL:      "http://127.0.0.1:8545",
	BlockNumber: big.NewInt(12914664),
	LocalCache:  true,
}

// TestBlockAndReceiptsFromEnv retrieves the block and receipts using env variables to override default config block number
func TestBlockAndReceiptsFromEnv(conf TestConfig) (*types.Block, types.Receipts, error) {
	blockNumberStr := os.Getenv(TEST_BLOCK_NUMBER)
	blockNumber, ok := new(big.Int).SetString(blockNumberStr, 10)
	if !ok {
		fmt.Printf("Warning: no blockNumber configured for statediffing mainnet tests, using default (%d)\r\n",
			DefaultTestConfig.BlockNumber)
	} else {
		conf.BlockNumber = blockNumber
	}
	return TestBlockAndReceipts(conf)
}

// TestBlockAndReceipts retrieves the block and receipts for the provided test config
// It first tries to load files from the local system before setting up and using an ethclient.Client to pull the data
func TestBlockAndReceipts(conf TestConfig) (*types.Block, types.Receipts, error) {
	var cli *ethclient.Client
	var err error
	var block *types.Block
	var receipts types.Receipts
	blockFilePath := fmt.Sprintf("%s_%s.rlp", defaultBlockFilePath, conf.BlockNumber.String())
	if _, err = os.Stat(blockFilePath); !errors.Is(err, os.ErrNotExist) {
		fmt.Printf("local file (%s) found for block %s\n", blockFilePath, conf.BlockNumber.String())
		block, err = LoadBlockRLP(blockFilePath)
		if err != nil {
			fmt.Printf("loading local file (%s) failed (%s), dialing remote client at %s\n", blockFilePath, err.Error(), conf.RawURL)
			cli, err = ethclient.Dial(conf.RawURL)
			if err != nil {
				return nil, nil, err
			}
			block, err = FetchBlock(cli, conf.BlockNumber)
			if err != nil {
				return nil, nil, err
			}
			if conf.LocalCache {
				if err := WriteBlockRLP(blockFilePath, block); err != nil {
					return nil, nil, err
				}
			}
		}
	} else {
		fmt.Printf("no local file found for block %s, dialing remote client at %s\n", conf.BlockNumber.String(), conf.RawURL)
		cli, err = ethclient.Dial(conf.RawURL)
		if err != nil {
			return nil, nil, err
		}
		block, err = FetchBlock(cli, conf.BlockNumber)
		if err != nil {
			return nil, nil, err
		}
		if conf.LocalCache {
			if err := WriteBlockRLP(blockFilePath, block); err != nil {
				return nil, nil, err
			}
		}
	}
	receiptsFilePath := fmt.Sprintf("%s_%s.rlp", defaultReceiptsFilePath, conf.BlockNumber.String())
	if _, err = os.Stat(receiptsFilePath); !errors.Is(err, os.ErrNotExist) {
		fmt.Printf("local file (%s) found for block %s receipts\n", receiptsFilePath, conf.BlockNumber.String())
		receipts, err = LoadReceiptsEncoding(receiptsFilePath, len(block.Transactions()))
		if err != nil {
			fmt.Printf("loading local file (%s) failed (%s), dialing remote client at %s\n", receiptsFilePath, err.Error(), conf.RawURL)
			if cli == nil {
				cli, err = ethclient.Dial(conf.RawURL)
				if err != nil {
					return nil, nil, err
				}
			}
			receipts, err = FetchReceipts(cli, block)
			if err != nil {
				return nil, nil, err
			}
			if conf.LocalCache {
				if err := WriteReceiptsEncoding(receiptsFilePath, block.Number(), receipts); err != nil {
					return nil, nil, err
				}
			}
		}
	} else {
		fmt.Printf("no local file found for block %s receipts, dialing remote client at %s\n", conf.BlockNumber.String(), conf.RawURL)
		if cli == nil {
			cli, err = ethclient.Dial(conf.RawURL)
			if err != nil {
				return nil, nil, err
			}
		}
		receipts, err = FetchReceipts(cli, block)
		if err != nil {
			return nil, nil, err
		}
		if conf.LocalCache {
			if err := WriteReceiptsEncoding(receiptsFilePath, block.Number(), receipts); err != nil {
				return nil, nil, err
			}
		}
	}
	return block, receipts, nil
}

// FetchBlock fetches the block at the provided height using the ethclient.Client
func FetchBlock(cli *ethclient.Client, blockNumber *big.Int) (*types.Block, error) {
	return cli.BlockByNumber(context.Background(), blockNumber)
}

// FetchReceipts fetches the receipts for the provided block using the ethclient.Client
func FetchReceipts(cli *ethclient.Client, block *types.Block) (types.Receipts, error) {
	receipts := make(types.Receipts, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		rct, err := cli.TransactionReceipt(context.Background(), tx.Hash())
		if err != nil {
			return nil, err
		}
		receipts[i] = rct
	}
	return receipts, nil
}

// WriteBlockRLP writes out the RLP encoding of the block to the provided filePath
func WriteBlockRLP(filePath string, block *types.Block) error {
	if filePath == "" {
		filePath = fmt.Sprintf("%s_%s.rlp", defaultBlockFilePath, block.Number().String())
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot create file, file (%s) already exists", filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
	}
	fmt.Printf("writing block rlp to file at %s\r\n", filePath)
	if err := block.EncodeRLP(file); err != nil {
		return err
	}
	return file.Close()
}

// LoadBlockRLP loads block from the rlp at filePath
func LoadBlockRLP(filePath string) (*types.Block, error) {
	blockBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	block := new(types.Block)
	return block, rlp.DecodeBytes(blockBytes, block)
}

// LoadReceiptsEncoding loads receipts from the encoding at filePath
func LoadReceiptsEncoding(filePath string, cap int) (types.Receipts, error) {
	rctsBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	receipts := new(types.Receipts)
	return *receipts, rlp.DecodeBytes(rctsBytes, receipts)
}

// WriteReceiptsEncoding writes out the consensus encoding of the receipts to the provided io.WriteCloser
func WriteReceiptsEncoding(filePath string, blockNumber *big.Int, receipts types.Receipts) error {
	if filePath == "" {
		filePath = fmt.Sprintf("%s_%s.rlp", defaultReceiptsFilePath, blockNumber.String())
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot create file, file (%s) already exists", filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
	}
	defer file.Close()
	fmt.Printf("writing receipts rlp to file at %s\r\n", filePath)
	return rlp.Encode(file, receipts)
}
