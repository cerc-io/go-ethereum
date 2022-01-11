package validator

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pgipfsethdb "github.com/vulcanize/ipfs-ethdb/postgres"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)

	testBalance = big.NewInt(2e18)

	vldParams = &serviceParams{
		blockNum: 1,
		trailNum: 5,
	}

	cacheConfig = pgipfsethdb.CacheConfig{
		Name:           "db",
		Size:           3000000, // 3MB
		ExpiryDuration: time.Hour,
	}

	testConnStr = "postgresql://vdbm:password@localhost:5432/vulcanize_public?sslmode=disable"
)

// startEthService creates a full node instance for testing.
func startEthService(t *testing.T, genesis *core.Genesis, blocks []*types.Block) (*node.Node, *eth.Ethereum) {
	t.Helper()

	n, err := node.New(&node.Config{})
	require.NoError(t, err)

	ethcfg := &ethconfig.Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}, TrieTimeout: time.Minute, TrieDirtyCache: 256, TrieCleanCache: 256, PostgresDB: true, PgConnStr: testConnStr, PgCacheConfig: cacheConfig}
	ethservice, err := eth.New(n, ethcfg)
	require.NoError(t, err)

	err = n.Start()
	require.NoError(t, err)

	if _, err := ethservice.BlockChain().InsertChain(blocks); err != nil {
		n.Close()
		t.Fatal("can't import test blocks:", err)
	}

	ethservice.SetEtherbase(testAddr)
	ethservice.SetSynced()

	return n, ethservice
}

func generatePreMergeChain(n int) (*core.Genesis, []*types.Block) {
	db := rawdb.NewMemoryDatabase()
	config := params.AllEthashProtocolChanges
	genesis := &core.Genesis{
		Config:    config,
		Alloc:     core.GenesisAlloc{testAddr: {Balance: testBalance}},
		ExtraData: []byte("test genesis"),
		Timestamp: 9000,
		BaseFee:   big.NewInt(params.InitialBaseFee),
	}
	testNonce := uint64(0)
	generate := func(i int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("test"))
		tx, _ := types.SignTx(types.NewTransaction(testNonce, common.HexToAddress("0x9a9070028361F7AAbeB3f2F2Dc07F82C4a98A02a"), big.NewInt(1), params.TxGas, big.NewInt(params.InitialBaseFee*2), nil), types.LatestSigner(config), testKey)
		g.AddTx(tx)
		testNonce++
	}
	gblock := genesis.ToBlock(db)
	engine := ethash.NewFaker()
	blocks, _ := core.GenerateChain(config, gblock, engine, db, n, generate)
	totalDifficulty := big.NewInt(0)
	for _, b := range blocks {
		totalDifficulty.Add(totalDifficulty, b.Difficulty())
	}
	config.TerminalTotalDifficulty = totalDifficulty
	return genesis, blocks
}

func TestValidatorService(t *testing.T) {
	genesis, blocks := generatePreMergeChain(10)
	n, ethSrvc := startEthService(t, genesis, blocks)
	defer n.Close()

	vldSrvc := newService(ethSrvc, vldParams)
	blockNum, err := vldSrvc.start()
	require.NoError(t, err)

	expectedBlockNum := vldParams.blockNum + vldParams.trailNum
	require.Less(t, expectedBlockNum, blockNum)
}
