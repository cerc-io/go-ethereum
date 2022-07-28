// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/external"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/accounts/scwallet"
	"github.com/ethereum/go-ethereum/accounts/usbwallet"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff"
	dumpdb "github.com/ethereum/go-ethereum/statediff/indexer/database/dump"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/naoina/toml"
	"github.com/urfave/cli/v2"
	"math/big"
	"os"
	"reflect"
	"time"
	"unicode"
)

var (
	dumpConfigCommand = &cli.Command{
		Action:      dumpConfig,
		Name:        "dumpconfig",
		Usage:       "Show configuration values",
		ArgsUsage:   "",
		Flags:       flags.Merge(nodeFlags, rpcFlags),
		Description: `The dumpconfig command shows configuration values.`,
	}

	configFileFlag = &cli.StringFlag{
		Name:     "config",
		Usage:    "TOML configuration file",
		Category: flags.EthCategory,
	}
)

// These settings ensure that TOML keys use the same names as Go struct fields.
var tomlSettings = toml.Config{
	NormFieldName: func(rt reflect.Type, key string) string {
		return key
	},
	FieldToKey: func(rt reflect.Type, field string) string {
		return field
	},
	MissingField: func(rt reflect.Type, field string) error {
		id := fmt.Sprintf("%s.%s", rt.String(), field)
		if deprecated(id) {
			log.Warn("Config field is deprecated and won't have an effect", "name", id)
			return nil
		}
		var link string
		if unicode.IsUpper(rune(rt.Name()[0])) && rt.PkgPath() != "main" {
			link = fmt.Sprintf(", see https://godoc.org/%s#%s for available fields", rt.PkgPath(), rt.Name())
		}
		return fmt.Errorf("field '%s' is not defined in %s%s", field, rt.String(), link)
	},
}

type ethstatsConfig struct {
	URL string `toml:",omitempty"`
}

type gethConfig struct {
	Eth      ethconfig.Config
	Node     node.Config
	Ethstats ethstatsConfig
	Metrics  metrics.Config
}

func loadConfig(file string, cfg *gethConfig) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	err = tomlSettings.NewDecoder(bufio.NewReader(f)).Decode(cfg)
	// Add file name to errors that have a line number.
	if _, ok := err.(*toml.LineError); ok {
		err = errors.New(file + ", " + err.Error())
	}
	return err
}

func defaultNodeConfig() node.Config {
	cfg := node.DefaultConfig
	cfg.Name = clientIdentifier
	cfg.Version = params.VersionWithCommit(gitCommit, gitDate)
	cfg.HTTPModules = append(cfg.HTTPModules, "eth")
	cfg.WSModules = append(cfg.WSModules, "eth")
	cfg.IPCPath = "geth.ipc"
	return cfg
}

// makeConfigNode loads geth configuration and creates a blank node instance.
func makeConfigNode(ctx *cli.Context) (*node.Node, gethConfig) {
	// Load defaults.
	cfg := gethConfig{
		Eth:     ethconfig.Defaults,
		Node:    defaultNodeConfig(),
		Metrics: metrics.DefaultConfig,
	}

	// Load config file.
	if file := ctx.String(configFileFlag.Name); file != "" {
		if err := loadConfig(file, &cfg); err != nil {
			utils.Fatalf("%v", err)
		}
	}

	// Apply flags.
	utils.SetNodeConfig(ctx, &cfg.Node)
	stack, err := node.New(&cfg.Node)
	if err != nil {
		utils.Fatalf("Failed to create the protocol stack: %v", err)
	}
	// Node doesn't by default populate account manager backends
	if err := setAccountManagerBackends(stack); err != nil {
		utils.Fatalf("Failed to set account manager backends: %v", err)
	}

	utils.SetEthConfig(ctx, stack, &cfg.Eth)
	if ctx.IsSet(utils.EthStatsURLFlag.Name) {
		cfg.Ethstats.URL = ctx.String(utils.EthStatsURLFlag.Name)
	}
	applyMetricConfig(ctx, &cfg)
	if ctx.Bool(utils.StateDiffFlag.Name) {
		cfg.Eth.Diffing = true
	}

	return stack, cfg
}

// makeFullNode loads geth configuration and creates the Ethereum backend.
func makeFullNode(ctx *cli.Context) (*node.Node, ethapi.Backend) {
	stack, cfg := makeConfigNode(ctx)
	if ctx.IsSet(utils.OverrideGrayGlacierFlag.Name) {
		cfg.Eth.OverrideGrayGlacier = new(big.Int).SetUint64(ctx.Uint64(utils.OverrideGrayGlacierFlag.Name))
	}
	if ctx.IsSet(utils.OverrideTerminalTotalDifficulty.Name) {
		cfg.Eth.OverrideTerminalTotalDifficulty = flags.GlobalBig(ctx, utils.OverrideTerminalTotalDifficulty.Name)
	}

	backend, eth := utils.RegisterEthService(stack, &cfg.Eth)
	// Warn users to migrate if they have a legacy freezer format.
	if eth != nil && !ctx.IsSet(utils.IgnoreLegacyReceiptsFlag.Name) {
		firstIdx := uint64(0)
		// Hack to speed up check for mainnet because we know
		// the first non-empty block.
		ghash := rawdb.ReadCanonicalHash(eth.ChainDb(), 0)
		if cfg.Eth.NetworkId == 1 && ghash == params.MainnetGenesisHash {
			firstIdx = 46147
		}
		isLegacy, _, err := dbHasLegacyReceipts(eth.ChainDb(), firstIdx)
		if err != nil {
			log.Error("Failed to check db for legacy receipts", "err", err)
		} else if isLegacy {
			stack.Close()
			utils.Fatalf("Database has receipts with a legacy format. Please run `geth db freezer-migrate`.")
		}
	}

	if ctx.Bool(utils.StateDiffFlag.Name) {
		var indexerConfig interfaces.Config
		var clientName, nodeID string
		if ctx.IsSet(utils.StateDiffWritingFlag.Name) {
			clientName = ctx.String(utils.StateDiffDBClientNameFlag.Name)
			if ctx.IsSet(utils.StateDiffDBNodeIDFlag.Name) {
				nodeID = ctx.String(utils.StateDiffDBNodeIDFlag.Name)
			} else {
				utils.Fatalf("Must specify node ID for statediff DB output")
			}

			dbTypeStr := ctx.String(utils.StateDiffDBTypeFlag.Name)
			dbType, err := shared.ResolveDBType(dbTypeStr)
			if err != nil {
				utils.Fatalf("%v", err)
			}
			switch dbType {
			case shared.FILE:
				fileModeStr := ctx.String(utils.StateDiffFileMode.Name)
				fileMode, err := file.ResolveFileMode(fileModeStr)
				if err != nil {
					utils.Fatalf("%v", err)
				}

				indexerConfig = file.Config{
					Mode:                     fileMode,
					OutputDir:                ctx.String(utils.StateDiffFileCsvDir.Name),
					FilePath:                 ctx.String(utils.StateDiffFilePath.Name),
					WatchedAddressesFilePath: ctx.String(utils.StateDiffWatchedAddressesFilePath.Name),
				}
			case shared.POSTGRES:
				driverTypeStr := ctx.String(utils.StateDiffDBDriverTypeFlag.Name)
				driverType, err := postgres.ResolveDriverType(driverTypeStr)
				if err != nil {
					utils.Fatalf("%v", err)
				}
				pgConfig := postgres.Config{
					Hostname:     ctx.String(utils.StateDiffDBHostFlag.Name),
					Port:         ctx.Int(utils.StateDiffDBPortFlag.Name),
					DatabaseName: ctx.String(utils.StateDiffDBNameFlag.Name),
					Username:     ctx.String(utils.StateDiffDBUserFlag.Name),
					Password:     ctx.String(utils.StateDiffDBPasswordFlag.Name),
					ID:           nodeID,
					ClientName:   clientName,
					Driver:       driverType,
				}
				if ctx.IsSet(utils.StateDiffDBMinConns.Name) {
					pgConfig.MinConns = ctx.Int(utils.StateDiffDBMinConns.Name)
				}
				if ctx.IsSet(utils.StateDiffDBMaxConns.Name) {
					pgConfig.MaxConns = ctx.Int(utils.StateDiffDBMaxConns.Name)
				}
				if ctx.IsSet(utils.StateDiffDBMaxIdleConns.Name) {
					pgConfig.MaxIdle = ctx.Int(utils.StateDiffDBMaxIdleConns.Name)
				}
				if ctx.IsSet(utils.StateDiffDBMaxConnLifetime.Name) {
					pgConfig.MaxConnLifetime = time.Duration(ctx.Duration(utils.StateDiffDBMaxConnLifetime.Name).Seconds())
				}
				if ctx.IsSet(utils.StateDiffDBMaxConnIdleTime.Name) {
					pgConfig.MaxConnIdleTime = time.Duration(ctx.Duration(utils.StateDiffDBMaxConnIdleTime.Name).Seconds())
				}
				if ctx.IsSet(utils.StateDiffDBConnTimeout.Name) {
					pgConfig.ConnTimeout = time.Duration(ctx.Duration(utils.StateDiffDBConnTimeout.Name).Seconds())
				}
				indexerConfig = pgConfig
			case shared.DUMP:
				dumpTypeStr := ctx.String(utils.StateDiffDBDumpDst.Name)
				dumpType, err := dumpdb.ResolveDumpType(dumpTypeStr)
				if err != nil {
					utils.Fatalf("%v", err)
				}
				switch dumpType {
				case dumpdb.STDERR:
					indexerConfig = dumpdb.Config{Dump: os.Stdout}
				case dumpdb.STDOUT:
					indexerConfig = dumpdb.Config{Dump: os.Stderr}
				case dumpdb.DISCARD:
					indexerConfig = dumpdb.Config{Dump: dumpdb.NewDiscardWriterCloser()}
				default:
					utils.Fatalf("unrecognized dump destination: %s", dumpType)
				}
			default:
				utils.Fatalf("unrecognized database type: %s", dbType)
			}
		}
		p := statediff.Config{
			IndexerConfig:     indexerConfig,
			KnownGapsFilePath: ctx.String(utils.StateDiffKnownGapsFilePath.Name),
			ID:                nodeID,
			ClientName:        clientName,
			Context:           context.Background(),
			EnableWriteLoop:   ctx.Bool(utils.StateDiffWritingFlag.Name),
			NumWorkers:        ctx.Uint(utils.StateDiffWorkersFlag.Name),
			WaitForSync:       ctx.Bool(utils.StateDiffWaitForSync.Name),
		}
		utils.RegisterStateDiffService(stack, eth, &cfg.Eth, p, backend)
	}

	// Configure GraphQL if requested
	if ctx.IsSet(utils.GraphQLEnabledFlag.Name) {
		utils.RegisterGraphQLService(stack, backend, cfg.Node)
	}
	// Add the Ethereum Stats daemon if requested.
	if cfg.Ethstats.URL != "" {
		utils.RegisterEthStatsService(stack, backend, cfg.Ethstats.URL)
	}
	return stack, backend
}

// dumpConfig is the dumpconfig command.
func dumpConfig(ctx *cli.Context) error {
	_, cfg := makeConfigNode(ctx)
	comment := ""

	if cfg.Eth.Genesis != nil {
		cfg.Eth.Genesis = nil
		comment += "# Note: this config doesn't contain the genesis block.\n\n"
	}

	out, err := tomlSettings.Marshal(&cfg)
	if err != nil {
		return err
	}

	dump := os.Stdout
	if ctx.NArg() > 0 {
		dump, err = os.OpenFile(ctx.Args().Get(0), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer dump.Close()
	}
	dump.WriteString(comment)
	dump.Write(out)

	return nil
}

func applyMetricConfig(ctx *cli.Context, cfg *gethConfig) {
	if ctx.IsSet(utils.MetricsEnabledFlag.Name) {
		cfg.Metrics.Enabled = ctx.Bool(utils.MetricsEnabledFlag.Name)
	}
	if ctx.IsSet(utils.MetricsEnabledExpensiveFlag.Name) {
		cfg.Metrics.EnabledExpensive = ctx.Bool(utils.MetricsEnabledExpensiveFlag.Name)
	}
	if ctx.IsSet(utils.MetricsHTTPFlag.Name) {
		cfg.Metrics.HTTP = ctx.String(utils.MetricsHTTPFlag.Name)
	}
	if ctx.IsSet(utils.MetricsPortFlag.Name) {
		cfg.Metrics.Port = ctx.Int(utils.MetricsPortFlag.Name)
	}
	if ctx.IsSet(utils.MetricsEnableInfluxDBFlag.Name) {
		cfg.Metrics.EnableInfluxDB = ctx.Bool(utils.MetricsEnableInfluxDBFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBEndpointFlag.Name) {
		cfg.Metrics.InfluxDBEndpoint = ctx.String(utils.MetricsInfluxDBEndpointFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBDatabaseFlag.Name) {
		cfg.Metrics.InfluxDBDatabase = ctx.String(utils.MetricsInfluxDBDatabaseFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBUsernameFlag.Name) {
		cfg.Metrics.InfluxDBUsername = ctx.String(utils.MetricsInfluxDBUsernameFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBPasswordFlag.Name) {
		cfg.Metrics.InfluxDBPassword = ctx.String(utils.MetricsInfluxDBPasswordFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBTagsFlag.Name) {
		cfg.Metrics.InfluxDBTags = ctx.String(utils.MetricsInfluxDBTagsFlag.Name)
	}
	if ctx.IsSet(utils.MetricsEnableInfluxDBV2Flag.Name) {
		cfg.Metrics.EnableInfluxDBV2 = ctx.Bool(utils.MetricsEnableInfluxDBV2Flag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBTokenFlag.Name) {
		cfg.Metrics.InfluxDBToken = ctx.String(utils.MetricsInfluxDBTokenFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBBucketFlag.Name) {
		cfg.Metrics.InfluxDBBucket = ctx.String(utils.MetricsInfluxDBBucketFlag.Name)
	}
	if ctx.IsSet(utils.MetricsInfluxDBOrganizationFlag.Name) {
		cfg.Metrics.InfluxDBOrganization = ctx.String(utils.MetricsInfluxDBOrganizationFlag.Name)
	}
}

func deprecated(field string) bool {
	switch field {
	case "ethconfig.Config.EVMInterpreter":
		return true
	case "ethconfig.Config.EWASMInterpreter":
		return true
	default:
		return false
	}
}

func setAccountManagerBackends(stack *node.Node) error {
	conf := stack.Config()
	am := stack.AccountManager()
	keydir := stack.KeyStoreDir()
	scryptN := keystore.StandardScryptN
	scryptP := keystore.StandardScryptP
	if conf.UseLightweightKDF {
		scryptN = keystore.LightScryptN
		scryptP = keystore.LightScryptP
	}

	// Assemble the supported backends
	if len(conf.ExternalSigner) > 0 {
		log.Info("Using external signer", "url", conf.ExternalSigner)
		if extapi, err := external.NewExternalBackend(conf.ExternalSigner); err == nil {
			am.AddBackend(extapi)
			return nil
		} else {
			return fmt.Errorf("error connecting to external signer: %v", err)
		}
	}

	// For now, we're using EITHER external signer OR local signers.
	// If/when we implement some form of lockfile for USB and keystore wallets,
	// we can have both, but it's very confusing for the user to see the same
	// accounts in both externally and locally, plus very racey.
	am.AddBackend(keystore.NewKeyStore(keydir, scryptN, scryptP))
	if conf.USB {
		// Start a USB hub for Ledger hardware wallets
		if ledgerhub, err := usbwallet.NewLedgerHub(); err != nil {
			log.Warn(fmt.Sprintf("Failed to start Ledger hub, disabling: %v", err))
		} else {
			am.AddBackend(ledgerhub)
		}
		// Start a USB hub for Trezor hardware wallets (HID version)
		if trezorhub, err := usbwallet.NewTrezorHubWithHID(); err != nil {
			log.Warn(fmt.Sprintf("Failed to start HID Trezor hub, disabling: %v", err))
		} else {
			am.AddBackend(trezorhub)
		}
		// Start a USB hub for Trezor hardware wallets (WebUSB version)
		if trezorhub, err := usbwallet.NewTrezorHubWithWebUSB(); err != nil {
			log.Warn(fmt.Sprintf("Failed to start WebUSB Trezor hub, disabling: %v", err))
		} else {
			am.AddBackend(trezorhub)
		}
	}
	if len(conf.SmartCardDaemonPath) > 0 {
		// Start a smart card hub
		if schub, err := scwallet.NewHub(conf.SmartCardDaemonPath, scwallet.Scheme, keydir); err != nil {
			log.Warn(fmt.Sprintf("Failed to start smart card hub, disabling: %v", err))
		} else {
			am.AddBackend(schub)
		}
	}

	return nil
}
