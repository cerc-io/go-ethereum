package service

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff/builder"
)

type BlockChain interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	GetBlockByHash(hash common.Hash) *types.Block
	AddToStateDiffProcessedCollection(hash common.Hash)
}

type StateDiffService struct {
	sync.Mutex
	Builder       builder.Builder
	BlockChain    BlockChain
	QuitChan      chan bool
	Subscriptions Subscriptions
}

type Subscriptions map[rpc.ID]subscription

type subscription struct {
	PayloadChan chan<- StateDiffPayload
	QuitChan    chan<- bool
}

type StateDiffPayload struct {
	BlockRlp  []byte            `json:"block"`
	StateDiff builder.StateDiff `json:"state_diff"`
	Err       error             `json:"error"`
}

func NewStateDiffService(db ethdb.Database, blockChain *core.BlockChain) (*StateDiffService, error) {
	return &StateDiffService{
		BlockChain:    blockChain,
		Builder:       builder.NewBuilder(db, blockChain),
		QuitChan:      make(chan bool),
		Subscriptions: make(Subscriptions),
	}, nil
}

func (sds *StateDiffService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the Whisper implementation offers
func (sds *StateDiffService) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: APIName,
			Version:   APIVersion,
			Service:   NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

func (sds *StateDiffService) Loop(chainEventCh chan core.ChainEvent) {
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()

	blocksCh := make(chan *types.Block, 10)
	errCh := chainEventSub.Err()

	go func() {
	HandleChainEventChLoop:
		for {
			select {
			//Notify chain event channel of events
			case chainEvent := <-chainEventCh:
				log.Debug("Event received from chainEventCh", "event", chainEvent)
				blocksCh <- chainEvent.Block
				//if node stopped
			case err := <-errCh:
				log.Warn("Error from chain event subscription, breaking loop.", "error", err)
				close(sds.QuitChan)
				break HandleChainEventChLoop
			case <-sds.QuitChan:
				break HandleChainEventChLoop
			}
		}
	}()

	//loop through chain events until no more
HandleBlockChLoop:
	for {
		select {
		case block := <-blocksCh:
			currentBlock := block
			parentHash := currentBlock.ParentHash()
			parentBlock := sds.BlockChain.GetBlockByHash(parentHash)
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block",
					"parent block hash", parentHash.String(),
					"current block number", currentBlock.Number())
				break HandleBlockChLoop
			}

			stateDiff, err := sds.Builder.BuildStateDiff(parentBlock.Root(), currentBlock.Root(), currentBlock.Number().Int64(), currentBlock.Hash())
			if err != nil {
				log.Error("Error building statediff", "block number", currentBlock.Number(), "error", err)
			}
			rlpBuff := new(bytes.Buffer)
			currentBlock.EncodeRLP(rlpBuff)
			blockRlp := rlpBuff.Bytes()
			payload := StateDiffPayload{
				BlockRlp:  blockRlp,
				StateDiff: *stateDiff,
				Err:       err,
			}
			// If we have any websocket subscription listening in, send the data to them
			sds.Send(payload)
		case <-sds.QuitChan:
			log.Debug("Quitting the statediff block channel")
			sds.Close()
			return
		}
	}
}

func (sds *StateDiffService) Subscribe(id rpc.ID, sub chan<- StateDiffPayload, quitChan chan<- bool) {
	log.Info("Subscribing to the statediff service")
	sds.Lock()
	sds.Subscriptions[id] = subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.Unlock()
}

func (sds *StateDiffService) Unsubscribe(id rpc.ID) error {
	log.Info("Unsubscribing from the statediff service")
	sds.Lock()
	_, ok := sds.Subscriptions[id]
	if !ok {
		return fmt.Errorf("cannot unsubscribe; subscription for id %s does not exist", id)
	}
	delete(sds.Subscriptions, id)
	sds.Unlock()
	return nil
}

func (sds *StateDiffService) Send(payload StateDiffPayload) {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.PayloadChan <- payload:
			log.Info("sending state diff payload to subscription %s", id)
		default:
			log.Info("unable to send payload to subscription %s; channel has no receiver", id)
		}
	}
	sds.Unlock()
}

func (sds *StateDiffService) Close() {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.QuitChan <- true:
			delete(sds.Subscriptions, id)
			log.Info("closing subscription %s", id)
		default:
			log.Info("unable to close subscription %s; channel has no receiver", id)
		}
	}
	sds.Unlock()
}

func (sds *StateDiffService) Start(server *p2p.Server) error {
	log.Info("Starting statediff service")

	chainEventCh := make(chan core.ChainEvent, 10)
	go sds.Loop(chainEventCh)

	return nil
}

func (sds *StateDiffService) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}
