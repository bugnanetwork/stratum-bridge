package bugnastratum

import (
	"context"
	"fmt"
	"time"

	"github.com/bugnanetwork/bugna-stratum-bridge/src/gostratum"
	"github.com/bugnanetwork/bugnad/app/appmessage"
	"github.com/bugnanetwork/bugnad/infrastructure/network/rpcclient"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type BugnaApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	bugnad        *rpcclient.RPCClient
	connected     bool
}

func NewBugnaAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger) (*BugnaApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &BugnaApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "bugnaapi:"+address)),
		bugnad:        client,
		connected:     true,
	}, nil
}

func (ks *BugnaApi) Start(ctx context.Context, blockCb func()) {
	ks.waitForSync(true)
	go ks.startBlockTemplateListener(ctx, blockCb)
	go ks.startStatsThread(ctx)
}

func (ks *BugnaApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ks.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := ks.bugnad.GetBlockDAGInfo()
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from bugna, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := ks.bugnad.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from bugna, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (ks *BugnaApi) reconnect() error {
	if ks.bugnad != nil {
		return ks.bugnad.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(ks.address)
	if err != nil {
		return err
	}
	ks.bugnad = client
	return nil
}

func (s *BugnaApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking bugnad sync state")
	}
	for {
		clientInfo, err := s.bugnad.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from bugnad @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("Bugna is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("bugnad synced, starting server")
	}
	return nil
}

func (s *BugnaApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := s.bugnad.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		blockReadyChan <- true
	})
	if err != nil {
		s.logger.Error("fatal: failed to register for block notifications from bugna")
	}

	ticker := time.NewTicker(s.blockWaitTime)
	for {
		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking bugnad sync state, attempting reconnect: ", err)
			if err := s.reconnect(); err != nil {
				s.logger.Error("error reconnecting to bugnad, waiting before retry: ", err)
				time.Sleep(5 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (ks *BugnaApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	template, err := ks.bugnad.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`'%s' via onemorebsmith/bugna-stratum-bridge_%s`, client.RemoteApp, version))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from bugna")
	}
	return template, nil
}
