package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/push"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// Manager struct
type Manager struct {
	synchronizer  Synchronizer
	logger        logging.LoggerInterface
	statusChannel chan string
	config        conf.AdvancedConfig
	authClient    service.AuthClient
	pushManager   *push.PushManager
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	synchronizer Synchronizer,
	logger logging.LoggerInterface,
	statusChannel chan string,
	config conf.AdvancedConfig,
	authClient service.AuthClient,
	splitStorage storage.SplitStorage,
) (*Manager, error) {
	if statusChannel == nil || cap(statusChannel) < 1 {
		return nil, errors.New("Status channel cannot be nil nor having capacity")
	}
	pushManager, err := push.NewPushManager(logger, synchronizer.SynchronizeSegment, synchronizer.SynchronizeSplits, splitStorage, &config)
	if err != nil {
		return nil, err
	}
	return &Manager{
		synchronizer:  synchronizer,
		logger:        logger,
		statusChannel: statusChannel,
		config:        config,
		authClient:    authClient,
		pushManager:   pushManager,
	}, nil
}

// Start starts synchronization through Split
func (s *Manager) Start() error {
	token, err := s.authClient.Authenticate()
	if err != nil {
		return err
	}

	err = s.synchronizer.SyncAll()
	if err != nil {
		s.statusChannel <- "ERROR"
	}
	s.synchronizer.StartPeriodicDataRecording()
	s.statusChannel <- "READY"

	if s.config.StreamingEnabled && token.PushEnabled {
		channels, err := token.ChannelList()
		if err == nil {
			s.logger.Info("Start Streaming")
			err := s.pushManager.Start(token.Token, channels)
			if err == nil {
				return nil
			}
		}
	}

	s.logger.Info("Start periodic polling")
	s.synchronizer.StartPeriodicFetching()
	return nil
}

// Stop stop synchronizaation through Split
func (s *Manager) Stop() {
	s.logger.Info("STOPPING MANAGER TASKS")
	if s.pushManager.IsRunning() {
		s.pushManager.Stop()
	}
	s.synchronizer.StopPeriodicFetching()
	s.synchronizer.StopPeriodicDataRecording()
}
