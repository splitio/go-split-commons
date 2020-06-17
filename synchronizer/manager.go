package synchronizer

import (
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
) *Manager {
	return &Manager{
		synchronizer:  synchronizer,
		logger:        logger,
		statusChannel: statusChannel,
		config:        config,
		authClient:    authClient,
		pushManager:   push.NewPushManager(logger, synchronizer.SynchronizeSegment, synchronizer.SynchronizeSplits, splitStorage, &config),
	}
}

func (s *Manager) startPolling() {
	s.synchronizer.StartPeriodicFetching()
}

// Start starts synchronization through Split
func (s *Manager) Start() error {
	token, err := s.authClient.Authenticate()
	if err != nil {
		return err
	}

	go func() {
		err = s.synchronizer.SyncAll()
		if err != nil {
			s.statusChannel <- "ERROR"
		}
		s.synchronizer.StartPeriodicDataRecording()
		s.statusChannel <- "READY"
	}()

	if s.config.StreamingEnabled && token.PushEnabled {
		s.logger.Info("Start Streaming")
		channels, err := token.ChannelList()
		if err == nil {
			s.pushManager.Start(token.Token, channels)
			return nil
		}
	}

	s.logger.Info("Start periodic polling")
	s.startPolling()
	return nil
}

// Stop stop synchronizaation through Split
func (s *Manager) Stop() {
	s.logger.Debug("STOPPING PERIODIC TASKS")
	s.synchronizer.StopPeriodicFetching()
	s.synchronizer.StopPeriodicDataRecording()
}
