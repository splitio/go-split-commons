package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/push"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	// Ready represents ready
	Ready = iota
	// StreamingReady ready
	StreamingReady
	// Error represents some error in SSE streaming
	Error
)

// Manager struct
type Manager struct {
	synchronizer    Synchronizer
	logger          logging.LoggerInterface
	config          conf.AdvancedConfig
	authClient      service.AuthClient
	pushManager     *push.PushManager
	managerStatus   chan int
	streamingStatus chan int
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	synchronizer Synchronizer,
	logger logging.LoggerInterface,
	statusChannel chan int,
	config conf.AdvancedConfig,
	authClient service.AuthClient,
	splitStorage storage.SplitStorage,
	managerStatus chan int,
) (*Manager, error) {
	if statusChannel == nil || cap(statusChannel) < 1 {
		return nil, errors.New("Status channel cannot be nil nor having capacity")
	}
	streamingStatus := make(chan int, 1)
	pushManager, err := push.NewPushManager(logger, synchronizer.SynchronizeSegment, synchronizer.SynchronizeSplits, splitStorage, &config, streamingStatus)
	if err != nil {
		return nil, err
	}
	return &Manager{
		synchronizer:    synchronizer,
		logger:          logger,
		streamingStatus: streamingStatus,
		config:          config,
		authClient:      authClient,
		pushManager:     pushManager,
		managerStatus:   managerStatus,
	}, nil
}

// Start starts synchronization through Split
func (s *Manager) Start() {
	token, err := s.authClient.Authenticate()
	if err != nil {
		s.managerStatus <- Error
		return
	}

	err = s.synchronizer.SyncAll()
	if err != nil {
		s.managerStatus <- Error
		return
	}
	s.synchronizer.StartPeriodicDataRecording()
	s.managerStatus <- Ready

	if s.config.StreamingEnabled && token.PushEnabled {
		channels, err := token.ChannelList()
		if err == nil {
			s.logger.Info("Start Streaming")
			s.pushManager.Start(token.Token, channels)
			for {
				status := <-s.streamingStatus
				switch status {
				case push.Ready:
					s.logger.Info("SSE Streaming is ready")
					s.managerStatus <- StreamingReady
				case push.Error:
					s.pushManager.Stop()
					s.logger.Info("Start periodic polling due error in Streaming")
					s.synchronizer.StartPeriodicFetching()
					return
				}
			}
		}
	}

	s.logger.Info("Start periodic polling")
	s.synchronizer.StartPeriodicFetching()
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
