package synchronizer

import (
	"errors"
	"sync/atomic"

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

const (
	// Created flags
	Created = iota
	// Starting flags
	Starting
	// Streaming flags
	Streaming
	// Polling flags
	Polling
)

// Manager struct
type Manager struct {
	synchronizer    Synchronizer
	logger          logging.LoggerInterface
	config          conf.AdvancedConfig
	pushManager     *push.PushManager
	managerStatus   chan<- int
	streamingStatus chan int
	status          atomic.Value
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	synchronizer Synchronizer,
	logger logging.LoggerInterface,
	config conf.AdvancedConfig,
	authClient service.AuthClient,
	splitStorage storage.SplitStorage,
	managerStatus chan int,
) (*Manager, error) {
	if managerStatus == nil || cap(managerStatus) < 1 {
		return nil, errors.New("Status channel cannot be nil nor having capacity")
	}
	streamingStatus := make(chan int, 1000)
	pushManager, err := push.NewPushManager(logger, synchronizer.SynchronizeSegment, synchronizer.SynchronizeSplits, splitStorage, &config, streamingStatus, authClient)
	if err != nil {
		return nil, err
	}
	status := atomic.Value{}
	status.Store(Created)
	return &Manager{
		synchronizer:    synchronizer,
		logger:          logger,
		streamingStatus: streamingStatus,
		config:          config,
		pushManager:     pushManager,
		managerStatus:   managerStatus,
		status:          status,
	}, nil
}

func (s *Manager) startPolling() {
	s.pushManager.StopWorkers()
	s.synchronizer.StartPeriodicFetching()
	s.status.Store(Polling)
}

// Start starts synchronization through Split
func (s *Manager) Start() {
	err := s.synchronizer.SyncAll()
	if err != nil {
		s.managerStatus <- Error
		return
	}
	s.synchronizer.StartPeriodicDataRecording()
	s.managerStatus <- Ready

	if s.config.StreamingEnabled {
		s.logger.Info("Start Streaming")
		go s.pushManager.Start()
		// Listens Streaming Status
		for {
			status := <-s.streamingStatus
			switch status {
			// Backoff is running -> start polling until auth is ok
			case push.BackoffAuth:
				fallthrough
			case push.BackoffSSE:
				if s.status.Load().(int) != Polling {
					s.logger.Info("Start periodic polling due backoff")
					s.startPolling()
				}
			// SSE Streaming and workers are ready
			case push.Ready:
				// If Ready comes eventually when Backoff is done and polling is running
				if s.status.Load().(int) == Polling {
					s.synchronizer.StopPeriodicFetching()
				}
				s.logger.Info("SSE Streaming is ready")
				s.managerStatus <- StreamingReady
				s.status.Store(Streaming)
				go s.synchronizer.SyncAll()
			// Error occurs and it will switch to polling
			case push.Error:
				s.pushManager.Stop()
				s.logger.Info("Start periodic polling due error in Streaming")
				s.startPolling()
				return
			// Publisher sends that there is no Notification Managers available
			case push.PushIsDown:
				// If streaming is already running, proceeding to stop workers
				// and keeping SSE running
				if s.status.Load().(int) == Streaming {
					s.logger.Info("Start periodic polling due error in Streaming")
					s.startPolling()
				}
			// Publisher sends that there are at least one Notification Manager available
			case push.PushIsUp:
				// If streaming is not already running, proceeding to start workers
				if s.status.Load().(int) != Streaming {
					s.logger.Info("Stop periodic polling due Publishers Available")
					s.pushManager.StartWorkers()
					s.synchronizer.StopPeriodicFetching()
					s.status.Store(Streaming)
					go s.synchronizer.SyncAll()
				}
			case push.TokenExpiration:
				go s.pushManager.Start()
			}
		}
	} else {
		s.logger.Info("Start periodic polling")
		s.startPolling()
	}
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
