package synchronizer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-split-commons/v2/push"
	"github.com/splitio/go-split-commons/v2/service"
	"github.com/splitio/go-split-commons/v2/storage"
	"github.com/splitio/go-toolkit/v3/backoff"
	"github.com/splitio/go-toolkit/v3/logging"
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
	// Idle flags
	Idle = iota
	// Streaming flags
	Streaming
	// Polling flags
	Polling
)

// Manager interface
type Manager interface {
	Start()
	Stop()
	IsRunning() bool
}

// ManagerImpl struct
type ManagerImpl struct {
	synchronizer    Synchronizer
	logger          logging.LoggerInterface
	config          conf.AdvancedConfig
	pushManager     push.Manager
	managerStatus   chan int
	streamingStatus chan int64
	status          atomic.Value
	backoff         backoff.Interface
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	synchronizer Synchronizer,
	logger logging.LoggerInterface,
	config conf.AdvancedConfig,
	authClient service.AuthClient,
	splitStorage storage.SplitStorage,
	managerStatus chan int,
) (*ManagerImpl, error) {
	if managerStatus == nil || cap(managerStatus) < 1 {
		return nil, errors.New("Status channel cannot be nil nor having capacity")
	}

	status := atomic.Value{}
	status.Store(Idle)
	manager := &ManagerImpl{
		backoff:       backoff.New(),
		synchronizer:  synchronizer,
		logger:        logger,
		config:        config,
		managerStatus: managerStatus,
		status:        status,
	}
	if config.StreamingEnabled {
		streamingStatus := make(chan int64, 1000)
		pushManager, err := push.NewManager(logger, synchronizer, &config, streamingStatus, authClient)
		if err != nil {
			return nil, err
		}
		manager.pushManager = pushManager
		manager.streamingStatus = streamingStatus
	}

	return manager, nil
}

func (s *ManagerImpl) startPolling() {
	s.status.Store(Polling)
	s.pushManager.StopWorkers()
	s.synchronizer.StartPeriodicFetching()
}

// IsRunning returns true if is in Streaming or Polling
func (s *ManagerImpl) IsRunning() bool {
	return s.status.Load().(int) != Idle
}

// Start starts synchronization through Split
func (s *ManagerImpl) Start() {
	if s.IsRunning() {
		s.logger.Info("Manager is already running, skipping start")
		return
	}
	select {
	case <-s.managerStatus:
		// Discarding previous status before starting
	default:
	}
	err := s.synchronizer.SyncAll()
	if err != nil {
		s.managerStatus <- Error
		return
	}
	s.logger.Debug("SyncAll Ready")
	s.managerStatus <- Ready
	s.synchronizer.StartPeriodicDataRecording()
	if !s.config.StreamingEnabled {
		s.logger.Info("Start periodic polling")
		s.synchronizer.StartPeriodicFetching()
		s.status.Store(Polling)
		return
	}

	// Start streaming
	s.logger.Info("Starting Streaming")
	s.pushManager.Start()
	// Listens Streaming Status
	for {
		status := <-s.streamingStatus
		fmt.Println("status a push manager", status)

		switch status {
		case push.StatusUp:
			s.synchronizer.StopPeriodicFetching()
			s.synchronizer.SyncAll()
			s.pushManager.StartWorkers()
			s.status.Store(Streaming)
			s.backoff.Reset()
			// TODO: Log
		case push.StatusDown:
			s.synchronizer.SyncAll()
			s.startPolling()
			// TODO: Log
		case push.StatusRetryableError:
			s.pushManager.Stop()
			s.synchronizer.SyncAll()
			s.startPolling()
			time.Sleep(s.backoff.Next())
			// TODO: Log
			s.pushManager.Start()
		case push.StatusNonRetryableError:
			s.pushManager.StopWorkers()
			s.pushManager.Stop()
			s.synchronizer.SyncAll()
			s.synchronizer.StartPeriodicFetching()
			// TODO: log
		}
	}
}

// Stop stop synchronizaation through Split
func (s *ManagerImpl) Stop() {
	s.logger.Info("STOPPING MANAGER TASKS")
	// TODO
	/*
		if s.pushManager != nil && s.pushManager.IsRunning() {
			s.pushManager.Stop()
		}
	*/
	s.synchronizer.StopPeriodicFetching()
	s.synchronizer.StopPeriodicDataRecording()
	s.status.Store(Idle)
}
