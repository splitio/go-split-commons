package synchronizer

import (
	"github.com/splitio/go-toolkit/logging"
)

// Manager struct
type Manager struct {
	synchronizer  Synchronizer
	logger        logging.LoggerInterface
	statusChannel chan string
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	sinchronizer Synchronizer,
	logger logging.LoggerInterface,
	statusChannel chan string,
) *Manager {
	return &Manager{
		synchronizer:  sinchronizer,
		logger:        logger,
		statusChannel: statusChannel,
	}
}

func (s *Manager) startPolling() {
	s.synchronizer.StartPeriodicFetching()
}

// Start starts synchronization through Split
func (s *Manager) Start() error {
	err := s.synchronizer.SyncAll()
	if err != nil {
		s.statusChannel <- "ERROR"
		return err
	}
	s.synchronizer.StartPeriodicDataRecording()
	s.statusChannel <- "READY"
	s.startPolling()
	return nil
}

// Stop stop synchronizaation through Split
func (s *Manager) Stop() {
	s.logger.Debug("STOPPING PERIODIC TASKS")
	s.synchronizer.StopPeriodicFetching()
	s.synchronizer.StopPeriodicDataRecording()
}
