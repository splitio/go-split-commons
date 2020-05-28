package sync

import (
	"github.com/splitio/go-toolkit/logging"
)

// SynchronizerManager struct
type SynchronizerManager struct {
	synchronizer  Synchronizer
	logger        logging.LoggerInterface
	statusChannel chan string
}

// NewSynchronizerManager creates new sync manager
func NewSynchronizerManager(
	sinchronizer Synchronizer,
	logger logging.LoggerInterface,
	statusChannel chan string,
) *SynchronizerManager {
	return &SynchronizerManager{
		synchronizer:  sinchronizer,
		logger:        logger,
		statusChannel: statusChannel,
	}
}

func (s *SynchronizerManager) startPolling() {
	s.synchronizer.StartPeriodicFetching()
}

// Start starts synchronization through Split
func (s *SynchronizerManager) Start() error {
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
func (s *SynchronizerManager) Stop() {
	s.logger.Debug("STOPPING PERIODIC TASKS")
	s.synchronizer.StopPeriodicFetching()
	s.synchronizer.StopPeriodicDataRecording()
}
