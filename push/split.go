package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	splitQueue chan dtos.SplitChangeUpdate
	sync       synchronizerInterface
	logger     logging.LoggerInterface
	lifecycle  lifecycle.Manager
}

// NewSplitUpdateWorker creates SplitUpdateWorker
func NewSplitUpdateWorker(
	splitQueue chan dtos.SplitChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*SplitUpdateWorker, error) {
	if cap(splitQueue) < 5000 {
		return nil, errors.New("")
	}

	worker := &SplitUpdateWorker{
		splitQueue: splitQueue,
		sync:       synchronizer,
		logger:     logger,
	}
	worker.lifecycle.Setup()
	return worker, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Split worker is already running")
		return
	}

	s.logger.Debug("Started SplitUpdateWorker")
	go func() {
		defer s.lifecycle.ShutdownComplete()
		if !s.lifecycle.InitializationComplete() {
			return
		}
		for {
			select {
			case splitUpdate := <-s.splitQueue:
				s.logger.Debug("Received Split update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", splitUpdate.ChangeNumber()))
				err := s.sync.SynchronizeFeatureFlags(&splitUpdate)
				if err != nil {
					s.logger.Error(err)
				}
			case <-s.lifecycle.ShutdownRequested():
				return
			}
		}
	}()
}

// Stop stops worker
func (s *SplitUpdateWorker) Stop() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Debug("Split worker not runnning. Ignoring.")
		return
	}
	s.lifecycle.AwaitShutdownComplete()
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	return s.lifecycle.IsRunning()
}
