package worker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/logging"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	activeGoroutines  *sync.WaitGroup
	splitQueue        chan dtos.SplitChangeNotification
	splitSynchronizer synchronizer.Synchronizer
	shouldKeepRunning int64
	logger            logging.LoggerInterface
}

// NewSplitUpdateWorker creates SplitUpdateWorker
func NewSplitUpdateWorker(splitQueue chan dtos.SplitChangeNotification, splitSynchronizer synchronizer.Synchronizer, logger logging.LoggerInterface) (*SplitUpdateWorker, error) {
	if cap(splitQueue) < 5000 {
		return nil, errors.New("")
	}

	return &SplitUpdateWorker{
		activeGoroutines:  &sync.WaitGroup{},
		splitQueue:        splitQueue,
		splitSynchronizer: splitSynchronizer,
		logger:            logger,
	}, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	if s.IsRunning() {
		s.logger.Info("Split worker is already running")
		return
	}
	s.activeGoroutines.Add(1)
	atomic.StoreInt64(&s.shouldKeepRunning, 1)
	go func() {
		defer s.activeGoroutines.Done()
		s.logger.Debug(atomic.LoadInt64(&s.shouldKeepRunning))
		s.logger.Debug(atomic.LoadInt64(&s.shouldKeepRunning) == 1)
		for atomic.LoadInt64(&s.shouldKeepRunning) == 1 {
			splitUpdate := <-s.splitQueue
			s.logger.Debug("Received Split update and proceding to perform fetch")
			s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", &splitUpdate.ChangeNumber))
			err := s.splitSynchronizer.SynchronizeSplits(&splitUpdate.ChangeNumber)
			if err != nil {
				s.logger.Error(err)
			}
		}
	}()
}

// Stop stops worker
func (s *SplitUpdateWorker) Stop() {
	atomic.StoreInt64(&s.shouldKeepRunning, 0)
	s.activeGoroutines.Wait()
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	return atomic.LoadInt64(&s.shouldKeepRunning) == 1
}
