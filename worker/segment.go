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

// SegmentUpdateWorker struct
type SegmentUpdateWorker struct {
	activeGoroutines    *sync.WaitGroup
	segmentQueue        chan dtos.SegmentChangeNotification
	segmentSynchronizer synchronizer.Synchronizer
	shouldKeepRunning   int64
	logger              logging.LoggerInterface
}

// NewSegmentUpdateWorker creates SegmentUpdateWorker
func NewSegmentUpdateWorker(segmentQueue chan dtos.SegmentChangeNotification, segmentSynchronizer synchronizer.Synchronizer, logger logging.LoggerInterface) (*SegmentUpdateWorker, error) {
	if cap(segmentQueue) < 5000 {
		return nil, errors.New("")
	}

	return &SegmentUpdateWorker{
		activeGoroutines:    &sync.WaitGroup{},
		segmentQueue:        segmentQueue,
		segmentSynchronizer: segmentSynchronizer,
		logger:              logger,
	}, nil
}

// Start starts worker
func (s *SegmentUpdateWorker) Start() {
	if s.IsRunning() {
		s.logger.Info("Segment worker is already running")
		return
	}
	s.activeGoroutines.Add(1)
	atomic.StoreInt64(&s.shouldKeepRunning, 1)
	go func() {
		defer s.activeGoroutines.Done()
		for atomic.LoadInt64(&s.shouldKeepRunning) == 1 {
			segmentUpdate := <-s.segmentQueue
			s.logger.Debug("Received Segment update and proceding to perform fetch")
			s.logger.Debug(fmt.Sprintf("SegmentName: %s\nChangeNumber: %d", segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber))
			err := s.segmentSynchronizer.SynchronizeSegment(segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber)
			if err != nil {
				s.logger.Error(err)
			}
		}
	}()
}

// Stop stops worker
func (s *SegmentUpdateWorker) Stop() {
	atomic.StoreInt64(&s.shouldKeepRunning, 0)
	s.activeGoroutines.Wait()
}

// IsRunning indicates if worker is running or not
func (s *SegmentUpdateWorker) IsRunning() bool {
	return atomic.LoadInt64(&s.shouldKeepRunning) == 1
}
