package push

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

// SegmentUpdateWorker struct
type SegmentUpdateWorker struct {
	activeGoroutines  *sync.WaitGroup
	segmentQueue      chan dtos.SegmentChangeNotification
	handler           func(segmentName string, till *int64) error
	shouldKeepRunning int64
	logger            logging.LoggerInterface
	stop              chan struct{}
}

// NewSegmentUpdateWorker creates SegmentUpdateWorker
func NewSegmentUpdateWorker(segmentQueue chan dtos.SegmentChangeNotification, handler func(segmentName string, till *int64) error, logger logging.LoggerInterface) (*SegmentUpdateWorker, error) {
	if cap(segmentQueue) < 5000 {
		return nil, errors.New("")
	}

	return &SegmentUpdateWorker{
		activeGoroutines: &sync.WaitGroup{},
		segmentQueue:     segmentQueue,
		handler:          handler,
		logger:           logger,
		stop:             make(chan struct{}, 1),
	}, nil
}

// Start starts worker
func (s *SegmentUpdateWorker) Start() {
	s.logger.Info("Started SegmentUpdateWorker")
	if s.IsRunning() {
		s.logger.Info("Segment worker is already running")
		return
	}
	s.activeGoroutines.Add(1)
	atomic.StoreInt64(&s.shouldKeepRunning, 1)
	go func() {
		defer s.activeGoroutines.Done()
		for atomic.LoadInt64(&s.shouldKeepRunning) == 1 {
			select {
			case segmentUpdate := <-s.segmentQueue:
				s.logger.Debug("Received Segment update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("SegmentName: %s\nChangeNumber: %d", segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber))
				err := s.handler(segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber)
				if err != nil {
					s.logger.Error(err)
				}
			case <-s.stop:
				break
			}
		}
	}()
}

// Stop stops worker
func (s *SegmentUpdateWorker) Stop() {
	atomic.StoreInt64(&s.shouldKeepRunning, 0)
	s.stop <- struct{}{}
	s.activeGoroutines.Wait()
}

// IsRunning indicates if worker is running or not
func (s *SegmentUpdateWorker) IsRunning() bool {
	return atomic.LoadInt64(&s.shouldKeepRunning) == 1
}
