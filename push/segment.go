package push

import (
	"errors"
	"fmt"
	"sync"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

// SegmentUpdateWorker struct
type SegmentUpdateWorker struct {
	activeGoroutines *sync.WaitGroup
	segmentQueue     chan dtos.SegmentChangeNotification
	handler          func(segmentName string, till *int64) error
	logger           logging.LoggerInterface
	stop             chan struct{}
	running          bool
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
		running:          false,
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
	s.running = true
	go func() {
		defer s.activeGoroutines.Done()
		for {
			select {
			case segmentUpdate := <-s.segmentQueue:
				s.logger.Debug("Received Segment update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("SegmentName: %s\nChangeNumber: %d", segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber))
				err := s.handler(segmentUpdate.SegmentName, &segmentUpdate.ChangeNumber)
				if err != nil {
					s.logger.Error(err)
				}
			case <-s.stop:
				return
			}
			s.activeGoroutines.Wait()
		}
	}()
}

// Stop stops worker
func (s *SegmentUpdateWorker) Stop() {
	s.stop <- struct{}{}
	s.running = false
}

// IsRunning indicates if worker is running or not
func (s *SegmentUpdateWorker) IsRunning() bool {
	return s.running
}
