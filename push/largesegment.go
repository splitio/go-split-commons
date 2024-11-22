package push

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

type LargeSegmentUpdateWorker struct {
	lsQueue   chan dtos.LargeSegmentChangeUpdate
	sync      synchronizerInterface
	logger    logging.LoggerInterface
	lifecycle lifecycle.Manager
}

func NewLargeSegmentUpdateWorker(
	lsQueue chan dtos.LargeSegmentChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*LargeSegmentUpdateWorker, error) {
	if cap(lsQueue) < 5000 {
		return nil, errors.New("largeSegmentQueue capacity must be larger")
	}

	running := atomic.Value{}
	running.Store(false)

	worker := &LargeSegmentUpdateWorker{
		lsQueue: lsQueue,
		sync:    synchronizer,
		logger:  logger,
	}
	worker.lifecycle.Setup()
	return worker, nil
}

// Start starts worker
func (s *LargeSegmentUpdateWorker) Start() {
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Large Segment worker is already running")
		return
	}

	go func() {
		if !s.lifecycle.InitializationComplete() {
			return
		}
		defer s.lifecycle.ShutdownComplete()
		for {
			select {
			case lstUpdate := <-s.lsQueue:
				s.logger.Debug("Received Large Segment updates and proceding to perform fetch")
				for _, ls := range lstUpdate.LargeSegments {
					s.logger.Debug(fmt.Sprintf("LargeSegmentName: %s\nChangeNumber: %d", ls.Name, lstUpdate.ChangeNumber()))
					ls.ChangeNumber = lstUpdate.ChangeNumber()
					err := s.sync.SynchronizeLargeSegmentUpdate(&ls)
					if err != nil {
						s.logger.Error(err)
					}
				}
			case <-s.lifecycle.ShutdownRequested():
				return
			}
		}
	}()
}

// Stop stops worker
func (s *LargeSegmentUpdateWorker) Stop() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Debug("Large Segment worker not runnning. Ignoring.")
		return
	}
	s.lifecycle.AwaitShutdownComplete()
}

// IsRunning indicates if worker is running or not
func (s *LargeSegmentUpdateWorker) IsRunning() bool {
	return s.lifecycle.IsRunning()
}
