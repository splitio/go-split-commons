package push

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/splitio/go-toolkit/v3/common"
	"github.com/splitio/go-toolkit/v3/logging"
)

// SegmentUpdateWorker struct
type SegmentUpdateWorker struct {
	segmentQueue chan SegmentChangeUpdate
	sync         synchronizerInterface
	logger       logging.LoggerInterface
	stop         chan struct{}
	stopped      chan struct{}
	status       int32
}

// NewSegmentUpdateWorker creates SegmentUpdateWorker
func NewSegmentUpdateWorker(
	segmentQueue chan SegmentChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*SegmentUpdateWorker, error) {
	if cap(segmentQueue) < 5000 {
		return nil, errors.New("")
	}
	running := atomic.Value{}
	running.Store(false)

	return &SegmentUpdateWorker{
		segmentQueue: segmentQueue,
		sync:         synchronizer,
		logger:       logger,
		stop:         make(chan struct{}, 1),
		stopped:      make(chan struct{}, 1),
	}, nil
}

// Start starts worker
func (s *SegmentUpdateWorker) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, workerStatusIdle, workerStatusRunning) {
		s.logger.Info("Segment worker is already running")
		return
	}

	go func() {
		defer func() { s.stopped <- struct{}{} }()
		defer atomic.StoreInt32(&s.status, workerStatusIdle)
		for {
			select {
			case segmentUpdate := <-s.segmentQueue:
				s.logger.Debug("Received Segment update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("SegmentName: %s\nChangeNumber: %d", segmentUpdate.SegmentName(), segmentUpdate.ChangeNumber()))
				err := s.sync.SynchronizeSegment(segmentUpdate.SegmentName(), common.Int64Ref(segmentUpdate.ChangeNumber()))
				if err != nil {
					s.logger.Error(err)
				}
			case <-s.stop:
				return
			}
		}
	}()
}

// Stop stops worker
func (s *SegmentUpdateWorker) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, workerStatusRunning, workerStatusShuttingDown) {
		s.logger.Debug("Split worker not runnning. Ignoring.")
		return
	}
	s.stop <- struct{}{}
	<-s.stopped
}

// IsRunning indicates if worker is running or not
func (s *SegmentUpdateWorker) IsRunning() bool {
	return atomic.LoadInt32(&s.status) == workerStatusRunning
}
