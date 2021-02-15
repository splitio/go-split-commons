package push

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/splitio/go-toolkit/v4/common"
	"github.com/splitio/go-toolkit/v4/logging"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	splitQueue chan SplitChangeUpdate
	sync       synchronizerInterface
	logger     logging.LoggerInterface
	stop       chan struct{}
	stopped    chan struct{}
	status     int32
}

// NewSplitUpdateWorker creates SplitUpdateWorker
func NewSplitUpdateWorker(
	splitQueue chan SplitChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*SplitUpdateWorker, error) {
	if cap(splitQueue) < 5000 {
		return nil, errors.New("")
	}

	return &SplitUpdateWorker{
		splitQueue: splitQueue,
		sync:       synchronizer,
		logger:     logger,
		stop:       make(chan struct{}, 1),
		stopped:    make(chan struct{}, 1),
	}, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, workerStatusIdle, workerStatusRunning) {
		s.logger.Info("Split worker is already running")
		return
	}

	s.logger.Debug("Started SplitUpdateWorker")
	go func() {
		defer func() { s.stopped <- struct{}{} }()
		defer atomic.StoreInt32(&s.status, workerStatusIdle)
		for {
			select {
			case splitUpdate := <-s.splitQueue:
				s.logger.Debug("Received Split update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", splitUpdate.ChangeNumber()))
				err := s.sync.SynchronizeSplits(common.Int64Ref(splitUpdate.ChangeNumber()))
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
func (s *SplitUpdateWorker) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, workerStatusRunning, workerStatusShuttingDown) {
		s.logger.Debug("Split worker not runnning. Ignoring.")
		return
	}
	s.stop <- struct{}{}
	<-s.stopped
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	return atomic.LoadInt32(&s.status) == workerStatusRunning
}
