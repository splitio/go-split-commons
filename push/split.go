package push

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/splitio/go-toolkit/v3/common"
	"github.com/splitio/go-toolkit/v3/logging"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	activeGoroutines *sync.WaitGroup
	splitQueue       chan SplitChangeUpdate
	sync             synchronizerInterface
	logger           logging.LoggerInterface
	stop             chan struct{}
	running          atomic.Value
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
	running := atomic.Value{}
	running.Store(false)

	return &SplitUpdateWorker{
		activeGoroutines: &sync.WaitGroup{},
		splitQueue:       splitQueue,
		sync:             synchronizer,
		logger:           logger,
		running:          running,
		stop:             make(chan struct{}, 1),
	}, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	s.logger.Debug("Started SplitUpdateWorker")
	if s.IsRunning() {
		s.logger.Info("Split worker is already running")
		return
	}
	s.running.Store(true)
	go func() {
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
	if s.IsRunning() {
		s.stop <- struct{}{}
		s.running.Store(false)
	}
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	return s.running.Load().(bool)
}
