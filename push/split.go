package push

import (
	"errors"
	"fmt"
	"sync"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	activeGoroutines *sync.WaitGroup
	splitQueue       chan dtos.SplitChangeNotification
	handler          func(till *int64) error
	logger           logging.LoggerInterface
	stop             chan struct{}
	running          bool
	mutex            *sync.RWMutex
}

// NewSplitUpdateWorker creates SplitUpdateWorker
func NewSplitUpdateWorker(splitQueue chan dtos.SplitChangeNotification, handler func(till *int64) error, logger logging.LoggerInterface) (*SplitUpdateWorker, error) {
	if cap(splitQueue) < 5000 {
		return nil, errors.New("")
	}

	return &SplitUpdateWorker{
		activeGoroutines: &sync.WaitGroup{},
		splitQueue:       splitQueue,
		handler:          handler,
		logger:           logger,
		stop:             make(chan struct{}, 1),
		running:          false,
		mutex:            &sync.RWMutex{},
	}, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	s.logger.Info("Started SplitUpdateWorker")
	if s.IsRunning() {
		s.logger.Info("Split worker is already running")
		return
	}
	s.activeGoroutines.Add(1)
	s.mutex.Lock()
	s.running = true
	s.mutex.Unlock()
	go func() {
		defer s.activeGoroutines.Done()
		for {
			select {
			case splitUpdate := <-s.splitQueue:
				s.logger.Debug("Received Split update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", splitUpdate.ChangeNumber))
				err := s.handler(&splitUpdate.ChangeNumber)
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
	s.stop <- struct{}{}
	s.activeGoroutines.Wait()
	s.mutex.Lock()
	s.running = false
	s.mutex.Unlock()
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.running
}
