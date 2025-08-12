package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

// SplitUpdateWorker struct
type RuleBasedUpdateWorker struct {
	ruleBasedQueue chan dtos.RuleBasedChangeUpdate
	sync           synchronizerInterface
	logger         logging.LoggerInterface
	lifecycle      lifecycle.Manager
}

// NewRuleBasedUpdateWorker creates SplitRuleBasedWorker
func NewRuleBasedUpdateWorker(
	ruleBasedQueue chan dtos.RuleBasedChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*RuleBasedUpdateWorker, error) {
	if cap(ruleBasedQueue) < 5000 {
		return nil, errors.New("")
	}

	worker := &RuleBasedUpdateWorker{
		ruleBasedQueue: ruleBasedQueue,
		sync:           synchronizer,
		logger:         logger,
	}
	worker.lifecycle.Setup()
	return worker, nil
}

// Start starts worker
func (s *RuleBasedUpdateWorker) Start() {
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Rule-based worker is already running")
		return
	}

	s.logger.Debug("Started RuleBasedUpdateWorker")
	go func() {
		defer s.lifecycle.ShutdownComplete()
		s.lifecycle.InitializationComplete()
		for {
			select {
			case ruleBasedUpdate := <-s.ruleBasedQueue:
				s.logger.Debug("Received Rule-based update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", ruleBasedUpdate.ChangeNumber()))
				err := s.sync.SynchronizeRuleBasedSegments(&ruleBasedUpdate)
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
func (s *RuleBasedUpdateWorker) Stop() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Debug("Rule-based worker not runnning. Ignoring.")
		return
	}
	s.lifecycle.AwaitShutdownComplete()
}

// IsRunning indicates if worker is running or not
func (s *RuleBasedUpdateWorker) IsRunning() bool {
	return s.lifecycle.IsRunning()
}
