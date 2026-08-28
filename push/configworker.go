package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v10/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

type ConfigUpdateWorker struct {
	configQueue chan dtos.ConfigChangeUpdate
	sync        synchronizerInterface
	logger      logging.LoggerInterface
	lifecycle   lifecycle.Manager
}

func NewConfigUpdateWorker(
	configQueue chan dtos.ConfigChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
) (*ConfigUpdateWorker, error) {
	if cap(configQueue) < 5000 {
		return nil, errors.New("configQueue capacity must be larger")
	}

	worker := &ConfigUpdateWorker{
		configQueue: configQueue,
		sync:        synchronizer,
		logger:      logger,
	}
	worker.lifecycle.Setup()
	return worker, nil
}

// Start starts worker
func (s *ConfigUpdateWorker) Start() {
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Config worker is already running")
		return
	}

	go func() {
		if !s.lifecycle.InitializationComplete() {
			return
		}
		defer s.lifecycle.ShutdownComplete()
		for {
			select {
			case update := <-s.configQueue:
				s.logger.Debug(fmt.Sprintf("Received Config update. ChangeNumber: %d", update.ChangeNumber()))
				err := s.sync.SynchronizeConfig(&update)
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
func (s *ConfigUpdateWorker) Stop() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Debug("Config worker not runnning. Ignoring.")
		return
	}
	s.lifecycle.AwaitShutdownComplete()
}

// IsRunning indicates if worker is running or not
func (s *ConfigUpdateWorker) IsRunning() bool {
	return s.lifecycle.IsRunning()
}
