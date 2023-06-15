package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-split-commons/v4/util"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

// SplitUpdateWorker struct
type SplitUpdateWorker struct {
	splitQueue chan SplitChangeUpdate
	sync       synchronizerInterface
	logger     logging.LoggerInterface
	lifecycle  lifecycle.Manager
	ffStorage  storage.SplitStorage
}

// NewSplitUpdateWorker creates SplitUpdateWorker
func NewSplitUpdateWorker(
	splitQueue chan SplitChangeUpdate,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
	storage storage.SplitStorage,
) (*SplitUpdateWorker, error) {
	if cap(splitQueue) < 5000 {
		return nil, errors.New("")
	}

	worker := &SplitUpdateWorker{
		splitQueue: splitQueue,
		sync:       synchronizer,
		logger:     logger,
		ffStorage:  storage,
	}
	worker.lifecycle.Setup()
	return worker, nil
}

// Start starts worker
func (s *SplitUpdateWorker) Start() {
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Split worker is already running")
		return
	}

	s.logger.Debug("Started SplitUpdateWorker")
	go func() {
		defer s.lifecycle.ShutdownComplete()
		if !s.lifecycle.InitializationComplete() {
			return
		}
		for {
			select {
			case splitUpdate := <-s.splitQueue:
				s.logger.Debug("Received Split update and proceding to perform fetch")
				s.logger.Debug(fmt.Sprintf("ChangeNumber: %d", splitUpdate.ChangeNumber()))
				if !s.addOrUpdateFeatureFlag(splitUpdate) {
					err := s.sync.SynchronizeSplits(common.Int64Ref(splitUpdate.ChangeNumber()))
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
func (s *SplitUpdateWorker) Stop() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Debug("Split worker not runnning. Ignoring.")
		return
	}
	s.lifecycle.AwaitShutdownComplete()
}

// IsRunning indicates if worker is running or not
func (s *SplitUpdateWorker) IsRunning() bool {
	return s.lifecycle.IsRunning()
}

func (s *SplitUpdateWorker) addOrUpdateFeatureFlag(featureFlagUpdate SplitChangeUpdate) bool {
	changeNumber, err := s.ffStorage.ChangeNumber()
	if err != nil {
		s.logger.Debug("problem getting change number from feature flag storage: %s", err.Error())
		return false
	}
	if changeNumber >= featureFlagUpdate.BaseUpdate.changeNumber {
		s.logger.Debug("the feature flag it's already updated")
		return true
	}
	if featureFlagUpdate.featureFlag != nil && featureFlagUpdate.previousChangeNumber == changeNumber {
		s.logger.Debug("updating feature flag %s", featureFlagUpdate.featureFlag.Name)
		var featureFlags []dtos.SplitDTO
		featureFlags = append(featureFlags, *featureFlagUpdate.featureFlag)
		featureFlagChange := dtos.SplitChangesDTO{Splits: featureFlags}
		activeFFs, inactiveFFs := util.ProcessFeatureFlagChanges(&featureFlagChange)
		s.ffStorage.Update(activeFFs, inactiveFFs, featureFlagUpdate.BaseUpdate.changeNumber)
		return true
	}
	s.logger.Debug("the feature flag was nil or the previous change number wasn't equal to the feature flag storage's change number")
	return false
}
