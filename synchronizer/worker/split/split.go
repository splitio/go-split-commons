package split

import (
	"strconv"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-split-commons/v3/util"
	"github.com/splitio/go-toolkit/v4/logging"
)

// UpdaterImpl struct for split sync
type UpdaterImpl struct {
	splitStorage   storage.SplitStorage
	splitFetcher   service.SplitFetcher
	metricsWrapper *storage.MetricWrapper
	logger         logging.LoggerInterface
}

// NewSplitFetcher creates new split synchronizer for processing split updates
func NewSplitFetcher(
	splitStorage storage.SplitStorage,
	splitFetcher service.SplitFetcher,
	metricsWrapper *storage.MetricWrapper,
	logger logging.LoggerInterface,
) Updater {
	return &UpdaterImpl{
		splitStorage:   splitStorage,
		splitFetcher:   splitFetcher,
		metricsWrapper: metricsWrapper,
		logger:         logger,
	}
}

func (s *UpdaterImpl) processUpdate(splits *dtos.SplitChangesDTO) {
	inactiveSplits := make([]dtos.SplitDTO, 0)
	activeSplits := make([]dtos.SplitDTO, 0)
	for _, split := range splits.Splits {
		if split.Status == "ACTIVE" {
			activeSplits = append(activeSplits, split)
		} else {
			inactiveSplits = append(inactiveSplits, split)
		}
	}

	// Add/Update active splits
	s.splitStorage.PutMany(activeSplits, splits.Till)

	// Remove inactive splits
	for _, split := range inactiveSplits {
		s.splitStorage.Remove(split.Name)
	}
}

// SynchronizeSplits syncs splits
func (s *UpdaterImpl) SynchronizeSplits(till *int64) error {
	// @TODO: add delays
	for {
		changeNumber, _ := s.splitStorage.ChangeNumber()
		if changeNumber == 0 {
			changeNumber = -1
		}
		if till != nil && *till < changeNumber {
			return nil
		}

		before := time.Now()
		splits, err := s.splitFetcher.Fetch(changeNumber)
		if err != nil {
			if httpError, ok := err.(*dtos.HTTPError); ok {
				s.metricsWrapper.StoreCounters(storage.SplitChangesCounter, strconv.Itoa(httpError.Code))
			}
			return err
		}
		s.processUpdate(splits)
		bucket := util.Bucket(time.Now().Sub(before).Nanoseconds())
		s.metricsWrapper.StoreCounters(storage.SplitChangesCounter, "ok")
		s.metricsWrapper.StoreLatencies(storage.SplitChangesLatency, bucket)
		if splits.Till == splits.Since || (till != nil && splits.Till >= *till) {
			return nil
		}
	}
}

// LocalKill marks a spit as killed in local storage
func (s *UpdaterImpl) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	s.splitStorage.KillLocally(splitName, defaultTreatment, changeNumber)
}
