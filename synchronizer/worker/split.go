package worker

import (
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

// SplitFetcher struct for split sync
type SplitFetcher struct {
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
) *SplitFetcher {
	return &SplitFetcher{
		splitStorage:   splitStorage,
		splitFetcher:   splitFetcher,
		metricsWrapper: metricsWrapper,
		logger:         logger,
	}
}

func (s *SplitFetcher) processUpdate(splits *dtos.SplitChangesDTO) {
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
func (s *SplitFetcher) SynchronizeSplits(till *int64) error {
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
			if _, ok := err.(*dtos.HTTPError); ok {
				s.metricsWrapper.StoreCounters(storage.SplitChangesCounter, string(err.(*dtos.HTTPError).Code))
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
