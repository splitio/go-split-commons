package worker

import (
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

const (
	splitChangesLatencies        = "splitChangeFetcher.time"
	splitChangesLatenciesBackend = "backend::/api/splitChanges" // @TODO add local
	splitChangesCounters         = "splitChangeFetcher.status.{status}"
	splitChangesLocalCounters    = "backend::request.{status}" // @TODO add local
)

// SplitFetcher struct for split sync
type SplitFetcher struct {
	splitStorage  storage.SplitStorage
	splitFetcher  service.SplitFetcher
	metricStorage storage.MetricsStorage
	logger        logging.LoggerInterface
}

// NewSplitFetcher creates new split synchronizer for processing split updates
func NewSplitFetcher(
	splitStorage storage.SplitStorage,
	splitFetcher service.SplitFetcher,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
) *SplitFetcher {
	return &SplitFetcher{
		splitStorage:  splitStorage,
		splitFetcher:  splitFetcher,
		metricStorage: metricStorage,
		logger:        logger,
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
				s.metricStorage.IncCounter(strings.Replace(splitChangesCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
			}
			return err
		}
		s.processUpdate(splits)
		bucket := util.Bucket(time.Now().Sub(before).Nanoseconds())
		s.metricStorage.IncLatency(splitChangesLatencies, bucket)
		s.metricStorage.IncCounter(strings.Replace(splitChangesCounters, "{status}", "200", 1))
		if splits.Till == splits.Since || (till != nil && splits.Till >= *till) {
			return nil
		}
	}
}
