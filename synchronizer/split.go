package synchronizer

import (
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	splitChangesLatencies        = "splitChangeFetcher.time"
	splitChangesLatenciesBackend = "backend::/api/splitChanges"
	splitChangesCounters         = "splitChangeFetcher.status.{status}"
	splitChangesLocalCounters    = "backend::request.{status}"
)

// SplitSynchronizer struct for split sync
type SplitSynchronizer struct {
	splitStorage  storage.SplitStorage
	splitFetcher  service.SplitFetcher
	metricStorage storage.MetricsStorage
	logger        logging.LoggerInterface
}

// NewSplitSynchronizer creates new split synchronizer for processing split updates
func NewSplitSynchronizer(
	splitStorage storage.SplitStorage,
	splitFetcher service.SplitFetcher,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
) *SplitSynchronizer {
	return &SplitSynchronizer{
		splitStorage:  splitStorage,
		splitFetcher:  splitFetcher,
		metricStorage: metricStorage,
		logger:        logger,
	}
}

func (s *SplitSynchronizer) processUpdate(splits *dtos.SplitChangesDTO) {
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
func (s *SplitSynchronizer) SynchronizeSplits(till *int64) error {
	// @TODO: add delays
	for {
		changeNumber, _ := s.splitStorage.ChangeNumber()
		if changeNumber == 0 {
			changeNumber = -1
		}

		before := time.Now()
		splits, err := s.splitFetcher.Fetch(changeNumber)
		if err != nil {
			if _, ok := err.(*dtos.HTTPError); ok {
				s.metricStorage.IncCounter(strings.Replace(splitChangesLocalCounters, "{status}", "error", 1))
				s.metricStorage.IncCounter(strings.Replace(splitChangesCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
			}
			return err
		}
		s.processUpdate(splits)
		elapsed := int(time.Now().Sub(before).Nanoseconds())
		s.metricStorage.IncLatency(splitChangesLatencies, elapsed)
		s.metricStorage.IncLatency(splitChangesLatenciesBackend, elapsed)
		s.metricStorage.IncCounter(strings.Replace(splitChangesLocalCounters, "{status}", "ok", 1))
		s.metricStorage.IncCounter(strings.Replace(splitChangesCounters, "{status}", "200", 1))
		if splits.Till == splits.Since || (till != nil && *till > splits.Till) {
			return nil
		}
	}
}
