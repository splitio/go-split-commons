package synchronizer

import (
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
)

// SplitSynchronizer struct for split sync
type SplitSynchronizer struct {
	splitStorage storage.SplitStorage
	splitFetcher service.SplitFetcher
}

// NewSplitSynchronizer creates new split synchronizer for processing split updates
func NewSplitSynchronizer(
	splitStorage storage.SplitStorage,
	splitFetcher service.SplitFetcher,
) *SplitSynchronizer {
	return &SplitSynchronizer{
		splitStorage: splitStorage,
		splitFetcher: splitFetcher,
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
func (s *SplitSynchronizer) SynchronizeSplits(till *int64) (bool, error) {
	// @TODO: add delays
	for {
		changeNumber, _ := s.splitStorage.ChangeNumber()
		if changeNumber == 0 {
			changeNumber = -1
		}

		splits, err := s.splitFetcher.Fetch(changeNumber)
		if err != nil {
			return false, err
		}
		s.processUpdate(splits)
		if splits.Till == splits.Since || (till != nil && *till > splits.Till) {
			return true, nil
		}
	}
}
