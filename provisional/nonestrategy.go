package provisional

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
)

// NoneImpl description
type NoneImpl struct {
	impressionsCounter *ImpressionsCounter
	uniqueKeysTracker  UniqueKeysTracker
}

// NewNoneImpl constructor
func NewNoneImpl(impressionCounter *ImpressionsCounter, uniqueKeysTracker UniqueKeysTracker) ProcessStrategyInterface {
	return &NoneImpl{
		impressionsCounter: impressionCounter,
		uniqueKeysTracker:  uniqueKeysTracker,
	}
}

// Apply description
func (s *NoneImpl) Apply(impression dtos.Impression) *dtos.Impression {
	now := time.Now().UTC().UnixNano()

	s.impressionsCounter.Inc(impression.FeatureName, now, 1)
	s.uniqueKeysTracker.Track(impression.FeatureName, impression.BucketingKey)

	return nil
}
