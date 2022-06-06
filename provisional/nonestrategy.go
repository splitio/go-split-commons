package provisional

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
)

// NoneImpl struct for none impression mode strategy.
type NoneImpl struct {
	impressionsCounter *ImpressionsCounter
	uniqueKeysTracker  UniqueKeysTracker
}

// NewNoneImpl creates new NoneImpl.
func NewNoneImpl(impressionCounter *ImpressionsCounter, uniqueKeysTracker UniqueKeysTracker) ProcessStrategyInterface {
	return &NoneImpl{
		impressionsCounter: impressionCounter,
		uniqueKeysTracker:  uniqueKeysTracker,
	}
}

// Apply track the total amount of evaluations and the unique keys.
func (s *NoneImpl) Apply(impression *dtos.Impression) bool {
	now := time.Now().UTC().UnixNano()

	s.impressionsCounter.Inc(impression.FeatureName, now, 1)
	s.uniqueKeysTracker.Track(impression.FeatureName, impression.BucketingKey)

	return false
}
