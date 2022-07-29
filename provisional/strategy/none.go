package strategy

import (
	"github.com/splitio/go-split-commons/v4/conf"
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/util"
)

// NoneImpl struct for none impression mode strategy.
type NoneImpl struct {
	impressionsCounter *ImpressionsCounter
	uniqueKeysTracker  UniqueKeysTracker
	listenerEnabled    bool
}

// NewNoneImpl creates new NoneImpl.
func NewNoneImpl(impressionCounter *ImpressionsCounter, uniqueKeysTracker UniqueKeysTracker, listenerEnabled bool) ProcessStrategyInterface {
	return &NoneImpl{
		impressionsCounter: impressionCounter,
		uniqueKeysTracker:  uniqueKeysTracker,
		listenerEnabled:    listenerEnabled,
	}
}

func (s *NoneImpl) apply(impression *dtos.Impression) bool {
	s.impressionsCounter.Inc(impression.FeatureName, impression.Time, 1)
	s.uniqueKeysTracker.Track(impression.FeatureName, impression.KeyName)

	return false
}

// Apply track the total amount of evaluations and the unique keys.
func (s *NoneImpl) Apply(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression) {
	forListener := make([]dtos.Impression, 0, len(impressions))

	for index := range impressions {
		impressions[index].Strategy = util.ImpressionModeShortVersion(conf.ImpressionsModeNone)
		s.apply(&impressions[index])
	}

	if s.listenerEnabled {
		forListener = impressions
	}

	return make([]dtos.Impression, 0, 0), forListener
}

// ApplySingle description
func (s *NoneImpl) ApplySingle(impression *dtos.Impression) bool {
	return s.apply(impression)
}
