package provisional

import (
	"github.com/splitio/go-split-commons/v4/dtos"
)

// DebugImpl struct for debug impression mode strategy.
type DebugImpl struct {
	impressionObserver ImpressionObserver
}

// NewDebugImpl creates new DebugImpl.
func NewDebugImpl(impressionObserver ImpressionObserver) ProcessStrategyInterface {
	return &DebugImpl{
		impressionObserver: impressionObserver,
	}
}

// Apply calculate the pt and return the impression.
func (s *DebugImpl) Apply(impression dtos.Impression) *dtos.Impression {
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, &impression)

	return &impression
}
