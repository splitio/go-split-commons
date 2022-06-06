package provisional

import (
	"github.com/splitio/go-split-commons/v4/dtos"
)

// DebugImpl description
type DebugImpl struct {
	impressionObserver ImpressionObserver
}

// NewDebugImpl constructor
func NewDebugImpl(impressionObserver ImpressionObserver) ProcessStrategyInterface {
	return &DebugImpl{
		impressionObserver: impressionObserver,
	}
}

// Apply description
func (s *DebugImpl) Apply(impression dtos.Impression) *dtos.Impression {
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, &impression)

	return &impression
}
