package provisional

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/util"
)

// ProcessStrategyInterface interface
type ProcessStrategyInterface interface {
	Apply(impression dtos.Impression) *dtos.Impression
}

// OptimizedImpl description
type OptimizedImpl struct {
	impressionObserver ImpressionObserver
	impressionsCounter *ImpressionsCounter
}

// NewOptimizedImpl constructor
func NewOptimizedImpl(impressionCounter *ImpressionsCounter, impressionObserver ImpressionObserver) ProcessStrategyInterface {
	return &OptimizedImpl{
		impressionObserver: impressionObserver,
		impressionsCounter: impressionCounter,
	}
}

// Apply description
func (s *OptimizedImpl) Apply(impression dtos.Impression) *dtos.Impression {
	now := time.Now().UTC().UnixNano()
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, &impression)
	s.impressionsCounter.Inc(impression.FeatureName, now, 1)

	if impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(now) {
		return &impression
	}

	return nil
}
