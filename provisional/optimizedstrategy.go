package provisional

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/util"
)

// ProcessStrategyInterface interface
type ProcessStrategyInterface interface {
	Apply(impression *dtos.Impression) bool
}

// OptimizedImpl struct for optimized impression mode strategy.
type OptimizedImpl struct {
	impressionObserver ImpressionObserver
	impressionsCounter *ImpressionsCounter
}

// NewOptimizedImpl creates new OptimizedImpl.
func NewOptimizedImpl(impressionCounter *ImpressionsCounter, impressionObserver ImpressionObserver) ProcessStrategyInterface {
	return &OptimizedImpl{
		impressionObserver: impressionObserver,
		impressionsCounter: impressionCounter,
	}
}

// Apply track the total amount of evaluations and deduplicate the impressions.
func (s *OptimizedImpl) Apply(impression *dtos.Impression) bool {
	now := time.Now().UTC().UnixNano()
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, impression)
	s.impressionsCounter.Inc(impression.FeatureName, now, 1)

	if impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(now) {
		return true
	}

	return false
}
