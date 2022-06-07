package strategy

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-split-commons/v4/telemetry"
	"github.com/splitio/go-split-commons/v4/util"
)

// OptimizedImpl struct for optimized impression mode strategy.
type OptimizedImpl struct {
	impressionObserver ImpressionObserver
	impressionsCounter *ImpressionsCounter
	runtimeTelemetry   storage.TelemetryRuntimeProducer
}

// NewOptimizedImpl creates new OptimizedImpl.
func NewOptimizedImpl(impressionObserver ImpressionObserver, impressionCounter *ImpressionsCounter, runtimeTelemetry storage.TelemetryRuntimeProducer) ProcessStrategyInterface {
	return &OptimizedImpl{
		impressionObserver: impressionObserver,
		impressionsCounter: impressionCounter,
		runtimeTelemetry:   runtimeTelemetry,
	}
}

func (s *OptimizedImpl) apply(impression *dtos.Impression, now int64) bool {
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, impression)
	s.impressionsCounter.Inc(impression.FeatureName, now, 1)

	if impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(now) {
		return true
	}

	return false
}

// Apply track the total amount of evaluations and deduplicate the impressions.
func (s *OptimizedImpl) Apply(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression) {
	now := time.Now().UTC().UnixNano()
	forLog := make([]dtos.Impression, 0, len(impressions))
	forListener := make([]dtos.Impression, 0, len(impressions))

	for _, impression := range impressions {
		if s.apply(&impression, now) {
			forLog = append(forLog, impression)
		}

		forListener = append(forListener, impression)
	}

	s.runtimeTelemetry.RecordImpressionsStats(telemetry.ImpressionsDeduped, int64(len(impressions)-len(forLog)))

	return forLog, forListener
}

// ApplySingle track the total amount of evaluations and deduplicate the impressions.
func (s *OptimizedImpl) ApplySingle(impression *dtos.Impression) bool {
	now := time.Now().UTC().UnixNano()

	return s.apply(impression, now)
}
