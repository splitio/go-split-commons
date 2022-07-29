package strategy

import (
	"github.com/splitio/go-split-commons/v4/conf"
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
	listenerEnabled    bool
}

// NewOptimizedImpl creates new OptimizedImpl.
func NewOptimizedImpl(impressionObserver ImpressionObserver, impressionCounter *ImpressionsCounter, runtimeTelemetry storage.TelemetryRuntimeProducer, listenerEnabled bool) ProcessStrategyInterface {
	return &OptimizedImpl{
		impressionObserver: impressionObserver,
		impressionsCounter: impressionCounter,
		runtimeTelemetry:   runtimeTelemetry,
		listenerEnabled:    listenerEnabled,
	}
}

func (s *OptimizedImpl) apply(impression *dtos.Impression) bool {
	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, impression)
	s.impressionsCounter.Inc(impression.FeatureName, impression.Time, 1)

	if impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(impression.Time) {
		return true
	}

	return false
}

// Apply track the total amount of evaluations and deduplicate the impressions.
func (s *OptimizedImpl) Apply(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression) {
	forLog := make([]dtos.Impression, 0, len(impressions))
	forListener := make([]dtos.Impression, 0, len(impressions))

	for index := range impressions {
		impressions[index].Strategy = util.ImpressionModeShortVersion(conf.ImpressionsModeOptimized)
		if s.apply(&impressions[index]) {
			forLog = append(forLog, impressions[index])
		}
	}

	if s.listenerEnabled {
		forListener = impressions
	}

	s.runtimeTelemetry.RecordImpressionsStats(telemetry.ImpressionsDeduped, int64(len(impressions)-len(forLog)))

	return forLog, forListener
}

// ApplySingle track the total amount of evaluations and deduplicate the impressions.
func (s *OptimizedImpl) ApplySingle(impression *dtos.Impression) bool {
	if util.ImpressionModeMapper(impression.Strategy) != conf.ImpressionsModeOptimized {
		return s.apply(impression)
	}

	impression.Pt, _ = s.impressionObserver.TestAndSet(impression.FeatureName, impression)
	if impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(impression.Time) {
		return true
	}

	return false
}
