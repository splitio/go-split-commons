package provisional

import (
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/util"
)

const lastSeenCacheSize = 500000 // cache up to 500k impression hashes

// ImpressionManager interface
type ImpressionManager interface {
	ProcessImpressions(impressions []dtos.Impression) ProcessResult
}

// ImpressionManagerImpl implements
type ImpressionManagerImpl struct {
	impressionObserver    ImpressionObserver
	impressionsCounter    *ImpressionsCounter
	shouldAddPreviousTime bool
	isOptimized           bool
}

func shouldAddPreviousTime(managerConfig conf.ManagerConfig) bool {
	switch managerConfig.OperationMode {
	case conf.Standalone:
		return true
	default:
		return false
	}
}

func shouldBeOptimized(managerConfig conf.ManagerConfig) bool {
	if !shouldAddPreviousTime(managerConfig) {
		return false
	}
	if managerConfig.ImpressionsMode == conf.Optimized {
		return true
	}
	return false
}

// NewImpressionManager creates new ImpManager
func NewImpressionManager(managerConfig conf.ManagerConfig) (ImpressionManager, error) {
	impressionObserver, err := NewImpressionObserver(lastSeenCacheSize)
	if err != nil {
		return nil, err
	}

	impManager := &ImpressionManagerImpl{
		impressionObserver:    impressionObserver,
		impressionsCounter:    NewImpressionsCounter(),
		shouldAddPreviousTime: shouldAddPreviousTime(managerConfig),
		isOptimized:           shouldBeOptimized(managerConfig),
	}

	return impManager, nil
}

func (i *ImpressionManagerImpl) processImpression(impression dtos.Impression, impressionsForLog *[]dtos.Impression, impressionsForListener *[]dtos.Impression) {
	if i.shouldAddPreviousTime {
		impression.Pt, _ = i.impressionObserver.TestAndSet(impression.FeatureName, &impression) // Adds previous time if it is enabled
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	if i.isOptimized { // isOptimized
		i.impressionsCounter.Inc(impression.FeatureName, now, 1) // Increments impression counter per featureName
	}

	if !i.isOptimized || impression.Pt == 0 || impression.Pt < util.TruncateTimeFrame(now) {
		*impressionsForLog = append(*impressionsForLog, impression)
	}

	*impressionsForListener = append(*impressionsForListener, impression)
}

// ProcessResult struct for returning in impressions deduping
type ProcessResult struct {
	ImpressionsForListener []dtos.Impression
	ImpressionsForLog      []dtos.Impression
}

// ProcessImpressions bulk processes
func (i *ImpressionManagerImpl) ProcessImpressions(impressions []dtos.Impression) ProcessResult {
	impressionsForListener := make([]dtos.Impression, 0, len(impressions))
	impressionsForLog := make([]dtos.Impression, 0, len(impressions))

	for _, impression := range impressions {
		i.processImpression(impression, &impressionsForLog, &impressionsForListener)
	}

	return ProcessResult{
		ImpressionsForListener: impressionsForListener,
		ImpressionsForLog:      impressionsForLog,
	}
}
