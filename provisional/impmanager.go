package provisional

import (
	"github.com/splitio/go-split-commons/v4/conf"
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/provisional/strategy"
	"github.com/splitio/go-split-commons/v4/storage"
)

const lastSeenCacheSize = 500000 // cache up to 500k impression hashes

// ImpressionManager interface
type ImpressionManager interface {
	ProcessImpressions(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression)
	ProcessSingle(impression *dtos.Impression) (toLog bool, toListener bool)
}

// ImpressionManagerImpl implements
type ImpressionManagerImpl struct {
	listenerEnabled  bool
	runtimeTelemetry storage.TelemetryRuntimeProducer
	processStrategy  strategy.ProcessStrategyInterface
}

// NewImpressionManager creates new ImpManager
func NewImpressionManager(managerConfig conf.ManagerConfig, processStrategy strategy.ProcessStrategyInterface) ImpressionManager {
	impManager := &ImpressionManagerImpl{
		listenerEnabled: managerConfig.ListenerEnabled,
		processStrategy: processStrategy,
	}

	return impManager
}

// ProcessImpressions bulk processes
func (i *ImpressionManagerImpl) ProcessImpressions(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression) {
	forLog, forListener := i.processStrategy.Apply(impressions)

	if i.listenerEnabled {
		return forLog, forListener
	}

	return forLog, make([]dtos.Impression, 0)
}

// ProcessSingle accepts a pointer to an impression, updates it's PT accordingly,
// and returns whether it should be sent to the BE and to the lister
func (i *ImpressionManagerImpl) ProcessSingle(impression *dtos.Impression) (toLog bool, toListener bool) {
	toLog = i.processStrategy.ApplySingle(impression)

	return toLog, i.listenerEnabled
}
