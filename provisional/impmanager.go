package provisional

import (
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/provisional/strategy"
)

// ImpressionManager interface
type ImpressionManager interface {
	ProcessImpressions(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression)
	ProcessSingle(impression *dtos.Impression) bool
	Process(impressions []dtos.Impression, listenerEnabled bool) ([]dtos.Impression, []dtos.Impression)
}

// ImpressionManagerImpl implements
type ImpressionManagerImpl struct {
	processStrategy strategy.ProcessStrategyInterface
	keyTracker      strategy.ProcessStrategyInterface
}

// DEPRECATED
// NewImpressionManager creates new ImpManager
func NewImpressionManager(processStrategy strategy.ProcessStrategyInterface) ImpressionManager {
	return &ImpressionManagerImpl{
		processStrategy: processStrategy,
	}
}

func NewImpressionManagerImp(none *strategy.NoneImpl, processStrategy strategy.ProcessStrategyInterface) ImpressionManager {
	return &ImpressionManagerImpl{
		processStrategy: processStrategy,
		keyTracker:      none,
	}
}

// ProcessImpressions bulk processes
func (i *ImpressionManagerImpl) ProcessImpressions(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression) {
	return i.processStrategy.Apply(impressions)
}

// ProcessSingle accepts a pointer to an impression, updates it's PT accordingly,
// and returns whether it should be sent to the BE and to the lister
func (i *ImpressionManagerImpl) ProcessSingle(impression *dtos.Impression) bool {
	return i.processStrategy.ApplySingle(impression)
}

func (i *ImpressionManagerImpl) Process(impressions []dtos.Impression, listenerEnabled bool) ([]dtos.Impression, []dtos.Impression) {
	forLog := make([]dtos.Impression, 0, len(impressions))
	forListener := make([]dtos.Impression, 0, len(impressions))

	for index := range impressions {
		if impressions[index].Disabled {
			i.keyTracker.ApplySingle(&impressions[index])
		} else if i.processStrategy.ApplySingle(&impressions[index]) {
			forLog = append(forLog, impressions[index])
		}
	}

	if listenerEnabled {
		forListener = impressions
	}

	return forLog, forListener
}
