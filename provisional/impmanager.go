package provisional

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/provisional/strategy"
)

// ImpressionManager interface
type ImpressionManager interface {
	ProcessImpressions(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression)
	ProcessSingle(impression *dtos.Impression) bool
	Process(values []dtos.ImpressionDecorated, listenerEnabled bool) ([]dtos.Impression, []dtos.Impression)
}

// ImpressionManagerImpl implements
type ImpressionManagerImpl struct {
	processStrategy strategy.ProcessStrategyInterface
	noneStrategy    strategy.ProcessStrategyInterface
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
		noneStrategy:    none,
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

func (i *ImpressionManagerImpl) Process(values []dtos.ImpressionDecorated, listenerEnabled bool) ([]dtos.Impression, []dtos.Impression) {
	forLog := make([]dtos.Impression, 0, len(values))
	forListener := make([]dtos.Impression, 0, len(values))

	for index := range values {
		if !values[index].Disabled && i.processStrategy != nil {
			toLog := i.processStrategy.ApplySingle(&values[index].Impression)

			if toLog {
				forLog = append(forLog, values[index].Impression)
			}
		} else {
			i.noneStrategy.ApplySingle(&values[index].Impression)
		}

		if listenerEnabled {
			forListener = append(forListener, values[index].Impression)
		}
	}

	return forLog, forListener
}
