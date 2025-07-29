package grammar

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v6/engine/grammar/constants"

	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Split struct with added logic that wraps around a DTO
type Split struct {
	splitData  *dtos.SplitDTO
	conditions []*Condition
}

var conditionReplacementUnsupportedMatcher []*Condition = []*Condition{
	BuildCondition(ConditionTypeWhitelist, impressionlabels.UnsupportedMatcherType,
		[]Partition{{PartitionData: dtos.PartitionDTO{Treatment: "control", Size: 100}}}, []MatcherInterface{NewAllKeysMatcher(false)},
		"AND")}

// NewSplit instantiates a new Split object and all it's internal structures mapped to model classes
func NewSplit(splitDTO *dtos.SplitDTO, ctx *injection.Context, logger logging.LoggerInterface) *Split {
	split := Split{
		conditions: processConditions(splitDTO, ctx, logger),
		splitData:  splitDTO,
	}

	return &split
}

func processConditions(splitDTO *dtos.SplitDTO, ctx *injection.Context, logger logging.LoggerInterface) []*Condition {
	conditionsToReturn := make([]*Condition, 0)
	for _, cond := range splitDTO.Conditions {
		condition, err := NewCondition(&cond, ctx, logger)
		if err != nil {
			logger.Debug("Overriding conditions due unexpected matcher received")
			return conditionReplacementUnsupportedMatcher
		}
		conditionsToReturn = append(conditionsToReturn, condition)
	}
	return conditionsToReturn
}

// Name returns the name of the feature
func (s *Split) Name() string {
	return s.splitData.Name
}

// Seed returns the seed use for hashing
func (s *Split) Seed() int64 {
	return s.splitData.Seed
}

// Status returns whether the feature flag is active or arhived
func (s *Split) Status() string {
	status := s.splitData.Status
	if status == "" || (status != constants.SplitStatusActive && status != constants.SplitStatusArchived) {
		return constants.SplitStatusActive
	}
	return status
}

// Killed returns whether the feature flag has been killed or not
func (s *Split) Killed() bool {
	return s.splitData.Killed
}

// DefaultTreatment returns the default treatment for the current feature flag
func (s *Split) DefaultTreatment() string {
	return s.splitData.DefaultTreatment
}

// TrafficAllocation returns the traffic allocation configured for the current feature flag
func (s *Split) TrafficAllocation() int {
	return s.splitData.TrafficAllocation
}

// TrafficAllocationSeed returns the seed for traffic allocation configured for this feature flag
func (s *Split) TrafficAllocationSeed() int64 {
	return s.splitData.TrafficAllocationSeed
}

// Algo returns the hashing algorithm configured for this feature flag
func (s *Split) Algo() int {
	switch s.splitData.Algo {
	case constants.SplitAlgoLegacy:
		return constants.SplitAlgoLegacy
	case constants.SplitAlgoMurmur:
		return constants.SplitAlgoMurmur
	default:
		return constants.SplitAlgoLegacy
	}
}

// Conditions returns a slice of Condition objects
func (s *Split) Conditions() []*Condition {
	return s.conditions
}

// ChangeNumber returns the change number for this feature flag
func (s *Split) ChangeNumber() int64 {
	return s.splitData.ChangeNumber
}

// Configurations returns the configurations for this feature flag
func (s *Split) Configurations() map[string]string {
	return s.splitData.Configurations
}

func (s *Split) ImpressionsDisabled() bool {
	return s.splitData.ImpressionsDisabled
}
