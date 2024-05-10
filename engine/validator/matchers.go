package validator

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/engine/evaluator"
	"github.com/splitio/go-split-commons/v5/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v5/engine/grammar"
	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

// unsupportedMatcherConditionReplacement is the default condition to be used when a matcher is not supported
var unsupportedMatcherConditionReplacement []dtos.ConditionDTO = []dtos.ConditionDTO{{
	ConditionType: grammar.ConditionTypeWhitelist,
	Label:         impressionlabels.UnsupportedMatcherType,
	Partitions:    []dtos.PartitionDTO{{Treatment: evaluator.Control, Size: 100}},
	MatcherGroup: dtos.MatcherGroupDTO{
		Combiner: "AND",
		Matchers: []dtos.MatcherDTO{{MatcherType: matchers.MatcherTypeAllKeys, Negate: false}},
	},
}}

func shouldOverrideConditions(conditions []dtos.ConditionDTO, logger logging.LoggerInterface) bool {
	for _, condition := range conditions {
		for _, matcher := range condition.MatcherGroup.Matchers {
			_, err := matchers.BuildMatcher(&matcher, &injection.Context{}, logger)
			if err != nil {
				return true
			}
		}
	}
	return false
}

// ProcessMatchers processes the matchers of a split and validates them
func ProcessMatchers(split *dtos.SplitDTO, logger logging.LoggerInterface) {
	if shouldOverrideConditions(split.Conditions, logger) {
		split.Conditions = unsupportedMatcherConditionReplacement
	}
}

// MakeUnsupportedMatcherConditionReplacement returns the default condition to be used when a matcher is not supported
func MakeUnsupportedMatcherConditionReplacement() []dtos.ConditionDTO {
	return unsupportedMatcherConditionReplacement
}
