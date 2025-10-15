package validator

import (
	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine/evaluator"
	"github.com/splitio/go-split-commons/v8/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v8/engine/grammar"
	"github.com/splitio/go-split-commons/v8/engine/grammar/datatypes"
	"github.com/splitio/go-toolkit/v5/logging"
)

type Validator struct {
	ruleBuilder grammar.RuleBuilder
}

func NewValidator(ruleBuilder grammar.RuleBuilder) Validator {
	return Validator{
		ruleBuilder: ruleBuilder,
	}
}

// unsupportedMatcherConditionReplacement is the default condition to be used when a matcher is not supported
var unsupportedMatcherConditionReplacement []dtos.ConditionDTO = []dtos.ConditionDTO{{
	ConditionType: grammar.ConditionTypeWhitelist,
	Label:         impressionlabels.UnsupportedMatcherType,
	Partitions:    []dtos.PartitionDTO{{Treatment: evaluator.Control, Size: 100}},
	MatcherGroup: dtos.MatcherGroupDTO{
		Combiner: "AND",
		Matchers: []dtos.MatcherDTO{{MatcherType: grammar.MatcherTypeAllKeys, Negate: false}},
	},
}}

// unsupportedMatcherRBConditionReplacement is the default condition to be used when a matcher is not supported
var unsupportedMatcherRBConditionReplacement []dtos.RuleBasedConditionDTO = []dtos.RuleBasedConditionDTO{{
	ConditionType: grammar.ConditionTypeWhitelist,
	MatcherGroup: dtos.MatcherGroupDTO{
		Combiner: "AND",
		Matchers: []dtos.MatcherDTO{{MatcherType: grammar.MatcherTypeAllKeys, Negate: false}},
	},
}}

func (v *Validator) shouldOverrideConditions(conditions []dtos.ConditionDTO, logger logging.LoggerInterface) bool {
	for _, condition := range conditions {
		for _, matcher := range condition.MatcherGroup.Matchers {
			_, err := v.ruleBuilder.BuildMatcher(&matcher)
			if _, ok := err.(datatypes.UnsupportedMatcherError); ok {
				return true
			}
		}
	}
	return false
}

func (v *Validator) shouldOverrideRBConditions(conditions []dtos.RuleBasedConditionDTO, logger logging.LoggerInterface) bool {
	for _, condition := range conditions {
		for _, matcher := range condition.MatcherGroup.Matchers {
			_, err := v.ruleBuilder.BuildMatcher(&matcher)
			if _, ok := err.(datatypes.UnsupportedMatcherError); ok {
				return true
			}
		}
	}
	return false
}

// ProcessMatchers processes the matchers of a split and validates them
func (v *Validator) ProcessMatchers(split *dtos.SplitDTO, logger logging.LoggerInterface) {
	if v.shouldOverrideConditions(split.Conditions, logger) {
		split.Conditions = unsupportedMatcherConditionReplacement
	}
}

// ProcessMatchers processes the matchers of a rule-based and validates them
func (v *Validator) ProcessRBMatchers(ruleBased *dtos.RuleBasedSegmentDTO, logger logging.LoggerInterface) {
	if v.shouldOverrideRBConditions(ruleBased.Conditions, logger) {
		ruleBased.Conditions = unsupportedMatcherRBConditionReplacement
	}
}

// MakeUnsupportedMatcherConditionReplacement returns the default condition to be used when a matcher is not supported
func MakeUnsupportedMatcherConditionReplacement() []dtos.ConditionDTO {
	return unsupportedMatcherConditionReplacement
}
