package validator

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

var goClientFeatureFlagsRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString, grammar.MatcherTypeInSplitTreatment,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver,
	grammar.MatcherTypeInRuleBasedSegment}
var goClientRuleBasedSegmentRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver,
	grammar.MatcherTypeInRuleBasedSegment}

func TestProcessRBMatchers(t *testing.T) {
	// Test case 1: Rule-based segment with unsupported matcher
	ruleBased := &dtos.RuleBasedSegmentDTO{
		Name:         "test-segment",
		ChangeNumber: 123,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: "NEW_MATCHER", KeySelector: nil},
					},
				},
			},
		},
	}
	validator := NewValidator(grammar.NewRuleBuilder(nil, nil, nil, goClientFeatureFlagsRules, goClientRuleBasedSegmentRules, logging.NewLogger(nil)))
	validator.ProcessRBMatchers(ruleBased, logging.NewLogger(nil))
	if len(ruleBased.Conditions) != 1 {
		t.Error("Conditions should have been overridden")
	}
	if ruleBased.Conditions[0].ConditionType != grammar.ConditionTypeWhitelist {
		t.Error("ConditionType should be WHITELIST")
	}
	if ruleBased.Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}

	// Test case 2: Rule-based segment with supported matcher
	ruleBased = &dtos.RuleBasedSegmentDTO{
		Name:         "test-segment",
		ChangeNumber: 123,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: grammar.MatcherTypeEndsWith, KeySelector: nil, String: common.StringRef("test")},
					},
				},
			},
		},
	}
	validator.ProcessRBMatchers(ruleBased, logging.NewLogger(nil))
	if len(ruleBased.Conditions) != 1 {
		t.Error("Conditions should not have been overridden")
	}
	if ruleBased.Conditions[0].ConditionType != grammar.ConditionTypeRollout {
		t.Error("ConditionType should be ROLLOUT")
	}
	if ruleBased.Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeEndsWith {
		t.Error("MatcherType should be ENDS_WITH")
	}
}

func TestProcessMatchers(t *testing.T) {
	split := &dtos.SplitDTO{
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: grammar.MatcherTypeEndsWith, KeySelector: nil, String: common.StringRef("test")},
					},
				},
			},
			{
				ConditionType: "NEW_MATCHER",
				Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: "NEW_MATCHER", KeySelector: nil},
					},
				},
			},
		},
	}
	validator := NewValidator(grammar.NewRuleBuilder(nil, nil, nil, goClientFeatureFlagsRules, goClientRuleBasedSegmentRules, logging.NewLogger(nil)))
	validator.ProcessMatchers(split, logging.NewLogger(nil))
	if len(split.Conditions) != 1 {
		t.Error("Conditions should have been overridden")
	}
	if split.Conditions[0].ConditionType != grammar.ConditionTypeWhitelist {
		t.Error("ConditionType should be WHITELIST")
	}
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}

	split = &dtos.SplitDTO{
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: grammar.MatcherTypeAllKeys, KeySelector: nil},
					},
				},
			},
		},
	}
	validator.ProcessMatchers(split, logging.NewLogger(nil))

	if split.Conditions[0].ConditionType != grammar.ConditionTypeRollout {
		t.Error("ConditionType should be ROLLOUT")
	}
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}
}
