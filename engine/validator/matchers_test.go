package validator

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/engine/grammar"
	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestProcessMatchers(t *testing.T) {
	split := &dtos.SplitDTO{
		Conditions: []dtos.ConditionDTO{
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
	ProcessMatchers(split, logging.NewLogger(nil))
	if split.Conditions[0].ConditionType != grammar.ConditionTypeWhitelist {
		t.Error("ConditionType should be WHITELIST")
	}
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != matchers.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}

	split = &dtos.SplitDTO{
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: matchers.MatcherTypeAllKeys, KeySelector: nil},
					},
				},
			},
		},
	}
	ProcessMatchers(split, logging.NewLogger(nil))

	if split.Conditions[0].ConditionType != grammar.ConditionTypeRollout {
		t.Error("ConditionType should be ROLLOUT")
	}
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != matchers.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}
}
