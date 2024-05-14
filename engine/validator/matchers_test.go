package validator

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestProcessMatchers(t *testing.T) {
	split := &dtos.SplitDTO{
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: grammar.ConditionTypeRollout,
				Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{MatcherType: matchers.MatcherTypeEndsWith, KeySelector: nil, String: common.StringRef("test")},
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
	ProcessMatchers(split, logging.NewLogger(nil))
	if len(split.Conditions) != 1 {
		t.Error("Conditions should have been overridden")
	}
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
