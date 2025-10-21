package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v7/engine/grammar/datatypes"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

func TestConditionWrapperObject(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	condition1 := dtos.ConditionDTO{
		ConditionType: "WHITELIST",
		Label:         "Label1",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					Negate:      false,
					MatcherType: "ALL_KEYS",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
				{
					Negate:      true,
					MatcherType: "ALL_KEYS",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      75,
				Treatment: "on",
			},
			{
				Size:      25,
				Treatment: "off",
			},
		},
	}

	wrapped, err := NewCondition(&condition1, logger, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil))

	if err != nil {
		t.Error("err should be nil")
	}

	if wrapped.Label() != "Label1" {
		t.Error("Label not set properly")
	}

	if wrapped.combiner != "AND" {
		t.Error("Combiner not set properly")
	}

	if len(wrapped.matchers) != len(condition1.MatcherGroup.Matchers) {
		t.Error("Incorrect number of matchers")
	}

	if wrapped.ConditionType() != ConditionTypeWhitelist {
		t.Error("Incorrect condition type")
	}

	treatment1 := wrapped.CalculateTreatment(50)
	if treatment1 != nil && *treatment1 != "on" {
		t.Error("CalculateTreatment returned incorrect treatment")
	}

	treatment2 := wrapped.CalculateTreatment(80)
	if treatment2 != nil && *treatment2 != "off" {
		t.Error("CalculateTreatment returned incorrect treatment")
	}
}

func TestAnotherWrapper(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	condition1 := dtos.ConditionDTO{
		ConditionType: "ROLLOUT",
		Label:         "Label2",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					Negate:      false,
					MatcherType: "ALL_KEYS",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
				{
					Negate:      true,
					MatcherType: "ALL_KEYS",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      75,
				Treatment: "on",
			},
			{
				Size:      25,
				Treatment: "off",
			},
		},
	}

	wrapped, err := NewCondition(&condition1, logger, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil))
	if err != nil {
		t.Error("err should be nil")
	}

	if wrapped.Label() != "Label2" {
		t.Error("Label not set properly")
	}

	if wrapped.combiner != "AND" {
		t.Error("Combiner not set properly")
	}

	if len(wrapped.matchers) != len(condition1.MatcherGroup.Matchers) {
		t.Error("Incorrect number of matchers")
	}

	if wrapped.ConditionType() != ConditionTypeRollout {
		t.Error("Incorrect condition type")
	}

	treatment1 := wrapped.CalculateTreatment(50)
	if treatment1 != nil && *treatment1 != "on" {
		t.Error("CalculateTreatment returned incorrect treatment")
	}

	treatment2 := wrapped.CalculateTreatment(80)
	if treatment2 != nil && *treatment2 != "off" {
		t.Error("CalculateTreatment returned incorrect treatment")
	}
}

func TestConditionUnsupportedMatcherWrapperObject(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	condition1 := dtos.ConditionDTO{
		ConditionType: "WHITELIST",
		Label:         "Label1",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					Negate:      false,
					MatcherType: "UNSUPPORTED",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
				{
					Negate:      true,
					MatcherType: "ALL_KEYS",
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      75,
				Treatment: "on",
			},
			{
				Size:      25,
				Treatment: "off",
			},
		},
	}

	_, err := NewCondition(&condition1, logger, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil))

	if err == nil {
		t.Error("err should not be nil")
	}

	if _, ok := err.(datatypes.UnsupportedMatcherError); !ok {
		t.Error("err should be UnsupportedMatcherError")
	}
}

func TestConditionMatcherWithNilStringWrapperObject(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	condition1 := dtos.ConditionDTO{
		ConditionType: "WHITELIST",
		Label:         "Label1",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					Negate:      false,
					MatcherType: constants.MatcherTypeStartsWith,
					KeySelector: &dtos.KeySelectorDTO{
						Attribute:   nil,
						TrafficType: "something",
					},
					Whitelist: nil,
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      75,
				Treatment: "on",
			},
			{
				Size:      25,
				Treatment: "off",
			},
		},
	}

	condition, err := NewCondition(&condition1, logger, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil))

	if err != nil {
		t.Error("err should be nil")
	}

	if len(condition.matchers) != 0 {
		t.Error("matchers should be empty")
	}
}

func TestNewRBCondition(t *testing.T) {
	tests := []struct {
		name        string
		condition   *dtos.RuleBasedConditionDTO
		wantErr     bool
		errContains string
	}{
		{
			name: "valid condition with ALL_KEYS matcher",
			condition: &dtos.RuleBasedConditionDTO{
				ConditionType: "MATCHES_STRING",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: "ALL_KEYS",
							KeySelector: &dtos.KeySelectorDTO{Attribute: nil},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "unsupported matcher type",
			condition: &dtos.RuleBasedConditionDTO{
				ConditionType: "MATCHES_STRING",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: "UNSUPPORTED_TYPE",
						},
					},
				},
			},
			wantErr:     true,
			errContains: "Unable to create matcher for matcher type: UNSUPPORTED_TYPE",
		},
		{
			name: "multiple matchers with AND combiner",
			condition: &dtos.RuleBasedConditionDTO{
				ConditionType: "MATCHES_STRING",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: "ALL_KEYS",
							KeySelector: &dtos.KeySelectorDTO{Attribute: nil},
						},
						{
							MatcherType: "WHITELIST",
							KeySelector: &dtos.KeySelectorDTO{Attribute: nil},
							Whitelist: &dtos.WhitelistMatcherDataDTO{
								Whitelist: []string{"key1"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	logger := logging.NewLogger(&logging.LoggerOptions{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewRBCondition(tt.condition, logger, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil))

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, cond)

			// Check fields
			assert.Equal(t, tt.condition.ConditionType, cond.conditionType)
			assert.Equal(t, tt.condition.MatcherGroup.Combiner, cond.combiner)
			assert.Len(t, cond.matchers, len(tt.condition.MatcherGroup.Matchers))
			assert.Empty(t, cond.partitions)

			// Test condition matches
			if tt.name == "valid condition with ALL_KEYS matcher" {
				assert.True(t, cond.Matches("key1", nil, nil))
				assert.True(t, cond.Matches("key2", nil, nil))
			} else if tt.name == "multiple matchers with AND combiner" {
				assert.True(t, cond.Matches("key1", nil, nil))
				assert.False(t, cond.Matches("key2", nil, nil))
			}
		})
	}
}
