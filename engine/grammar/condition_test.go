package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers/datatypes"

	"github.com/splitio/go-toolkit/v5/logging"
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

	wrapped, err := NewCondition(&condition1, nil, logger)

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

	wrapped, err := NewCondition(&condition1, nil, logger)
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

	_, err := NewCondition(&condition1, nil, logger)

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
					MatcherType: matchers.MatcherTypeStartsWith,
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

	condition, err := NewCondition(&condition1, nil, logger)

	if err != nil {
		t.Error("err should be nil")
	}

	if len(condition.matchers) != 0 {
		t.Error("matchers should be empty")
	}
}
