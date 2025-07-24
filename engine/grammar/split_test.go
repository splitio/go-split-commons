package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/condition"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSplitCreation(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := dtos.SplitDTO{
		Algo:                  1,
		ChangeNumber:          123,
		Conditions:            []dtos.ConditionDTO{},
		DefaultTreatment:      "def",
		Killed:                false,
		Name:                  "split1",
		Seed:                  1234,
		Status:                "ACTIVE",
		TrafficAllocation:     100,
		TrafficAllocationSeed: 333,
		TrafficTypeName:       "tt1",
	}
	split := NewSplit(&dto, nil, logger)

	if split.Algo() != condition.SplitAlgoLegacy {
		t.Error("Algo() should return legacy")
	}

	if split.ChangeNumber() != 123 {
		t.Error("Incorrect changenumber returned")
	}

	if split.DefaultTreatment() != "def" {
		t.Error("Incorrect default treatment")
	}

	if split.Killed() {
		t.Error("Split should not be marked as killed")
	}

	if split.Seed() != 1234 {
		t.Error("Incorrect seed")
	}

	if split.Name() != "split1" {
		t.Error("Incorrect split name")
	}

	if split.Status() != condition.SplitStatusActive {
		t.Error("Status should be active")
	}

	if split.TrafficAllocation() != 100 {
		t.Error("Traffic allocation should be 100")
	}
}

func TestSplitCreationWithConditionsMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := dtos.SplitDTO{
		Algo:         1,
		ChangeNumber: 123,
		Conditions: []dtos.ConditionDTO{{
			ConditionType: condition.ConditionTypeWhitelist,
			Label:         "test",
			Partitions:    []dtos.PartitionDTO{{Treatment: "off", Size: 100}},
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{{MatcherType: matchers.MatcherTypeAllKeys, Negate: false}},
			},
		}, {
			ConditionType: condition.ConditionTypeWhitelist,
			Label:         "test",
			Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{{MatcherType: matchers.MatcherTypeAllKeys, Negate: false}},
			},
		}},
		DefaultTreatment:      "def",
		Killed:                false,
		Name:                  "split1",
		Seed:                  1234,
		Status:                "ACTIVE",
		TrafficAllocation:     100,
		TrafficAllocationSeed: 333,
		TrafficTypeName:       "tt1",
	}
	split := NewSplit(&dto, nil, logger)

	if split.Algo() != condition.SplitAlgoLegacy {
		t.Error("Algo() should return legacy")
	}

	if split.ChangeNumber() != 123 {
		t.Error("Incorrect changenumber returned")
	}

	if split.DefaultTreatment() != "def" {
		t.Error("Incorrect default treatment")
	}

	if split.Killed() {
		t.Error("Split should not be marked as killed")
	}

	if split.Seed() != 1234 {
		t.Error("Incorrect seed")
	}

	if split.Name() != "split1" {
		t.Error("Incorrect split name")
	}

	if split.Status() != condition.SplitStatusActive {
		t.Error("Status should be active")
	}

	if split.TrafficAllocation() != 100 {
		t.Error("Traffic allocation should be 100")
	}

	if len(split.conditions) != 2 {
		t.Error("conditions length should be 2")
	}
}

func TestSplitCreationWithUnsupportedMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := dtos.SplitDTO{
		Algo:         1,
		ChangeNumber: 123,
		Conditions: []dtos.ConditionDTO{{
			ConditionType: condition.ConditionTypeWhitelist,
			Label:         "test",
			Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{{MatcherType: "unssuported", Negate: false}},
			},
		}, {
			ConditionType: condition.ConditionTypeWhitelist,
			Label:         "test",
			Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{{MatcherType: matchers.MatcherTypeAllKeys, Negate: false}},
			},
		}},
		DefaultTreatment:      "def",
		Killed:                false,
		Name:                  "split1",
		Seed:                  1234,
		Status:                "ACTIVE",
		TrafficAllocation:     100,
		TrafficAllocationSeed: 333,
		TrafficTypeName:       "tt1",
	}
	split := NewSplit(&dto, nil, logger)

	if split.Algo() != condition.SplitAlgoLegacy {
		t.Error("Algo() should return legacy")
	}

	if split.ChangeNumber() != 123 {
		t.Error("Incorrect changenumber returned")
	}

	if split.DefaultTreatment() != "def" {
		t.Error("Incorrect default treatment")
	}

	if split.Killed() {
		t.Error("Split should not be marked as killed")
	}

	if split.Seed() != 1234 {
		t.Error("Incorrect seed")
	}

	if split.Name() != "split1" {
		t.Error("Incorrect split name")
	}

	if split.Status() != condition.SplitStatusActive {
		t.Error("Status should be active")
	}

	if split.TrafficAllocation() != 100 {
		t.Error("Traffic allocation should be 100")
	}

	if len(split.conditions) != 1 {
		t.Error("conditions length should be 1")
	}

	if split.conditions[0].Combiner != "AND" {
		t.Error("combiner should be AND")
	}
}
