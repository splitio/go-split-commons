package dtos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertConfigToSplit_WithAllFields(t *testing.T) {
	config := ConfigDTO{
		Name:                  "test_config",
		Status:                "ACTIVE",
		Killed:                false,
		TrafficTypeName:       "account",
		DefaultTreatment:      "on",
		ChangeNumber:          123456,
		TrafficAllocation:     100,
		TrafficAllocationSeed: 789,
		Seed:                  456,
		Configurations: map[string]string{
			"on":  "{\"color\": \"red\"}",
			"off": "{\"color\": \"blue\"}",
		},
		Conditions: []ConditionDTO{
			{
				ConditionType: "WHITELIST",
				Label:         "custom rule",
				MatcherGroup: MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []MatcherDTO{
						{
							MatcherType: "WHITELIST",
							Negate:      false,
						},
					},
				},
				Partitions: []PartitionDTO{
					{
						Treatment: "on",
						Size:      100,
					},
				},
			},
		},
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, "test_config", split.Name)
	assert.Equal(t, "ACTIVE", split.Status)
	assert.Equal(t, false, split.Killed)
	assert.Equal(t, "account", split.TrafficTypeName)
	assert.Equal(t, "on", split.DefaultTreatment)
	assert.Equal(t, int64(123456), split.ChangeNumber)
	assert.Equal(t, 100, split.TrafficAllocation)
	assert.Equal(t, int64(789), split.TrafficAllocationSeed)
	assert.Equal(t, int64(456), split.Seed)
	assert.Equal(t, 2, split.Algo)
	assert.Equal(t, 2, len(split.Configurations))
	assert.Equal(t, "{\"color\": \"red\"}", split.Configurations["on"])
	assert.Equal(t, 1, len(split.Conditions))
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
}

func TestConvertConfigToSplit_WithDefaults(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_config",
		ChangeNumber: 123456,
		Seed:         456,
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, "test_config", split.Name)
	assert.Equal(t, "ACTIVE", split.Status, "Status should default to ACTIVE")
	assert.Equal(t, "user", split.TrafficTypeName, "TrafficTypeName should default to user")
	assert.Equal(t, "default", split.DefaultTreatment, "DefaultTreatment should default to default")
	assert.Equal(t, 2, split.Algo, "Algo should always be 2")
}

func TestConvertConfigToSplit_WithEmptyConditions(t *testing.T) {
	config := ConfigDTO{
		Name:             "test_config",
		DefaultTreatment: "off",
		ChangeNumber:     123456,
		Seed:             456,
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, 1, len(split.Conditions), "Should create default condition")
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[0].Label)
	assert.Equal(t, "AND", split.Conditions[0].MatcherGroup.Combiner)
	assert.Equal(t, 1, len(split.Conditions[0].MatcherGroup.Matchers))
	assert.Equal(t, "ALL_KEYS", split.Conditions[0].MatcherGroup.Matchers[0].MatcherType)
	assert.Equal(t, false, split.Conditions[0].MatcherGroup.Matchers[0].Negate)
	assert.Equal(t, 1, len(split.Conditions[0].Partitions))
	assert.Equal(t, "off", split.Conditions[0].Partitions[0].Treatment)
	assert.Equal(t, 100, split.Conditions[0].Partitions[0].Size)
}

func TestConvertConfigToSplit_WithNilConditions(t *testing.T) {
	config := ConfigDTO{
		Name:             "test_config",
		DefaultTreatment: "control",
		ChangeNumber:     123456,
		Seed:             456,
		Conditions:       nil,
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, 1, len(split.Conditions), "Should create default condition when nil")
	assert.Equal(t, "control", split.Conditions[0].Partitions[0].Treatment)
}

func TestConvertConfigToSplit_WithEmptyDefaultTreatment(t *testing.T) {
	config := ConfigDTO{
		Name:             "test_config",
		DefaultTreatment: "",
		ChangeNumber:     123456,
		Seed:             456,
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, "default", split.DefaultTreatment, "Should use 'default' when empty")
	assert.Equal(t, "default", split.Conditions[0].Partitions[0].Treatment, "Default condition should use default treatment")
}

func TestConvertConfigToSplit_KilledFlag(t *testing.T) {
	config := ConfigDTO{
		Name:         "killed_config",
		Killed:       true,
		ChangeNumber: 123456,
		Seed:         456,
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, true, split.Killed)
}

func TestConvertConfigToSplit_WithConfigurations(t *testing.T) {
	config := ConfigDTO{
		Name:         "config_with_configs",
		ChangeNumber: 123456,
		Seed:         456,
		Configurations: map[string]string{
			"on":      "{\"size\": 10}",
			"off":     "{\"size\": 20}",
			"default": "{\"size\": 15}",
		},
	}

	split := ConvertConfigToSplit(config)

	assert.NotNil(t, split.Configurations)
	assert.Equal(t, 3, len(split.Configurations))
	assert.Equal(t, "{\"size\": 10}", split.Configurations["on"])
	assert.Equal(t, "{\"size\": 20}", split.Configurations["off"])
	assert.Equal(t, "{\"size\": 15}", split.Configurations["default"])
}

func TestConvertConfigToSplit_WithNilConfigurations(t *testing.T) {
	config := ConfigDTO{
		Name:           "config_no_configs",
		ChangeNumber:   123456,
		Seed:           456,
		Configurations: nil,
	}

	split := ConvertConfigToSplit(config)

	assert.Nil(t, split.Configurations)
}
