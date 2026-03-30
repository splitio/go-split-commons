package dtos

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ConvertConfigToSplit tests

func TestConvertConfigToSplitWithAllFields(t *testing.T) {
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

func TestConvertConfigToSplitWithEmptyConditions(t *testing.T) {
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

func TestConvertConfigToSplitWithNilConditions(t *testing.T) {
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

func TestConvertConfigToSplitWithEmptyDefaultTreatment(t *testing.T) {
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

func TestConvertConfigToSplitWithConfigurations(t *testing.T) {
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

func TestConvertConfigToSplitWithNilConfigurations(t *testing.T) {
	config := ConfigDTO{
		Name:           "config_no_configs",
		ChangeNumber:   123456,
		Seed:           456,
		Configurations: nil,
	}

	split := ConvertConfigToSplit(config)

	assert.Nil(t, split.Configurations)
}

// FFResponseConfigs tests

func TestFFResponseConfigsNewFFResponseConfigs(t *testing.T) {
	jsonData := `{
		"configs": {
			"s": 100,
			"t": 200,
			"d": [
				{
					"name": "test_config",
					"status": "ACTIVE",
					"killed": false,
					"trafficTypeName": "user",
					"defaultTreatment": "on",
					"changeNumber": 150,
					"trafficAllocation": 100,
					"trafficAllocationSeed": 999,
					"seed": 777,
					"configurations": {
						"on": "{\"color\": \"blue\"}"
					},
					"conditions": []
				}
			]
		},
		"rbs": [
			{
				"name": "segment1",
				"changeNumber": 300
			}
		]
	}`

	ffResponse, err := NewFFResponseConfigs([]byte(jsonData))

	assert.NoError(t, err)
	assert.NotNil(t, ffResponse)
	assert.Equal(t, int64(100), ffResponse.FFSince())
	assert.Equal(t, int64(200), ffResponse.FFTill())
	assert.Equal(t, int64(100), ffResponse.RBSince())
	assert.Equal(t, int64(200), ffResponse.RBTill())
	assert.Equal(t, 1, len(ffResponse.FeatureFlags()))
	assert.Equal(t, 1, len(ffResponse.RuleBasedSegments()))
}

func TestFFResponseConfigsNeedsAnotherFetch(t *testing.T) {
	// Test when since != till (needs fetch)
	jsonData1 := `{
		"configs": {
			"s": 100,
			"t": 200,
			"d": []
		},
		"rbs": []
	}`

	ffResponse1, _ := NewFFResponseConfigs([]byte(jsonData1))
	assert.False(t, ffResponse1.NeedsAnotherFetch())

	// Test when since == till (no more data)
	jsonData2 := `{
		"configs": {
			"s": 100,
			"t": 100,
			"d": []
		},
		"rbs": []
	}`

	ffResponse2, _ := NewFFResponseConfigs([]byte(jsonData2))
	assert.True(t, ffResponse2.NeedsAnotherFetch())
}

func TestFFResponseConfigsFeatureFlags(t *testing.T) {
	jsonData := `{
		"configs": {
			"s": 100,
			"t": 200,
			"d": [
				{
					"name": "config1",
					"defaultTreatment": "on",
					"changeNumber": 150,
					"seed": 777
				},
				{
					"name": "config2",
					"defaultTreatment": "off",
					"changeNumber": 160,
					"seed": 888
				}
			]
		},
		"rbs": []
	}`

	ffResponse, err := NewFFResponseConfigs([]byte(jsonData))
	assert.NoError(t, err)

	splits := ffResponse.FeatureFlags()
	assert.Equal(t, 2, len(splits))
	assert.Equal(t, "config1", splits[0].Name)
	assert.Equal(t, "on", splits[0].DefaultTreatment)
	assert.Equal(t, "config2", splits[1].Name)
	assert.Equal(t, "off", splits[1].DefaultTreatment)

	// Verify defaults are applied
	assert.Equal(t, "ACTIVE", splits[0].Status)
	assert.Equal(t, "user", splits[0].TrafficTypeName)
	assert.Equal(t, 2, splits[0].Algo)
	assert.Equal(t, 1, len(splits[0].Conditions))
}

func TestFFResponseConfigsRuleBasedSegments(t *testing.T) {
	jsonData := `{
		"configs": {
			"s": 100,
			"t": 200,
			"d": []
		},
		"rbs": [
			{
				"name": "segment1",
				"changeNumber": 300
			},
			{
				"name": "segment2",
				"changeNumber": 400
			}
		]
	}`

	ffResponse, err := NewFFResponseConfigs([]byte(jsonData))
	assert.NoError(t, err)

	rbs := ffResponse.RuleBasedSegments()
	assert.Equal(t, 2, len(rbs))
	assert.Equal(t, "segment1", rbs[0].Name)
	assert.Equal(t, int64(300), rbs[0].ChangeNumber)
	assert.Equal(t, "segment2", rbs[1].Name)
	assert.Equal(t, int64(400), rbs[1].ChangeNumber)
}

func TestFFResponseConfigsInvalidJSON(t *testing.T) {
	jsonData := `invalid json`

	ffResponse, err := NewFFResponseConfigs([]byte(jsonData))
	assert.Error(t, err)
	assert.Nil(t, ffResponse)
}

func TestFFResponseConfigsEmptyResponse(t *testing.T) {
	jsonData := `{
		"configs": {
			"s": 0,
			"t": 0,
			"d": []
		},
		"rbs": []
	}`

	ffResponse, err := NewFFResponseConfigs([]byte(jsonData))
	assert.NoError(t, err)
	assert.NotNil(t, ffResponse)
	assert.Equal(t, int64(0), ffResponse.FFSince())
	assert.Equal(t, int64(0), ffResponse.FFTill())
	assert.Equal(t, 0, len(ffResponse.FeatureFlags()))
	assert.Equal(t, 0, len(ffResponse.RuleBasedSegments()))
	assert.True(t, ffResponse.NeedsAnotherFetch())
}

func TestFFResponseConfigs_ImplementsFFResponse(t *testing.T) {
	jsonData := `{
		"configs": {
			"s": 100,
			"t": 200,
			"d": []
		},
		"rbs": []
	}`

	ffResponse, _ := NewFFResponseConfigs([]byte(jsonData))

	// Verify it implements FFResponse interface
	var _ FFResponse = ffResponse
	assert.NotNil(t, ffResponse)
}

func TestFFResponseConfigsWithAllConfigFields(t *testing.T) {
	config := ConfigDTO{
		Name:                  "full_config",
		Status:                "ACTIVE",
		Killed:                false,
		TrafficTypeName:       "account",
		DefaultTreatment:      "premium",
		ChangeNumber:          500,
		TrafficAllocation:     100,
		TrafficAllocationSeed: 12345,
		Seed:                  67890,
		Configurations: map[string]string{
			"premium": "{\"features\": [\"a\", \"b\"]}",
			"free":    "{\"features\": [\"a\"]}",
		},
		Conditions: []ConditionDTO{
			{
				ConditionType: "WHITELIST",
				Label:         "whitelisted users",
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
						Treatment: "premium",
						Size:      100,
					},
				},
			},
		},
	}

	responseDTO := ConfigsResponseDTO{
		Configs: ConfigsDataDTO{
			Since:   100,
			Till:    200,
			Configs: []ConfigDTO{config},
		},
		RBS: []RuleBasedSegmentDTO{},
	}

	jsonData, _ := json.Marshal(responseDTO)
	ffResponse, err := NewFFResponseConfigs(jsonData)

	assert.NoError(t, err)
	assert.NotNil(t, ffResponse)

	splits := ffResponse.FeatureFlags()
	assert.Equal(t, 1, len(splits))

	split := splits[0]
	assert.Equal(t, "full_config", split.Name)
	assert.Equal(t, "ACTIVE", split.Status)
	assert.Equal(t, false, split.Killed)
	assert.Equal(t, "account", split.TrafficTypeName)
	assert.Equal(t, "premium", split.DefaultTreatment)
	assert.Equal(t, int64(500), split.ChangeNumber)
	assert.Equal(t, 100, split.TrafficAllocation)
	assert.Equal(t, int64(12345), split.TrafficAllocationSeed)
	assert.Equal(t, int64(67890), split.Seed)
	assert.Equal(t, 2, split.Algo)
	assert.Equal(t, 2, len(split.Configurations))
	assert.Equal(t, 1, len(split.Conditions))
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
}
