package dtos

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ConvertConfigToSplit tests

func TestConvertConfigToSplitWithAllFields(t *testing.T) {
	config := ConfigDTO{
		Name:            "test_config",
		Status:          "ACTIVE",
		Killed:          false,
		TrafficTypeName: "account",
		ChangeNumber:    123456,
		Sets:            []string{"set1", "set2"},
		Variants: []VariantDTO{
			{
				Name:       "on",
				Definition: map[string]interface{}{"color": "red"},
			},
			{
				Name:       "off",
				Definition: map[string]interface{}{"color": "blue"},
			},
		},
		Targeting: TargetingDTO{
			Default:               "on",
			Seed:                  456,
			TrafficAllocation:     100,
			TrafficAllocationSeed: 789,
			Conditions: []RawConditionDTO{
				{
					Label: "custom rule",
					Partitions: []RawPartitionDTO{
						{
							Variant: "on",
							Size:    100,
						},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "WHITELIST",
							Attribute: "account_type",
							Data: map[string]interface{}{
								"strings": []interface{}{"premium", "enterprise"},
							},
						},
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
	assert.JSONEq(t, "{\"color\":\"red\"}", split.Configurations["on"])
	// Should have 2 conditions: 1 from targeting + 1 default rule
	assert.Equal(t, 2, len(split.Conditions))
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
	assert.Equal(t, "custom rule", split.Conditions[0].Label)
	// Verify default rule is always appended
	assert.Equal(t, "ROLLOUT", split.Conditions[1].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[1].Label)
	assert.Equal(t, "on", split.Conditions[1].Partitions[0].Treatment)
	assert.Equal(t, 2, len(split.Sets))
	assert.Equal(t, "set1", split.Sets[0])
}

func TestConvertConfigToSplitWithDefaults(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_config",
		ChangeNumber: 123456,
		Targeting: TargetingDTO{
			Seed: 456,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, "test_config", split.Name)
	assert.Equal(t, "ACTIVE", split.Status, "Status should default to ACTIVE")
	assert.Equal(t, "user", split.TrafficTypeName, "TrafficTypeName should default to user")
	assert.Equal(t, "control", split.DefaultTreatment, "DefaultTreatment should default to control")
	assert.Equal(t, 2, split.Algo, "Algo should always be 2")
	// Should have 1 default rule condition
	assert.Equal(t, 1, len(split.Conditions))
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[0].Label)
}

func TestConvertConfigToSplitWithEmptyConditions(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_config",
		ChangeNumber: 123456,
		Targeting: TargetingDTO{
			Default: "off",
			Seed:    456,
		},
	}

	split := ConvertConfigToSplit(config)

	// Empty conditions should still result in default rule being appended
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
		Name:         "test_config",
		ChangeNumber: 123456,
		Targeting: TargetingDTO{
			Default:    "control",
			Seed:       456,
			Conditions: nil,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, 1, len(split.Conditions), "Should create default condition when nil")
	assert.Equal(t, "control", split.Conditions[0].Partitions[0].Treatment)
}

func TestConvertConfigToSplitWithEmptyDefaultTreatment(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_config",
		ChangeNumber: 123456,
		Targeting: TargetingDTO{
			Default: "",
			Seed:    456,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, "control", split.DefaultTreatment, "Should use 'control' when empty")
	assert.Equal(t, "control", split.Conditions[0].Partitions[0].Treatment, "Default condition should use default treatment")
}

func TestConvertConfigToSplitKilledFlag(t *testing.T) {
	config := ConfigDTO{
		Name:         "killed_config",
		Killed:       true,
		ChangeNumber: 123456,
		Targeting: TargetingDTO{
			Seed: 456,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.Equal(t, true, split.Killed)
}

func TestConvertConfigToSplitWithConfigurations(t *testing.T) {
	config := ConfigDTO{
		Name:         "config_with_configs",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{
				Name:       "on",
				Definition: map[string]interface{}{"size": 10},
			},
			{
				Name:       "off",
				Definition: map[string]interface{}{"size": 20},
			},
			{
				Name:       "default",
				Definition: map[string]interface{}{"size": 15},
			},
		},
		Targeting: TargetingDTO{
			Seed: 456,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.NotNil(t, split.Configurations)
	assert.Equal(t, 3, len(split.Configurations))
	assert.JSONEq(t, "{\"size\":10}", split.Configurations["on"])
	assert.JSONEq(t, "{\"size\":20}", split.Configurations["off"])
	assert.JSONEq(t, "{\"size\":15}", split.Configurations["default"])
}

func TestConvertConfigToSplitWithNilConfigurations(t *testing.T) {
	config := ConfigDTO{
		Name:         "config_no_configs",
		ChangeNumber: 123456,
		Variants:     nil,
		Targeting: TargetingDTO{
			Seed: 456,
		},
	}

	split := ConvertConfigToSplit(config)

	assert.NotNil(t, split.Configurations)
	assert.Equal(t, 0, len(split.Configurations))
}

// FFResponseConfigs tests

func TestFFResponseConfigsNewFFResponseConfigs(t *testing.T) {
	jsonData := `{
		"updated": [
			{
				"name": "test_config",
				"status": "ACTIVE",
				"killed": false,
				"trafficTypeName": "user",
				"changeNumber": 150,
				"sets": ["set1"],
				"variants": [
					{
						"name": "on",
						"definition": {"color": "blue"}
					}
				],
				"targeting": {
					"default": "on",
					"seed": 777,
					"trafficAllocation": 100,
					"trafficAllocationSeed": 999,
					"conditions": []
				}
			}
		],
		"since": 100,
		"till": 200,
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
		"updated": [],
		"since": 100,
		"till": 200,
		"rbs": []
	}`

	ffResponse1, _ := NewFFResponseConfigs([]byte(jsonData1))
	assert.False(t, ffResponse1.NeedsAnotherFetch())

	// Test when since == till (no more data)
	jsonData2 := `{
		"updated": [],
		"since": 100,
		"till": 100,
		"rbs": []
	}`

	ffResponse2, _ := NewFFResponseConfigs([]byte(jsonData2))
	assert.True(t, ffResponse2.NeedsAnotherFetch())
}

func TestFFResponseConfigsFeatureFlags(t *testing.T) {
	jsonData := `{
		"updated": [
			{
				"name": "config1",
				"changeNumber": 150,
				"targeting": {
					"default": "on",
					"seed": 777
				}
			},
			{
				"name": "config2",
				"changeNumber": 160,
				"targeting": {
					"default": "off",
					"seed": 888
				}
			}
		],
		"since": 100,
		"till": 200,
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
		"updated": [],
		"since": 100,
		"till": 200,
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
		"updated": [],
		"since": 0,
		"till": 0,
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
		"updated": [],
		"since": 100,
		"till": 200,
		"rbs": []
	}`

	ffResponse, _ := NewFFResponseConfigs([]byte(jsonData))

	// Verify it implements FFResponse interface
	var _ FFResponse = ffResponse
	assert.NotNil(t, ffResponse)
}

func TestConvertConfigToSplitWithVariantDefinitions(t *testing.T) {
	// Test that Variant Definitions (directives) are properly marshaled to JSON strings
	config := ConfigDTO{
		Name:         "test_variants",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{
				Name: "treatment_a",
				Definition: map[string]interface{}{
					"action":     "redirect",
					"url":        "https://example.com",
					"statusCode": 302,
					"enabled":    true,
				},
			},
			{
				Name: "treatment_b",
				Definition: map[string]interface{}{
					"action": "render",
					"template": map[string]interface{}{
						"name":  "banner.html",
						"theme": "dark",
					},
				},
			},
		},
		Targeting: TargetingDTO{
			Default: "treatment_a",
			Seed:    999,
		},
	}

	split := ConvertConfigToSplit(config)

	// Verify configurations are created from variant definitions
	assert.Equal(t, 2, len(split.Configurations))
	assert.Contains(t, split.Configurations, "treatment_a")
	assert.Contains(t, split.Configurations, "treatment_b")

	// Verify the JSON is properly formatted (Definition acts as directive for evaluator)
	var configA map[string]interface{}
	err := json.Unmarshal([]byte(split.Configurations["treatment_a"]), &configA)
	assert.NoError(t, err)
	assert.Equal(t, "redirect", configA["action"])
	assert.Equal(t, "https://example.com", configA["url"])
	assert.Equal(t, float64(302), configA["statusCode"])
	assert.Equal(t, true, configA["enabled"])

	var configB map[string]interface{}
	err = json.Unmarshal([]byte(split.Configurations["treatment_b"]), &configB)
	assert.NoError(t, err)
	assert.Equal(t, "render", configB["action"])
	template := configB["template"].(map[string]interface{})
	assert.Equal(t, "banner.html", template["name"])
	assert.Equal(t, "dark", template["theme"])
}

func TestFFResponseConfigsWithAllConfigFields(t *testing.T) {
	config := ConfigDTO{
		Name:            "full_config",
		Status:          "ACTIVE",
		Killed:          false,
		TrafficTypeName: "account",
		ChangeNumber:    500,
		Sets:            []string{"set1", "set2"},
		Variants: []VariantDTO{
			{
				Name:       "premium",
				Definition: map[string]interface{}{"features": []interface{}{"a", "b"}},
			},
			{
				Name:       "free",
				Definition: map[string]interface{}{"features": []interface{}{"a"}},
			},
		},
		Targeting: TargetingDTO{
			Default:               "premium",
			Seed:                  67890,
			TrafficAllocation:     100,
			TrafficAllocationSeed: 12345,
			Conditions: []RawConditionDTO{
				{
					Label: "whitelisted users",
					Partitions: []RawPartitionDTO{
						{
							Variant: "premium",
							Size:    100,
						},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "WHITELIST",
							Attribute: "user_type",
							Data: map[string]interface{}{
								"strings": []interface{}{"premium", "vip"},
							},
						},
					},
				},
			},
		},
	}

	responseDTO := ConfigsResponseDTO{
		Updated: []ConfigDTO{config},
		Since:   100,
		Till:    200,
		RBS:     []RuleBasedSegmentDTO{},
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
	// Should have 2 conditions: 1 from targeting + 1 default rule
	assert.Equal(t, 2, len(split.Conditions))
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
	assert.Equal(t, "ROLLOUT", split.Conditions[1].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[1].Label)
	assert.Equal(t, 2, len(split.Sets))
}

func TestConvertConfigWithWhitelistMatcher(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_whitelist",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{Name: "on", Definition: map[string]interface{}{"color": "blue"}},
		},
		Targeting: TargetingDTO{
			Default: "on",
			Seed:    12345,
			Conditions: []RawConditionDTO{
				{
					Label: "premium_users",
					Partitions: []RawPartitionDTO{
						{Variant: "on", Size: 100},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "WHITELIST",
							Attribute: "account_type",
							Data: map[string]interface{}{
								"strings": []interface{}{"premium", "enterprise"},
							},
						},
					},
				},
			},
		},
	}

	split := ConvertConfigToSplit(config)

	// Verify condition type is WHITELIST
	assert.Equal(t, 2, len(split.Conditions))
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
	assert.Equal(t, "premium_users", split.Conditions[0].Label)

	// Verify matcher conversion
	matcher := split.Conditions[0].MatcherGroup.Matchers[0]
	assert.Equal(t, "WHITELIST", matcher.MatcherType)
	assert.False(t, matcher.Negate)
	assert.NotNil(t, matcher.KeySelector)
	assert.Equal(t, "user", matcher.KeySelector.TrafficType)
	assert.Equal(t, "account_type", *matcher.KeySelector.Attribute)
	assert.NotNil(t, matcher.Whitelist)
	assert.Equal(t, 2, len(matcher.Whitelist.Whitelist))
	assert.Contains(t, matcher.Whitelist.Whitelist, "premium")
	assert.Contains(t, matcher.Whitelist.Whitelist, "enterprise")

	// Verify partition variant->treatment mapping
	assert.Equal(t, "on", split.Conditions[0].Partitions[0].Treatment)
}

func TestConvertConfigWithNumericMatcher(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_numeric",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{Name: "treatment", Definition: map[string]interface{}{"discount": 15}},
		},
		Targeting: TargetingDTO{
			Default: "control",
			Seed:    98765,
			Conditions: []RawConditionDTO{
				{
					Label: "high_value_segment",
					Partitions: []RawPartitionDTO{
						{Variant: "treatment", Size: 100},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "GREATER_THAN_OR_EQUAL_TO",
							Attribute: "total_purchases",
							Data: map[string]interface{}{
								"type":   "NUMERIC",
								"number": 5.0,
							},
						},
					},
				},
			},
		},
	}

	split := ConvertConfigToSplit(config)

	// Verify condition type is ROLLOUT (not WHITELIST)
	assert.Equal(t, 2, len(split.Conditions))
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)
	assert.Equal(t, "high_value_segment", split.Conditions[0].Label)

	// Verify matcher conversion
	matcher := split.Conditions[0].MatcherGroup.Matchers[0]
	assert.Equal(t, "GREATER_THAN_OR_EQUAL_TO", matcher.MatcherType)
	assert.False(t, matcher.Negate)
	assert.NotNil(t, matcher.KeySelector)
	assert.Equal(t, "user", matcher.KeySelector.TrafficType)
	assert.Equal(t, "total_purchases", *matcher.KeySelector.Attribute)
	assert.NotNil(t, matcher.UnaryNumeric)
	assert.Equal(t, "NUMERIC", matcher.UnaryNumeric.DataType)
	assert.Equal(t, int64(5), matcher.UnaryNumeric.Value)
}

func TestConvertConfigWithIsEqualToMatcher(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_equal",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{Name: "on", Definition: map[string]interface{}{}},
		},
		Targeting: TargetingDTO{
			Default: "off",
			Seed:    11111,
			Conditions: []RawConditionDTO{
				{
					Label: "specific_version",
					Partitions: []RawPartitionDTO{
						{Variant: "on", Size: 100},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "IS_EQUAL_TO",
							Attribute: "version",
							Data: map[string]interface{}{
								"type":   "NUMERIC",
								"number": 2.0,
							},
						},
					},
				},
			},
		},
	}

	split := ConvertConfigToSplit(config)

	// IS_EQUAL_TO should map to EQUAL_TO
	assert.Equal(t, 2, len(split.Conditions))
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)

	matcher := split.Conditions[0].MatcherGroup.Matchers[0]
	assert.Equal(t, "EQUAL_TO", matcher.MatcherType)
	assert.NotNil(t, matcher.UnaryNumeric)
	assert.Equal(t, "NUMERIC", matcher.UnaryNumeric.DataType)
	assert.Equal(t, int64(2), matcher.UnaryNumeric.Value)
}

func TestConvertConfigWithMultipleConditions(t *testing.T) {
	config := ConfigDTO{
		Name:         "test_multiple",
		ChangeNumber: 123456,
		Variants: []VariantDTO{
			{Name: "on", Definition: map[string]interface{}{}},
			{Name: "off", Definition: map[string]interface{}{}},
		},
		Targeting: TargetingDTO{
			Default: "off",
			Seed:    99999,
			Conditions: []RawConditionDTO{
				{
					Label: "whitelist_condition",
					Partitions: []RawPartitionDTO{
						{Variant: "on", Size: 100},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "WHITELIST",
							Attribute: "user_id",
							Data: map[string]interface{}{
								"strings": []interface{}{"user1", "user2"},
							},
						},
					},
				},
				{
					Label: "rollout_condition",
					Partitions: []RawPartitionDTO{
						{Variant: "on", Size: 50},
						{Variant: "off", Size: 50},
					},
					Matchers: []RawMatcherDTO{
						{
							Type:      "GREATER_THAN_OR_EQUAL_TO",
							Attribute: "age",
							Data: map[string]interface{}{
								"type":   "NUMERIC",
								"number": 18.0,
							},
						},
					},
				},
			},
		},
	}

	split := ConvertConfigToSplit(config)

	// Should have 3 conditions: 2 from targeting + 1 default rule
	assert.Equal(t, 3, len(split.Conditions))

	// First condition should be WHITELIST
	assert.Equal(t, "WHITELIST", split.Conditions[0].ConditionType)
	assert.Equal(t, "whitelist_condition", split.Conditions[0].Label)

	// Second condition should be ROLLOUT
	assert.Equal(t, "ROLLOUT", split.Conditions[1].ConditionType)
	assert.Equal(t, "rollout_condition", split.Conditions[1].Label)
	assert.Equal(t, 2, len(split.Conditions[1].Partitions))

	// Third condition should be default rule
	assert.Equal(t, "ROLLOUT", split.Conditions[2].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[2].Label)
	assert.Equal(t, "off", split.Conditions[2].Partitions[0].Treatment)
}

func TestConvertConfigWithRealJSONExample(t *testing.T) {
	// Test with the real JSON structure provided by the user
	jsonData := []byte(`{
		"updated": [
			{
				"name": "feature_new_checkout",
				"variants": [
					{
						"name": "on",
						"definition": {
							"color": "blue",
							"buttonText": "Complete Purchase"
						}
					},
					{
						"name": "off",
						"definition": {
							"color": "green",
							"buttonText": "Buy Now"
						}
					}
				],
				"targeting": {
					"default": "off",
					"seed": 12345,
					"trafficAllocation": 100,
					"trafficAllocationSeed": 67890,
					"conditions": [
						{
							"label": "premium_users",
							"partitions": [
								{
									"variant": "on",
									"size": 50
								},
								{
									"variant": "off",
									"size": 50
								}
							],
							"matchers": [
								{
									"type": "WHITELIST",
									"attribute": "account_type",
									"data": {
										"strings": ["premium", "enterprise"]
									}
								}
							]
						}
					]
				},
				"trafficTypeName": "user",
				"changeNumber": 1712345678,
				"version": 1,
				"status": "ACTIVE",
				"killed": false,
				"sets": ["checkout_features", "ui_experiments"]
			}
		],
		"since": 1712345600,
		"till": 1712349200
	}`)

	ffResponse, err := NewFFResponseConfigs(jsonData)
	assert.NoError(t, err)
	assert.NotNil(t, ffResponse)

	splits := ffResponse.FeatureFlags()
	assert.Equal(t, 1, len(splits))

	split := splits[0]
	assert.Equal(t, "feature_new_checkout", split.Name)
	assert.Equal(t, "ACTIVE", split.Status)
	assert.False(t, split.Killed)
	assert.Equal(t, "user", split.TrafficTypeName)
	assert.Equal(t, "off", split.DefaultTreatment)
	assert.Equal(t, int64(1712345678), split.ChangeNumber)
	assert.Equal(t, 100, split.TrafficAllocation)
	assert.Equal(t, int64(67890), split.TrafficAllocationSeed)
	assert.Equal(t, int64(12345), split.Seed)
	assert.Equal(t, 2, split.Algo)

	// Verify configurations
	assert.Equal(t, 2, len(split.Configurations))
	assert.Contains(t, split.Configurations, "on")
	assert.Contains(t, split.Configurations, "off")

	// Verify conditions: 1 from targeting + 1 default rule
	assert.Equal(t, 2, len(split.Conditions))

	// First condition
	cond1 := split.Conditions[0]
	assert.Equal(t, "WHITELIST", cond1.ConditionType)
	assert.Equal(t, "premium_users", cond1.Label)
	assert.Equal(t, "AND", cond1.MatcherGroup.Combiner)
	assert.Equal(t, 1, len(cond1.MatcherGroup.Matchers))

	// Verify matcher
	matcher := cond1.MatcherGroup.Matchers[0]
	assert.Equal(t, "WHITELIST", matcher.MatcherType)
	assert.False(t, matcher.Negate)
	assert.NotNil(t, matcher.KeySelector)
	assert.Equal(t, "user", matcher.KeySelector.TrafficType)
	assert.Equal(t, "account_type", *matcher.KeySelector.Attribute)
	assert.NotNil(t, matcher.Whitelist)
	assert.Equal(t, 2, len(matcher.Whitelist.Whitelist))
	assert.Contains(t, matcher.Whitelist.Whitelist, "premium")
	assert.Contains(t, matcher.Whitelist.Whitelist, "enterprise")

	// Verify partitions (variant->treatment mapping)
	assert.Equal(t, 2, len(cond1.Partitions))
	assert.Equal(t, "on", cond1.Partitions[0].Treatment)
	assert.Equal(t, 50, cond1.Partitions[0].Size)
	assert.Equal(t, "off", cond1.Partitions[1].Treatment)
	assert.Equal(t, 50, cond1.Partitions[1].Size)

	// Second condition (default rule)
	cond2 := split.Conditions[1]
	assert.Equal(t, "ROLLOUT", cond2.ConditionType)
	assert.Equal(t, "default rule", cond2.Label)
	assert.Equal(t, 1, len(cond2.MatcherGroup.Matchers))
	assert.Equal(t, "ALL_KEYS", cond2.MatcherGroup.Matchers[0].MatcherType)
	assert.Equal(t, 1, len(cond2.Partitions))
	assert.Equal(t, "off", cond2.Partitions[0].Treatment)
	assert.Equal(t, 100, cond2.Partitions[0].Size)

	// Verify sets
	assert.Equal(t, 2, len(split.Sets))
	assert.Contains(t, split.Sets, "checkout_features")
	assert.Contains(t, split.Sets, "ui_experiments")
}
