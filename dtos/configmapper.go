package dtos

import "encoding/json"

// ConvertConfigToSplit converts a ConfigDTO to a SplitDTO
// This mapper bridges the /configs endpoint response to the internal Split representation
// used by the evaluator.
func ConvertConfigToSplit(config ConfigDTO) SplitDTO {
	// Apply defaults as per SDK spec
	trafficTypeName := config.TrafficTypeName
	if trafficTypeName == "" {
		trafficTypeName = "user"
	}

	// Default to ACTIVE if status is missing
	status := config.Status
	if status == "" {
		status = "ACTIVE"
	}

	// Map Targeting.Default to DefaultTreatment
	defaultTreatment := config.Targeting.Default
	if defaultTreatment == "" {
		defaultTreatment = "control"
	}

	// Build configurations map from Variants
	// Each variant's Definition (acts as directive for evaluator) is marshaled to JSON
	configurations := make(map[string]string)
	for _, variant := range config.Variants {
		if variant.Definition != nil {
			// Marshal the Definition (directive) to JSON string
			definitionJSON, err := json.Marshal(variant.Definition)
			if err == nil {
				configurations[variant.Name] = string(definitionJSON)
			}
		}
	}

	// Transform raw conditions from /configs format to evaluator ConditionDTO format
	conditions := make([]ConditionDTO, 0, len(config.Targeting.Conditions)+1)
	for _, rawCondition := range config.Targeting.Conditions {
		conditions = append(conditions, convertRawCondition(rawCondition))
	}

	// ALWAYS append default rule condition at the end
	conditions = append(conditions, ConditionDTO{
		ConditionType: "ROLLOUT",
		Label:         "default rule",
		MatcherGroup: MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []MatcherDTO{
				{
					MatcherType: "ALL_KEYS",
					Negate:      false,
					KeySelector: nil,
				},
			},
		},
		Partitions: []PartitionDTO{
			{
				Treatment: defaultTreatment,
				Size:      100,
			},
		},
	})

	// Build the SplitDTO for the evaluator
	return SplitDTO{
		Name:                  config.Name,
		Killed:                config.Killed, // Defaults to false if missing in JSON
		ChangeNumber:          config.ChangeNumber,
		Configurations:        configurations,
		DefaultTreatment:      defaultTreatment,
		TrafficTypeName:       trafficTypeName,
		Status:                status,
		Algo:                  2, // Explicitly set to 2 (Murmur3) - not provided by /configs endpoint
		Seed:                  config.Targeting.Seed,
		TrafficAllocation:     config.Targeting.TrafficAllocation,
		TrafficAllocationSeed: config.Targeting.TrafficAllocationSeed,
		Conditions:            conditions,
		Sets:                  config.Sets, // Feature flag sets
	}
}

// convertRawCondition transforms a raw condition from /configs format to ConditionDTO
func convertRawCondition(raw RawConditionDTO) ConditionDTO {
	// Determine condition type based on matchers
	// If any matcher is WHITELIST type, the whole condition is WHITELIST, otherwise ROLLOUT
	conditionType := "ROLLOUT"
	for _, matcher := range raw.Matchers {
		if matcher.Type == "WHITELIST" {
			conditionType = "WHITELIST"
			break
		}
	}

	// Convert matchers
	matchers := make([]MatcherDTO, 0, len(raw.Matchers))
	for _, rawMatcher := range raw.Matchers {
		matchers = append(matchers, convertMatcher(rawMatcher))
	}

	// Convert partitions: variant -> treatment
	partitions := make([]PartitionDTO, 0, len(raw.Partitions))
	for _, rawPartition := range raw.Partitions {
		partitions = append(partitions, PartitionDTO{
			Treatment: rawPartition.Variant, // Map "variant" to "treatment"
			Size:      rawPartition.Size,
		})
	}

	return ConditionDTO{
		ConditionType: conditionType,
		Label:         raw.Label,
		MatcherGroup: MatcherGroupDTO{
			Combiner: "AND", // Always AND for /configs conditions
			Matchers: matchers,
		},
		Partitions: partitions,
	}
}

// convertMatcher transforms a raw matcher from /configs format to MatcherDTO
func convertMatcher(raw RawMatcherDTO) MatcherDTO {
	// Build keySelector if attribute is provided
	var keySelector *KeySelectorDTO
	if raw.Attribute != "" {
		attr := raw.Attribute
		keySelector = &KeySelectorDTO{
			TrafficType: "user",
			Attribute:   &attr,
		}
	}

	// Transform based on matcher type
	switch raw.Type {
	case "WHITELIST":
		// Extract strings from data
		var whitelist []string
		if stringsData, ok := raw.Data["strings"].([]interface{}); ok {
			whitelist = make([]string, 0, len(stringsData))
			for _, s := range stringsData {
				if str, ok := s.(string); ok {
					whitelist = append(whitelist, str)
				}
			}
		}
		return MatcherDTO{
			MatcherType: "WHITELIST",
			Negate:      false,
			KeySelector: keySelector,
			Whitelist: &WhitelistMatcherDataDTO{
				Whitelist: whitelist,
			},
		}

	case "IS_EQUAL_TO":
		// Map to EQUAL_TO with unary numeric data
		var dataType string
		var value int64
		if dt, ok := raw.Data["type"].(string); ok {
			dataType = dt
		}
		if num, ok := raw.Data["number"].(float64); ok {
			value = int64(num)
		}
		return MatcherDTO{
			MatcherType: "EQUAL_TO",
			Negate:      false,
			KeySelector: keySelector,
			UnaryNumeric: &UnaryNumericMatcherDataDTO{
				DataType: dataType,
				Value:    value,
			},
		}

	default:
		// Default to ALL_KEYS for unknown types
		return MatcherDTO{
			MatcherType: "ALL_KEYS",
			Negate:      false,
			KeySelector: keySelector,
		}
	}
}
