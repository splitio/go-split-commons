package dtos

import "encoding/json"

// VariantDTO represents a variant (treatment) with its definition (directive for evaluator)
type VariantDTO struct {
	Name       string      `json:"name"`
	Definition interface{} `json:"definition"` // Acts as directive for the evaluator
}

// RawMatcherDTO represents a matcher as returned by the /configs endpoint (needs transformation)
type RawMatcherDTO struct {
	Type      string                 `json:"type"`      // e.g., "WHITELIST", "IS_EQUAL_TO", "GREATER_THAN_OR_EQUAL_TO"
	Attribute string                 `json:"attribute"` // Attribute name (optional for ALL_KEYS)
	Data      map[string]interface{} `json:"data"`      // Flexible data structure
}

// RawPartitionDTO represents a partition as returned by the /configs endpoint
type RawPartitionDTO struct {
	Variant string `json:"variant"` // Uses "variant" instead of "treatment"
	Size    int    `json:"size"`
}

// RawConditionDTO represents a condition as returned by the /configs endpoint (needs transformation)
type RawConditionDTO struct {
	Label      string            `json:"label"`
	Partitions []RawPartitionDTO `json:"partitions"`
	Matchers   []RawMatcherDTO   `json:"matchers"` // Flat array, not MatcherGroup
	// Note: No ConditionType field in raw conditions
}

// TargetingDTO represents the targeting rules for a config
type TargetingDTO struct {
	Default               string            `json:"default"`               // The default variant name
	Seed                  int64             `json:"seed"`                  // Seed for hashing
	TrafficAllocation     int               `json:"trafficAllocation"`     // Percentage of traffic allocated
	TrafficAllocationSeed int64             `json:"trafficAllocationSeed"` // Seed for traffic allocation
	Conditions            []RawConditionDTO `json:"conditions"`            // Raw targeting conditions (need transformation)
}

// ConfigDTO represents a configuration definition fetched from the /configs endpoint
type ConfigDTO struct {
	Name            string        `json:"name"`
	TrafficTypeName string        `json:"trafficTypeName"`
	ChangeNumber    int64         `json:"changeNumber"`
	Status          string        `json:"status"`  // Defaults to "ACTIVE" if missing
	Killed          bool          `json:"killed"`  // Defaults to false if missing
	Sets            []string      `json:"sets"`    // Feature flag sets
	Variants        []VariantDTO  `json:"variants"`
	Targeting       TargetingDTO  `json:"targeting"`
}

// ConfigsResponseDTO represents the response from the /configs endpoint
type ConfigsResponseDTO struct {
	Updated []ConfigDTO           `json:"updated"` // List of updated configs
	Since   int64                 `json:"since"`   // Starting change number
	Till    int64                 `json:"till"`    // Ending change number
	RBS     []RuleBasedSegmentDTO `json:"rbs"`     // Rule-based segments
}

// FFResponseConfigs implements FFResponse interface for configs endpoint responses
type FFResponseConfigs struct {
	configsResponse ConfigsResponseDTO
}

// NewFFResponseConfigs creates a new FFResponseConfigs instance from JSON data
func NewFFResponseConfigs(data []byte) (FFResponse, error) {
	var configsResponse ConfigsResponseDTO
	err := json.Unmarshal(data, &configsResponse)
	if err != nil {
		return nil, err
	}
	return &FFResponseConfigs{
		configsResponse: configsResponse,
	}, nil
}

// NeedsAnotherFetch checks if another fetch is needed based on the since and till values
func (f *FFResponseConfigs) NeedsAnotherFetch() bool {
	return f.configsResponse.Since == f.configsResponse.Till
}

// RuleBasedSegments returns the list of rule-based segments from the response
func (f *FFResponseConfigs) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f.configsResponse.RBS
}

// FeatureFlags returns the list of feature flags (splits) converted from configs
func (f *FFResponseConfigs) FeatureFlags() []SplitDTO {
	splits := make([]SplitDTO, 0, len(f.configsResponse.Updated))
	for _, config := range f.configsResponse.Updated {
		splits = append(splits, ConvertConfigToSplit(config))
	}
	return splits
}

// FFTill returns the till value for feature flags
func (f *FFResponseConfigs) FFTill() int64 {
	return f.configsResponse.Till
}

// RBTill returns the till value for rule-based segments
func (f *FFResponseConfigs) RBTill() int64 {
	return f.configsResponse.Till
}

// FFSince returns the since value for feature flags
func (f *FFResponseConfigs) FFSince() int64 {
	return f.configsResponse.Since
}

// RBSince returns the since value for rule-based segments
func (f *FFResponseConfigs) RBSince() int64 {
	return f.configsResponse.Since
}

var _ FFResponse = (*FFResponseConfigs)(nil)
