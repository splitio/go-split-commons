package dtos

import "encoding/json"

// ConfigDTO represents a configuration definition fetched from the /configs endpoint
type ConfigDTO struct {
	Name                  string            `json:"name"`
	Status                string            `json:"status"`
	Killed                bool              `json:"killed"`
	TrafficTypeName       string            `json:"trafficTypeName"`
	DefaultTreatment      string            `json:"defaultTreatment"`
	ChangeNumber          int64             `json:"changeNumber"`
	TrafficAllocation     int               `json:"trafficAllocation"`
	TrafficAllocationSeed int64             `json:"trafficAllocationSeed"`
	Seed                  int64             `json:"seed"`
	Configurations        map[string]string `json:"configurations"`
	Conditions            []ConditionDTO    `json:"conditions"`
}

// ConfigsDataDTO represents the configs data wrapper in the response
type ConfigsDataDTO struct {
	Since   int64       `json:"s"`
	Till    int64       `json:"t"`
	Configs []ConfigDTO `json:"d"`
}

// ConfigsResponseDTO represents the response from the /configs endpoint
type ConfigsResponseDTO struct {
	Configs ConfigsDataDTO        `json:"configs"`
	RBS     []RuleBasedSegmentDTO `json:"rbs"`
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
	return f.configsResponse.Configs.Since == f.configsResponse.Configs.Till
}

// RuleBasedSegments returns the list of rule-based segments from the response
func (f *FFResponseConfigs) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f.configsResponse.RBS
}

// FeatureFlags returns the list of feature flags (splits) converted from configs
func (f *FFResponseConfigs) FeatureFlags() []SplitDTO {
	splits := make([]SplitDTO, 0, len(f.configsResponse.Configs.Configs))
	for _, config := range f.configsResponse.Configs.Configs {
		splits = append(splits, ConvertConfigToSplit(config))
	}
	return splits
}

// FFTill returns the till value for feature flags
func (f *FFResponseConfigs) FFTill() int64 {
	return f.configsResponse.Configs.Till
}

// RBTill returns the till value for rule-based segments
func (f *FFResponseConfigs) RBTill() int64 {
	return f.configsResponse.Configs.Till
}

// FFSince returns the since value for feature flags
func (f *FFResponseConfigs) FFSince() int64 {
	return f.configsResponse.Configs.Since
}

// RBSince returns the since value for rule-based segments
func (f *FFResponseConfigs) RBSince() int64 {
	return f.configsResponse.Configs.Since
}

var _ FFResponse = (*FFResponseConfigs)(nil)
