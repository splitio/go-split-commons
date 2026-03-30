package dtos

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
