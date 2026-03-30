package dtos

// ConvertConfigToSplit converts a ConfigDTO to a SplitDTO
func ConvertConfigToSplit(config ConfigDTO) SplitDTO {
	// Apply defaults
	trafficTypeName := config.TrafficTypeName
	if trafficTypeName == "" {
		trafficTypeName = "user"
	}

	status := config.Status
	if status == "" {
		status = "ACTIVE"
	}

	defaultTreatment := config.DefaultTreatment
	if defaultTreatment == "" {
		defaultTreatment = "default"
	}

	// Handle conditions - create default if empty
	conditions := config.Conditions
	if len(conditions) == 0 {
		conditions = []ConditionDTO{
			{
				ConditionType: "ROLLOUT",
				Label:         "default rule",
				MatcherGroup: MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []MatcherDTO{
						{
							MatcherType: "ALL_KEYS",
							Negate:      false,
						},
					},
				},
				Partitions: []PartitionDTO{
					{
						Treatment: defaultTreatment,
						Size:      100,
					},
				},
			},
		}
	}

	return SplitDTO{
		Name:                  config.Name,
		Killed:                config.Killed,
		ChangeNumber:          config.ChangeNumber,
		Configurations:        config.Configurations,
		DefaultTreatment:      defaultTreatment,
		TrafficTypeName:       trafficTypeName,
		Status:                status,
		Algo:                  2,
		Seed:                  config.Seed,
		TrafficAllocation:     config.TrafficAllocation,
		TrafficAllocationSeed: config.TrafficAllocationSeed,
		Conditions:            conditions,
	}
}
