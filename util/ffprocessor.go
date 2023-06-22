package util

import "github.com/splitio/go-split-commons/v4/dtos"

const ACTIVE = "ACTIVE"
const ARCHIVED = "ARCHIVED"

func ProcessFeatureFlagChanges(featureFlags *dtos.SplitChangesDTO) ([]dtos.SplitDTO, []dtos.SplitDTO) {
	toRemove := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	toAdd := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	for idx := range featureFlags.Splits {
		if featureFlags.Splits[idx].Status == ACTIVE {
			toAdd = append(toAdd, featureFlags.Splits[idx])
		} else {
			toRemove = append(toRemove, featureFlags.Splits[idx])
		}
	}
	return toAdd, toRemove
}
