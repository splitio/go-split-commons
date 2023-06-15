package util

import "github.com/splitio/go-split-commons/v4/dtos"

const ACTIVE = "ACTIVE"
const ARCHIVE = "ARCHIVE"
const INACTIVE = "INACTIVE"

func GetActiveAndInactiveFF(featureFlags *dtos.SplitChangesDTO) ([]dtos.SplitDTO, []dtos.SplitDTO) {
	inactiveFF := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	activeFF := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	for idx := range featureFlags.Splits {
		if featureFlags.Splits[idx].Status == ACTIVE {
			activeFF = append(activeFF, featureFlags.Splits[idx])
		} else {
			inactiveFF = append(inactiveFF, featureFlags.Splits[idx])
		}
	}
	return activeFF, inactiveFF
}
