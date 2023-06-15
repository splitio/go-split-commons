package util

import (
	"testing"

	"github.com/splitio/go-split-commons/v4/dtos"
)

func TestGetActiveFF(t *testing.T) {

	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: ACTIVE})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: ACTIVE})
	featureFlagChanges := &dtos.SplitChangesDTO{Splits: featureFlags}

	actives, inactives := GetActiveAndInactiveFF(featureFlagChanges)

	if len(actives) != 2 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 0 {
		t.Error("incative length should be 0")
	}
}

func TestGetInactiveFF(t *testing.T) {

	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: INACTIVE})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: ARCHIVE})
	featureFlagChanges := &dtos.SplitChangesDTO{Splits: featureFlags}

	actives, inactives := GetActiveAndInactiveFF(featureFlagChanges)

	if len(actives) != 0 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 2 {
		t.Error("incative length should be 0")
	}
}

func TestGetActiveAndInactiveFF(t *testing.T) {

	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: ACTIVE})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: ARCHIVE})
	featureFlagChanges := &dtos.SplitChangesDTO{Splits: featureFlags}

	actives, inactives := GetActiveAndInactiveFF(featureFlagChanges)

	if len(actives) != 1 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 1 {
		t.Error("incative length should be 0")
	}
}
