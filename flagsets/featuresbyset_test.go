package flagsets

import (
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
)

func TestFeaturesBySet(t *testing.T) {
	// Test NewFeaturesBySet
	currentSets := NewFeaturesBySet(
		[]dtos.SplitDTO{
			{Name: "split1", Sets: []string{"set1"}},
			{Name: "split2", Sets: []string{"set1", "set2", "set3"}},
			{Name: "split3", Sets: []string{"set1", "set4"}},
		},
	)
	// Test Sets()
	if len(currentSets.Sets()) != 4 {
		t.Error("It should add 4 sets")
	}
	if len(currentSets.FlagsFromSet("set1")) != 3 {
		t.Error("It should have only 3 featureFlags")
	}
	// Test IsFlagInSet
	if !currentSets.IsFlagInSet("set1", "split1") {
		t.Error("split1 should be present in set1")
	}
	if !currentSets.IsFlagInSet("set1", "split2") {
		t.Error("split2 should be present in set1")
	}
	if !currentSets.IsFlagInSet("set1", "split3") {
		t.Error("split3 should be present in set1")
	}

	// Test FlagsFromSet
	if len(currentSets.FlagsFromSet("set2")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.IsFlagInSet("set2", "split2") {
		t.Error("split2 should be present in set2")
	}

	if len(currentSets.FlagsFromSet("set3")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.IsFlagInSet("set3", "split2") {
		t.Error("split2 should be present in set3")
	}

	if len(currentSets.FlagsFromSet("set4")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.IsFlagInSet("set4", "split3") {
		t.Error("split3 should be present in set4")
	}

	// Test RemoveFlagFromSet
	currentSets.RemoveFlagFromSet("set4", "split3")
	if currentSets.IsFlagInSet("set4", "split3") {
		t.Error("split3 should not be present in set4")
	}
}

func TestDifference(t *testing.T) {
	one := NewFeaturesBySet(
		[]dtos.SplitDTO{
			{Name: "split1", Sets: []string{"set1"}},
			{Name: "split2", Sets: []string{"set1", "set2", "set3"}},
			{Name: "split3", Sets: []string{"set1", "set4"}},
		},
	)
	two := NewFeaturesBySet(
		[]dtos.SplitDTO{
			{Name: "split1", Sets: []string{"set1"}},
			{Name: "split2", Sets: []string{"set1"}},
		},
	)

	result := Difference(one, two)
	if !result.IsFlagInSet("set2", "split2") {
		t.Error("split2 should be present in set2")
	}
	if !result.IsFlagInSet("set3", "split2") {
		t.Error("split2 should be present in set3")
	}
	if !result.IsFlagInSet("set1", "split3") {
		t.Error("split3 should be present in set1")
	}
	if !result.IsFlagInSet("set4", "split3") {
		t.Error("split3 should be present in set4")
	}
}
