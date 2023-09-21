package redis

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
)

func TestFeaturesBySet(t *testing.T) {
	currentSets := newFeaturesBySet(
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1"}),
			createSampleSplit("split2", []string{"set1", "set2", "set3"}),
			createSampleSplit("split3", []string{"set1", "set4"}),
		},
	)
	if len(currentSets.data) != 4 {
		t.Error("It should add 4 sets")
	}
	if len(currentSets.data["set1"]) != 3 {
		t.Error("It should have only 3 featureFlags")
	}
	if !currentSets.isFlagInSet("set1", "split1") {
		t.Error("split1 should be present in set1")
	}
	if !currentSets.isFlagInSet("set1", "split2") {
		t.Error("split2 should be present in set1")
	}
	if !currentSets.isFlagInSet("set1", "split3") {
		t.Error("split3 should be present in set1")
	}

	if len(currentSets.data["set2"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.isFlagInSet("set2", "split2") {
		t.Error("split2 should be present in set2")
	}

	if len(currentSets.data["set3"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.isFlagInSet("set3", "split2") {
		t.Error("split2 should be present in set3")
	}

	if len(currentSets.data["set4"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !currentSets.isFlagInSet("set4", "split3") {
		t.Error("split3 should be present in set4")
	}

	currentSets.removeFlagFromSet("set4", "split3")
	if currentSets.isFlagInSet("set4", "split3") {
		t.Error("split3 should not be present in set4")
	}
}

func TestDifference(t *testing.T) {
	one := newFeaturesBySet(
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1"}),
			createSampleSplit("split2", []string{"set1", "set2", "set3"}),
			createSampleSplit("split3", []string{"set1", "set4"}),
		},
	)
	two := newFeaturesBySet(
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1"}),
			createSampleSplit("split2", []string{"set1"}),
		},
	)

	result := difference(one, two)
	if !result.isFlagInSet("set2", "split2") {
		t.Error("split2 should be present in set2")
	}
	if !result.isFlagInSet("set3", "split2") {
		t.Error("split2 should be present in set3")
	}
	if !result.isFlagInSet("set1", "split3") {
		t.Error("split3 should be present in set1")
	}
	if !result.isFlagInSet("set4", "split3") {
		t.Error("split3 should be present in set4")
	}
}

func TestCalculateSets(t *testing.T) {
	currentSets := newFeaturesBySet(nil)
	currentSets.add("set1", "split1")
	currentSets.add("set1", "split2")
	currentSets.add("set1", "split3")
	currentSets.add("set2", "split2")
	currentSets.add("set3", "split2")
	currentSets.add("set4", "split3")

	toAdd, toRemove := calculateSets(
		currentSets,
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1", "set4"}),
			createSampleSplit("split5", []string{"set1"}),
		},
		[]dtos.SplitDTO{
			createSampleSplit("split2", []string{"set2"}),
			createSampleSplit("split3", []string{"set1", "set4"}),
		},
	)

	if len(toAdd.data) != 2 {
		t.Error("It should add two sets")
	}
	if len(toAdd.data["set1"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toAdd.data["set1"]["split5"]; !ok {
		t.Error("split5 should be present in set1")
	}
	if len(toAdd.data["set4"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toAdd.data["set4"]["split1"]; !ok {
		t.Error("split1 should be present in set4")
	}

	// CurrentSets is updated and tracks the featureFlags to be removed
	if len(toRemove.data) != 4 {
		t.Error("It should consider 4 sets to remove")
	}
	if len(toRemove.data["set1"]) != 2 {
		t.Error("It should have only 2 featureFlags")
	}
	if _, ok := toRemove.data["set1"]["split2"]; !ok {
		t.Error("split2 should be present in set1")
	}
	if _, ok := toRemove.data["set1"]["split3"]; !ok {
		t.Error("split3 should be present in set1")
	}
	if len(toRemove.data["set2"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toRemove.data["set2"]["split2"]; !ok {
		t.Error("split2 should be present in set2")
	}
	if len(toRemove.data["set3"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toRemove.data["set3"]["split2"]; !ok {
		t.Error("split2 should be present in set3")
	}
	if len(toRemove.data["set4"]) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toRemove.data["set4"]["split3"]; !ok {
		t.Error("split3 should be present in set4")
	}
}
