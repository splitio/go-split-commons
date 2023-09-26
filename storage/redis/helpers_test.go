package redis

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
)

func TestCalculateSets(t *testing.T) {
	currentSets := flagsets.NewFeaturesBySet(nil)
	currentSets.Add("set1", "split1")
	currentSets.Add("set1", "split2")
	currentSets.Add("set1", "split3")
	currentSets.Add("set2", "split2")
	currentSets.Add("set3", "split2")
	currentSets.Add("set4", "split3")

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

	if len(toAdd.Sets()) != 2 {
		t.Error("It should add two sets")
	}
	if len(toAdd.FlagsFromSet("set1")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	asSet := set.NewSet()
	for _, flag := range toAdd.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if !asSet.Has("split5") {
		t.Error("split5 should be present in set1")
	}
	if len(toAdd.FlagsFromSet("set4")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	asSet = set.NewSet()
	for _, flag := range toAdd.FlagsFromSet("set4") {
		asSet.Add(flag)
	}
	if !asSet.Has("split1") {
		t.Error("split1 should be present in set4")
	}

	// CurrentSets is updated and tracks the featureFlags to be removed
	if len(toRemove.Sets()) != 4 {
		t.Error("It should consider 4 sets to remove")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if asSet.Size() != 2 {
		t.Error("It should have only 2 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set1")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should be present in set1")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set2") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set2")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set3") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set3")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set4") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should be present in set4")
	}
}
