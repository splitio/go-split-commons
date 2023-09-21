package redis

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
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
	if _, ok := toAdd.FlagsFromSet("set1")["split5"]; !ok {
		t.Error("split5 should be present in set1")
	}
	if len(toAdd.FlagsFromSet("set4")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := toAdd.FlagsFromSet("set4")["split1"]; !ok {
		t.Error("split1 should be present in set4")
	}

	// CurrentSets is updated and tracks the featureFlags to be removed
	if len(toRemove.Sets()) != 4 {
		t.Error("It should consider 4 sets to remove")
	}
	set1 := toRemove.FlagsFromSet("set1")
	if len(set1) != 2 {
		t.Error("It should have only 2 featureFlags")
	}
	if _, ok := set1["split2"]; !ok {
		t.Error("split2 should be present in set1")
	}
	if _, ok := set1["split3"]; !ok {
		t.Error("split3 should be present in set1")
	}
	set2 := toRemove.FlagsFromSet("set2")
	if len(set2) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := set2["split2"]; !ok {
		t.Error("split2 should be present in set2")
	}
	set3 := toRemove.FlagsFromSet("set3")
	if len(set3) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := set3["split2"]; !ok {
		t.Error("split2 should be present in set3")
	}
	set4 := toRemove.FlagsFromSet("set4")
	if len(set4) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if _, ok := set4["split3"]; !ok {
		t.Error("split3 should be present in set4")
	}
}
