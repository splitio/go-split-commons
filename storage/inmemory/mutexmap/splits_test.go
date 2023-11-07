package mutexmap

import (
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
)

func TestMMSplitStorage(t *testing.T) {
	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))

	cn, _ := splitStorage.ChangeNumber()
	if cn != -1 {
		t.Error("It should be -1")
	}

	result := splitStorage.All()
	if len(result) != 0 {
		t.Error("Unexpected number of splits returned")
	}

	splits := make([]dtos.SplitDTO, 0, 10)
	for index := 0; index < 10; index++ {
		splits = append(splits, dtos.SplitDTO{
			Name: fmt.Sprintf("SomeSplit_%d", index),
			Algo: index,
		})
	}
	splitStorage.Update(splits, nil, 123)

	newCN, _ := splitStorage.ChangeNumber()
	if newCN != 123 {
		t.Error("It should be 123")
	}

	for index := 0; index < 10; index++ {
		splitName := fmt.Sprintf("SomeSplit_%d", index)
		split := splitStorage.Split(splitName)
		if split == nil || split.Name != splitName || split.Algo != index {
			t.Error("Split not returned as expected")
		}
	}

	result = splitStorage.All()
	if len(result) != 10 {
		t.Error("Unexpected number of splits returned")
	}

	splitNames := make([]string, 0)
	for index := 0; index < 10; index++ {
		splitNames = append(splitNames, fmt.Sprintf("SomeSplit_%d", index))
	}
	splitsFetchedMany := splitStorage.FetchMany(splitNames)
	if len(splitsFetchedMany) != 10 {
		t.Error("It should return 10 splits")
	}
	for index := 0; index < 10; index++ {
		if splitsFetchedMany[fmt.Sprintf("SomeSplit_%d", index)] == nil {
			t.Error("It should not be nil")
		}
	}
	splitsFetchedMany = splitStorage.FetchMany([]string{"nonexistent_split"})
	if splitsFetchedMany["nonexistent_split"] != nil {
		t.Error("It should be nil")
	}

	split := splitStorage.Split("nonexistent_split")
	if split != nil {
		t.Error("Nil expected but split returned")
	}

	splitStorage.Remove("SomeSplit_7")
	for index := 0; index < 10; index++ {
		splitName := fmt.Sprintf("SomeSplit_%d", index)
		split := splitStorage.Split(splitName)
		if index == 7 {
			if split != nil {
				t.Error("Split Should have been removed and is present")
			}
		} else {
			if split == nil || split.Name != splitName || split.Algo != index {
				t.Error("Split should not have been removed or modified and it was")
			}
		}
	}
}

func TestSplitKillLocally(t *testing.T) {
	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))

	splitStorage.Update([]dtos.SplitDTO{{
		Name:             "some",
		Algo:             1,
		DefaultTreatment: "defaultTreatment",
		Killed:           false,
		ChangeNumber:     12345676,
	}}, nil, 12345678)

	fetchedSplit := splitStorage._get("some")
	if fetchedSplit.Killed {
		t.Error("It should not be killed")
	}
	if fetchedSplit.ChangeNumber != 12345676 {
		t.Error("It should not be updated")
	}
	if fetchedSplit.DefaultTreatment != "defaultTreatment" {
		t.Error("It should be defaultTreatment")
	}

	splitStorage.KillLocally("some", "anotherDefaultTreatment", 1)
	fetchedKilled := splitStorage._get("some")
	if fetchedKilled.Killed {
		t.Error("It should not be killed")
	}
	if fetchedKilled.ChangeNumber != 12345676 {
		t.Error("It should not be updated")
	}

	splitStorage.KillLocally("some", "anotherDefaultTreatment", 22345678)
	fetchedKilled = splitStorage._get("some")
	if !fetchedKilled.Killed {
		t.Error("It should be killed")
	}
	if fetchedKilled.ChangeNumber != 22345678 {
		t.Error("It should be updated")
	}
	if fetchedKilled.DefaultTreatment != "anotherDefaultTreatment" {
		t.Error("It should be anotherDefaultTreatment")
	}
}

func TestTrafficTypeOnUpdates(t *testing.T) {
	s1 := dtos.SplitDTO{
		Name:            "s1",
		TrafficTypeName: "tt1",
	}

	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{s1}, nil, 123)

	if !splitStorage.TrafficTypeExists("tt1") {
		t.Error("Traffic type 1 should exist.")
	}

	if splitStorage.TrafficTypeExists("tt2") {
		t.Error("Traffic type 2 should not exist.")
	}

	s1.TrafficTypeName = "tt2"
	splitStorage.Update([]dtos.SplitDTO{s1}, nil, 123)
	if splitStorage.TrafficTypeExists("tt1") {
		t.Error("Traffic type 1 should not exist.")
	}

	if !splitStorage.TrafficTypeExists("tt2") {
		t.Error("Traffic type 2 should exist.")
	}
}

func TestTrafficTypes(t *testing.T) {
	ttStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))

	if ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should not exist")
	}

	ttStorage.increaseTrafficTypeCount("mytest")
	if !ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should exist")
	}

	ttStorage.decreaseTrafficTypeCount("mytest")
	if ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should not exist")
	}
}

func TestMMSplitStorageWithFlagSets(t *testing.T) {
	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter([]string{"set1", "set2"}))

	cn, _ := splitStorage.ChangeNumber()
	if cn != -1 {
		t.Error("It should be -1")
	}

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set2"}}
	mockedSplit4 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set2"}}
	splitStorage.Update([]dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3, mockedSplit4}, []dtos.SplitDTO{}, 1)

	if splitStorage.Split("split1") == nil {
		t.Error("split1 should exist")
	}
	if splitStorage.Split("split3") == nil {
		t.Error("split3 should exist")
	}
	if splitStorage.Split("split4") == nil {
		t.Error("split4 should exist")
	}
	sets := splitStorage.flagSets.Sets()
	asMap := make(map[string]struct{})
	for _, set := range sets {
		asMap[set] = struct{}{}
	}
	if _, ok := asMap["set4"]; ok {
		t.Error("set4 should not exist")
	}
	if _, ok := asMap["set5"]; ok {
		t.Error("set5 should not exist")
	}
	asSet := set.NewSet()
	for _, flag := range splitStorage.flagSets.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only one element")
	}
	if !asSet.Has("split1") {
		t.Error("split1 should exist")
	}
	asSet = set.NewSet()
	for _, flag := range splitStorage.flagSets.FlagsFromSet("set2") {
		asSet.Add(flag)
	}
	if asSet.Size() != 2 {
		t.Error("It should have two elements")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should exist")
	}
	if !asSet.Has("split4") {
		t.Error("split4 should exist")
	}

	mockedSplit5 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	mockedSplit6 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	splitStorage.Update([]dtos.SplitDTO{mockedSplit5}, []dtos.SplitDTO{mockedSplit6}, 2)

	if splitStorage.Split("split1") != nil {
		t.Error("split1 should not exist")
	}
	if splitStorage.Split("split3") == nil {
		t.Error("split3 should exist")
	}
	if splitStorage.Split("split4") == nil {
		t.Error("split4 should exist")
	}
	asSet = set.NewSet()
	for _, flag := range splitStorage.flagSets.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only one element")
	}
	if !asSet.Has("split4") {
		t.Error("split4 should exist")
	}
	asSet = set.NewSet()
	for _, flag := range splitStorage.flagSets.FlagsFromSet("set2") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have one element")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should exist")
	}

	mockedSplit7 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{}}
	splitStorage.Update([]dtos.SplitDTO{}, []dtos.SplitDTO{mockedSplit7}, 3)
	asSet = set.NewSet()
	for _, flag := range splitStorage.flagSets.FlagsFromSet("set2") {
		asSet.Add(flag)
	}
	if asSet.Size() != 0 {
		t.Error("It should not have elements")
	}
}

func TestGetNamesByFlagSets(t *testing.T) {
	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter([]string{"set1", "set2"}))

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set2"}}
	mockedSplit4 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set2"}}
	splitStorage.Update([]dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3, mockedSplit4}, []dtos.SplitDTO{}, 1)

	ffBySets := splitStorage.GetNamesByFlagSets([]string{"set1", "set6"})
	if len(ffBySets["set1"]) != 1 {
		t.Error("size of names by set1 should be 1, but was: ", len(ffBySets["set1"]))
	}

	if len(ffBySets["set6"]) != 0 {
		t.Error("size of names by set6 should be 0, but was: ", len(ffBySets["set6"]))
	}

	ffBySets = splitStorage.GetNamesByFlagSets([]string{"set4"})
	if len(ffBySets["set4"]) != 0 {
		t.Error("size of names by set4 should be 0, but was: ", len(ffBySets["set4"]))
	}

	ffBySets = splitStorage.GetNamesByFlagSets([]string{"set2"})
	if len(ffBySets["set2"]) != 2 {
		t.Error("size of names by set2 should be 2, but was: ", len(ffBySets["set2"]))
	}
}
