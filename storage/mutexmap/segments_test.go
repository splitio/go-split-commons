package mutexmap

import (
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
)

func TestMMSegmentStorage(t *testing.T) {
	segments := make([][]string, 3)
	segments[0] = []string{"1a", "1b", "1c"}
	segments[1] = []string{"2a", "2b", "2c"}
	segments[2] = []string{"3a", "3b", "3c"}

	segmentStorage := NewMMSegmentStorage()
	for index, segment := range segments {
		setito := set.NewSet()
		for _, item := range segment {
			setito.Add(item)
		}
		segmentStorage.Update(fmt.Sprintf("segmentito_%d", index), setito, set.NewSet(), 123)
	}

	for i := 0; i < 3; i++ {
		segmentName := fmt.Sprintf("segmentito_%d", i)
		segment := segmentStorage.Keys(segmentName)
		if segment == nil {
			t.Errorf("%s should exist in storage and it doesn't.", segmentName)
		}

		for _, element := range segments[i] {
			if !segment.Has(element) {
				t.Errorf("%s should be part of set number %d and isn't.", element, i)
			}

			contained, _ := segmentStorage.SegmentContainsKey(segmentName, element)
			if !contained {
				t.Errorf("SegmentContainsKey should return true for segment '%s' and key '%s'", segmentName, element)
			}
		}
	}

	segment := segmentStorage.Keys("nonexistent_segment")
	if segment != nil {
		t.Error("Nil expected but segment returned")
	}

	delete(segmentStorage.data, "segmentito_1")
	delete(segmentStorage.till, "segmentito_1")
	for index := 0; index < 3; index++ {
		segmentName := fmt.Sprintf("segmentito_%d", index)
		segment := segmentStorage.Keys(segmentName)
		if index == 1 && segment != nil {
			t.Error("Segment Should have been removed and is present")
		}
		if index != 1 && segment == nil {
			t.Error("Segment should not have been removed it has")
		}
	}
}

func TestMMSplitStorageObjectLivesAfterDeletion(t *testing.T) {
	splitStorage := NewMMSplitStorage()
	splits := make([]dtos.SplitDTO, 10)
	for index := 0; index < 10; index++ {
		splits = append(splits, dtos.SplitDTO{
			Name: fmt.Sprintf("SomeSplit_%d", index),
			Algo: index,
		})
	}

	splitStorage.PutMany(splits, 123)
	someSplit0 := splitStorage.Split("SomeSplit_0")
	splitStorage.Remove("SomeSplit_0")

	if splitStorage.Split("SomeSplit_0") != nil {
		t.Error("Should have been deleted")
	}

	if someSplit0 == nil {
		t.Error("split0 shouldn't be nil")
	}

	if someSplit0.Name != "SomeSplit_0" {
		t.Error("Wrong name")
	}

	if someSplit0.Algo != 0 {
		t.Error("Wrong algo")
	}
}
