package mutexmap

import (
	"fmt"
	"testing"

	"github.com/splitio/go-toolkit/datastructures/set"
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
		segmentStorage.Put(fmt.Sprintf("segmentito_%d", index), setito, 123)
	}

	for i := 0; i < 3; i++ {
		segmentName := fmt.Sprintf("segmentito_%d", i)
		segment := segmentStorage.Get(segmentName)
		if segment == nil {
			t.Errorf("%s should exist in storage and it doesn't.", segmentName)
		}

		for _, element := range segments[i] {
			if !segment.Has(element) {
				t.Errorf("%s should be part of set number %d and isn't.", element, i)
			}
		}
	}

	segment := segmentStorage.Get("nonexistent_segment")
	if segment != nil {
		t.Error("Nil expected but segment returned")
	}

	segmentStorage.Remove("segmentito_1")
	for index := 0; index < 3; index++ {
		segmentName := fmt.Sprintf("segmentito_%d", index)
		segment := segmentStorage.Get(segmentName)
		if index == 1 && segment != nil {
			t.Error("Segment Should have been removed and is present")
		}
		if index != 1 && segment == nil {
			t.Error("Segment should not have been removed it has")
		}
	}
}
