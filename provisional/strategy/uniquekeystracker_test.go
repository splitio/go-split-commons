package strategy

import (
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v9/storage/filter"
)

func Test(t *testing.T) {
	bf := filter.NewBloomFilter(10000, 0.01)

	tracker := NewUniqueKeysTracker(bf)

	for i := 0; i < 10; i++ {
		if !tracker.Track("feature-1", "key-"+fmt.Sprint(i)) {
			t.Error("Should be true")
		}
	}

	for i := 0; i < 10; i++ {
		if !tracker.Track("feature-2", "key-"+fmt.Sprint(i)) {
			t.Error("Should be true")
		}
	}

	if tracker.Track("feature-2", "key-4") {
		t.Error("Should be false")
	}

	if tracker.Track("feature-1", "key-4") {
		t.Error("Should be false")
	}
}
