package strategy

import (
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v7/storage/filter"
	"github.com/splitio/go-split-commons/v7/storage/inmemory/mutexqueue"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestUniqueKeysTracker(t *testing.T) {
	bf := filter.NewBloomFilter(10000, 0.01)
	uniqueKeysStorage := mutexqueue.NewMQUniqueKeysStorage(100, make(chan string), logging.NewLogger(nil))
	tracker := NewUniqueKeysTracker(bf, uniqueKeysStorage)

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
