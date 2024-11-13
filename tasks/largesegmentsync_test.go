package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/storage/mocks"
	workerMock "github.com/splitio/go-split-commons/v6/synchronizer/mocks"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLargeSegmentSyncTaskHappyPath(t *testing.T) {
	var syncLargeSegmentCall int64
	updater := workerMock.MockLargeSegmenUpdater{
		SynchronizeLargeSegmentCall: func(name string, till *int64) error {
			atomic.AddInt64(&syncLargeSegmentCall, 1)
			return nil
		},
	}

	var lsNamesCall int64
	splitSorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			atomic.AddInt64(&lsNamesCall, 1)
			return set.NewSet("ls1", "ls2", "ls3")
		},
	}

	task := NewFetchLargeSegmentsTask(updater, splitSorage, 1, 10, 10, logging.NewLogger(&logging.LoggerOptions{}))

	task.Start()
	time.Sleep(3 * time.Second)
	if !task.IsRunning() {
		t.Error("Large Segment fetching task should be running")
	}

	task.Stop(true)
	if task.IsRunning() {
		t.Error("Large Segment fetching task should be stopped")
	}

	if syncLargeSegmentCall != 6 {
		t.Error("Sync Large Segment Call should be 6. Actual: ", syncLargeSegmentCall)
	}

	if lsNamesCall != 2 {
		t.Error("Large Segment Call should be 2. Actual: ", lsNamesCall)
	}
}
