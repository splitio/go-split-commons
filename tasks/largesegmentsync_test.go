package tasks

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	workerMock "github.com/splitio/go-split-commons/v6/synchronizer/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLargeSegmentSyncTaskHappyPath(t *testing.T) {
	var syncLargeSegmentCall int64
	var lsNamesCall int64
	updater := workerMock.MockLargeSegmenUpdater{
		SynchronizeLargeSegmentCall: func(name string, till *int64) (int64, error) {
			fmt.Println(name)
			atomic.AddInt64(&syncLargeSegmentCall, 1)
			return 0, nil
		},
		LargeSegmentNamesCall: func() []interface{} {
			atomic.AddInt64(&lsNamesCall, 1)
			tr := make([]interface{}, 0)
			tr = append(tr, "ls1")
			tr = append(tr, "ls2")
			tr = append(tr, "ls3")
			return tr
		},
	}

	task := NewFetchLargeSegmentsTask(updater, 1, 10, 10, logging.NewLogger(&logging.LoggerOptions{}))

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
