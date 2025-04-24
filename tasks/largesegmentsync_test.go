package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	hcMock "github.com/splitio/go-split-commons/v7/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v7/storage/mocks"
	syncMocks "github.com/splitio/go-split-commons/v7/synchronizer/mocks"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLargeSegmentSyncTaskHappyPath(t *testing.T) {
	var updater syncMocks.LargeSegmentUpdaterMock
	updater.On("SynchronizeLargeSegment", "ls1", (*int64)(nil)).Return(nil).Once()
	updater.On("SynchronizeLargeSegment", "ls2", (*int64)(nil)).Return(nil).Once()
	updater.On("SynchronizeLargeSegment", "ls3", (*int64)(nil)).Return(nil).Once()

	var lsNamesCall int64
	splitSorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			atomic.AddInt64(&lsNamesCall, 1)
			return set.NewSet("ls1", "ls2", "ls3")
		},
	}

	var notifyEventCalled int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	task := NewFetchLargeSegmentsTask(&updater, splitSorage, 1, 10, 10, logging.NewLogger(&logging.LoggerOptions{}), appMonitorMock)

	task.Start()
	time.Sleep(3 * time.Second)
	if !task.IsRunning() {
		t.Error("Large Segment fetching task should be running")
	}

	task.Stop(true)
	if task.IsRunning() {
		t.Error("Large Segment fetching task should be stopped")
	}

	if lsNamesCall != 2 {
		t.Error("Large Segment Call should be 2. Actual: ", lsNamesCall)
	}
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}

	updater.AssertExpectations(t)
}
