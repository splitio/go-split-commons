package tasks

import (
	"testing"
	"time"

	hcMock "github.com/splitio/go-split-commons/v9/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v9/storage/mocks"
	syncMocks "github.com/splitio/go-split-commons/v9/synchronizer/mocks"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLargeSegmentSyncTaskHappyPath(t *testing.T) {
	updater := &syncMocks.LargeSegmentUpdaterMock{}
	updater.On("SynchronizeLargeSegment", "ls1", (*int64)(nil)).Return(nil).Once()
	updater.On("SynchronizeLargeSegment", "ls2", (*int64)(nil)).Return(nil).Once()
	updater.On("SynchronizeLargeSegment", "ls3", (*int64)(nil)).Return(nil).Once()

	splitSorage := &mocks.SplitStorageMock{}
	splitSorage.On("LargeSegmentNames").Return(set.NewSet("ls1", "ls2", "ls3")).Once()

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything)
	task := NewFetchLargeSegmentsTask(updater, splitSorage, 2, 10, 10, logging.NewLogger(&logging.LoggerOptions{}), appMonitorMock)

	task.Start()
	time.Sleep(3 * time.Second)
	assert.True(t, task.IsRunning(), "Large Segment fetching task should be running")

	task.Stop(true)
	assert.False(t, task.IsRunning(), "Large Segment fetching task should be stopped")

	splitSorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	updater.AssertExpectations(t)
}
