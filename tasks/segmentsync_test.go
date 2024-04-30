package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	hcMock "github.com/splitio/go-split-commons/v5/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v5/service"
	fetcherMock "github.com/splitio/go-split-commons/v5/service/mocks"
	"github.com/splitio/go-split-commons/v5/storage/mocks"
	"github.com/splitio/go-split-commons/v5/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v5/telemetry"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSegmentSyncTask(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64
	var notifyEventCalled int64

	splitMockStorage := mocks.MockSplitStorage{
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}

	segmentMockStorage := mocks.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) { return -1, nil },
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			switch segmentName {
			case "segment1", "segment2":
				return nil
			default:
				t.Error("Wrong case")
			}
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			switch name {
			case "segment1":
				if !toAdd.Has("item1") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s1Requested, 1)
			case "segment2":
				if !toAdd.Has("item5") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s2Requested, 1)
			default:
				t.Error("Wrong case")
			}
			return nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.SegmentSync {
				t.Error("Resource should be segments")
			}
		},
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentFetchOptions) (*dtos.SegmentChangesDTO, error) {
			if !fetchOptions.CacheControlHeaders {
				t.Error("no cache shold be true")
			}
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			switch name {
			case "segment1":
				atomic.AddInt64(&s1Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 123, Till: 123}, nil
			case "segment2":
				atomic.AddInt64(&s2Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS2, Removed: []string{}, Since: 123, Till: 123}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	segmentTask := NewFetchSegmentsTask(
		segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock),
		1,
		10,
		100,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	segmentTask.Start()
	time.Sleep(3 * time.Second)
	if !segmentTask.IsRunning() {
		t.Error("Segment fetching task should be running")
	}

	segmentTask.Stop(true)
	if segmentTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if atomic.LoadInt64(&s1Requested) < 2 || atomic.LoadInt64(&s2Requested) < 2 {
		t.Error("It should call fetch twice")
	}
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}
}
