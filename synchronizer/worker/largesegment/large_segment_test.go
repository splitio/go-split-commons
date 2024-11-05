package largesegment

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	hcMock "github.com/splitio/go-split-commons/v6/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/api/specs"
	fetcherMock "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func validReqParams(t *testing.T, fetchOptions service.RequestParams, till string) {
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)
	if req.Header.Get("Cache-Control") != "no-cache" {
		t.Error("Wrong header")
	}
	if req.URL.Query().Get("till") != till {
		t.Error("Wrong till")
	}
}

func getRFE(name string, cn int64) *dtos.RfeDTO {
	return &dtos.RfeDTO{
		Params: dtos.ParamsDTO{
			Method: http.MethodGet,
			URL:    "",
		},
		TotalKeys:    10,
		ChangeNumber: cn,
		ExpiresAt:    time.Now().UnixMilli() + 1000,
		Version:      specs.MEMBERSHIP_V10,
		Name:         name,
	}
}

func TestSynchronizeLargeSegmentHappyPath(t *testing.T) {
	largeSegmentName := "large_segment_test"

	splitMockStorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("large_segment1", "large_segment2") },
	}

	fetcherCount := 0
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		RequestForExportCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error) {
			fetcherCount++
			switch fetcherCount {
			case 1:
				return getRFE(largeSegmentName, 100), nil
			case 2:
				return nil, dtos.HTTPError{
					Code:    http.StatusNotModified,
					Message: "[304] Not Modified",
				}
			default:
				return &dtos.RfeDTO{}, nil
			}
		},
		FetchCall: func(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error) {
			return &dtos.LargeSegmentDTO{
				Name:         largeSegmentName,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	if cn != 100 {
		t.Error("Change Number should be 100. Actual: ", cn)
	}

	if fetcherCount != 2 {
		t.Error("fetcherCount should be 2. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeCDNBypass(t *testing.T) {
	largeSegmentName := "large_segment_test"

	splitMockStorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("large_segment1", "large_segment2") },
	}

	var rfeCount int64
	var fetchCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		RequestForExportCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error) {
			atomic.AddInt64(&rfeCount, 1)
			switch called := atomic.LoadInt64(&rfeCount); {
			case called >= 1 && called <= 11:
				validReqParams(t, fetchOptions, "")
				return getRFE(largeSegmentName, 2), nil
			case called >= 12:
				validReqParams(t, fetchOptions, "2")
				return getRFE(largeSegmentName, 3), nil
			}
			return nil, dtos.HTTPError{Code: http.StatusInternalServerError, Message: "[500] Internal Server Error"}
		},
		FetchCall: func(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error) {
			atomic.AddInt64(&fetchCount, 1)
			return &dtos.LargeSegmentDTO{
				Name:         largeSegmentName,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	if cn != 3 {
		t.Error("Change Number should be 3. Actual: ", cn)
	}

	if atomic.LoadInt64(&rfeCount) != 13 {
		t.Error("fetcherCount should be 13. Actual: ", atomic.LoadInt64(&rfeCount))
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeCDNBypassLimit(t *testing.T) {
	largeSegmentName := "large_segment_test"

	splitMockStorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("large_segment1", "large_segment2") },
	}

	var rfeCount int64
	var fetchCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		RequestForExportCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error) {
			atomic.AddInt64(&rfeCount, 1)
			switch called := atomic.LoadInt64(&rfeCount); {
			case called >= 1 && called <= 11:
				validReqParams(t, fetchOptions, "")
				return getRFE(largeSegmentName, 2), nil
			case called >= 12:
				validReqParams(t, fetchOptions, "2")
				return getRFE(largeSegmentName, 2), nil
			}
			return nil, dtos.HTTPError{Code: http.StatusInternalServerError, Message: "[500] Internal Server Error"}
		},
		FetchCall: func(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error) {
			atomic.AddInt64(&fetchCount, 1)
			return &dtos.LargeSegmentDTO{
				Name:         largeSegmentName,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	if cn != 2 {
		t.Error("Change Number should be 2. Actual: ", cn)
	}

	if atomic.LoadInt64(&rfeCount) != 21 {
		t.Error("fetcherCount should be 13. Actual: ", atomic.LoadInt64(&rfeCount))
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestLargeSegmentNames(t *testing.T) {
	splitMockStorage := mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("large_segment1", "large_segment2") },
	}

	fetcher := fetcherMock.MockLargeSegmentFetcher{}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	lsNames := updater.LargeSegmentNames()
	if len(lsNames) != 2 {
		t.Error("Large Segmet Names should be 2. Actual: ", len(lsNames))
	}
}

func TestLargeSegmentSyncConcurrencyLimit(t *testing.T) {
	splitMockStorage := &mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			for idx := 0; idx < 100; idx++ {
				ss.Add(fmt.Sprintf("s%d", idx))
			}
			return ss
		},
	}

	// this will fail if at any time there are more than `maxConcurrency` fetches running
	emptyVal := struct{}{}
	var done sync.Map
	var inProgress int32
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		RequestForExportCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error) {
			if current := atomic.AddInt32(&inProgress, 1); current > maxConcurrency {
				t.Errorf("throguhput exceeded max expected concurrency of %d. Is: %d", maxConcurrency, current)
			}

			// hold the semaphore for a while
			time.Sleep(100 * time.Millisecond)
			done.Store(name, emptyVal)
			atomic.AddInt32(&inProgress, -1)
			return getRFE(name, 2), nil
		},
		FetchCall: func(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error) {
			return &dtos.LargeSegmentDTO{
				Name:         rfe.Name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	_, err := updater.SynchronizeLargeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	// assert that all segments have been "fetched"
	for idx := 0; idx < 100; idx++ {
		key := fmt.Sprintf("s%d", idx)
		if _, ok := done.Load(key); !ok {
			t.Errorf("segment '%s' not fetched", key)
		}
	}
}
