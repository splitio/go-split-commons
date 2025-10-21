package largesegment

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v8/dtos"
	hcMock "github.com/splitio/go-split-commons/v8/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v8/service"
	"github.com/splitio/go-split-commons/v8/service/api/specs"
	fetcherMock "github.com/splitio/go-split-commons/v8/service/mocks"
	"github.com/splitio/go-split-commons/v8/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v8/storage/mocks"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func validReqParams(t *testing.T, fetchOptions service.RequestParams, validateTill bool) {
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)
	if req.Header.Get("Cache-Control") != "no-cache" {
		t.Error("Wrong header")
	}
	till, _ := strconv.ParseInt(req.URL.Query().Get("till"), 10, 64)
	if validateTill && till <= 0 {
		t.Error("Wrong till")
	}
}

func buildLargeSegmentRFDResponseDTO(name string, cn int64) *dtos.LargeSegmentRFDResponseDTO {
	return &dtos.LargeSegmentRFDResponseDTO{
		NotificationType: LargeSegmentNewDefinition,
		ChangeNumber:     cn,
		SpecVersion:      specs.LARGESEGMENT_V10,
		RFD: &dtos.RFD{
			Data: dtos.Data{
				TotalKeys: 10,
				ExpiresAt: time.Now().UnixMilli() + 1000,
			},
			Params: dtos.Params{
				Method: http.MethodGet,
			},
		},
		Name: name,
	}
}

func TestSynchronizeLargeSegmentHappyPath(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			switch fetcherCount {
			case 1:
				return buildLargeSegmentRFDResponseDTO(name, 100), nil
			default:
				return &dtos.LargeSegmentRFDResponseDTO{}, nil
			}
		},
		DownloadFileCall: func(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: lsRFDResponseDTO.ChangeNumber,
			}, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	if *cn != 100 {
		t.Error("Change Number should be 100. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 2. Actual: ", fetcherCount)
	}

	if eventCall != 1 {
		t.Error("eventCall should be 2. Actual: ", eventCall)
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeURLExpiredCDNBypass(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var rfeCount int64
	var fetchCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&rfeCount, 1)
			switch called := atomic.LoadInt64(&rfeCount); {
			case called >= 1 && called <= 5:
				validReqParams(t, fetchOptions, false)
				dto := buildLargeSegmentRFDResponseDTO(name, 2)
				dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 10000
				return dto, nil
			case called >= 6:
				validReqParams(t, fetchOptions, true)
				return buildLargeSegmentRFDResponseDTO(name, 3), nil
			}
			return nil, dtos.HTTPError{Code: http.StatusInternalServerError, Message: "[500] Internal Server Error"}
		},
		DownloadFileCall: func(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			atomic.AddInt64(&fetchCount, 1)
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: lsRFDResponseDTO.ChangeNumber,
			}, nil
		},
	}

	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	if *cn != 3 {
		t.Error("Change Number should be 3. Actual: ", cn)
	}

	if atomic.LoadInt64(&rfeCount) != 6 {
		t.Error("fetcherCount should be 6. Actual: ", atomic.LoadInt64(&rfeCount))
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 2. Actual: ", atomic.LoadInt64(&eventCall))
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeURLExpiredCDNBypassLimit(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var rfeCount int64
	var fetchCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&rfeCount, 1)
			switch called := atomic.LoadInt64(&rfeCount); {
			case called >= 1 && called <= 5:
				validReqParams(t, fetchOptions, false)
				dto := buildLargeSegmentRFDResponseDTO(name, 2)
				dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 10000
				return dto, nil
			case called >= 6:
				validReqParams(t, fetchOptions, true)
				dto := buildLargeSegmentRFDResponseDTO(name, 2)
				dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 10000
				return dto, nil
			}
			return nil, dtos.HTTPError{Code: http.StatusInternalServerError, Message: "[500] Internal Server Error"}
		},
		DownloadFileCall: func(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			atomic.AddInt64(&fetchCount, 1)
			return &dtos.LargeSegment{
				Name:         largeSegmentName,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: lsRFDResponseDTO.ChangeNumber,
			}, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	if *cn != 2 {
		t.Error("Change Number should be -1. Actual: ", *cn)
	}

	if atomic.LoadInt64(&rfeCount) != 10 {
		t.Error("fetcherCount should be 10. Actual: ", atomic.LoadInt64(&rfeCount))
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}

	if largeSegmentStorage.Count() != 0 {
		t.Error("Large Segment count should be 0. Actual: ", largeSegmentStorage.Count())
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
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			if current := atomic.AddInt32(&inProgress, 1); current > maxConcurrency {
				t.Errorf("throguhput exceeded max expected concurrency of %d. Is: %d", maxConcurrency, current)
			}

			// hold the semaphore for a while
			time.Sleep(100 * time.Millisecond)
			done.Store(name, emptyVal)
			atomic.AddInt32(&inProgress, -1)
			return buildLargeSegmentRFDResponseDTO(name, 2), nil
		},
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

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

	lsCount := largeSegmentStorage.Count()
	if lsCount != 100 {
		t.Error("LS Count should be 100. Actual: ", lsCount)
	}

	if atomic.LoadInt64(&eventCall) != 101 {
		t.Error("eventCall should be 101. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestSynchronizeLargeSegmentFileNotModified(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			return nil, &dtos.HTTPError{Code: http.StatusNotModified, Message: "[304] Not Modified"}
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	if *cn != -1 {
		t.Error("Change Number should be -1. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 1. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 0 {
		t.Error("Large Segment count should be 0. Actual: ", largeSegmentStorage.Count())
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestSynchronizeLargeSegmentLSEmptyNotification(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			dto := buildLargeSegmentRFDResponseDTO(name, 10)
			dto.RFD = nil
			dto.NotificationType = LargeSegmentEmpty

			return dto, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	if *cn != 10 {
		t.Error("Change Number should be 10. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 1. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestSynchronizeLargeSegmentNewDefWithRFDnil(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			dto := buildLargeSegmentRFDResponseDTO(name, 10)
			dto.RFD = nil
			dto.NotificationType = LargeSegmentNewDefinition

			return dto, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err == nil {
		t.Error("Error should be something went wrong reading RequestForDownload data large_segment_test")
	}

	if cn != nil {
		t.Error("Change Number should be nil. Actual: ", *cn)
	}

	if fetcherCount != 5 {
		t.Error("fetcherCount should be 5. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 0 {
		t.Error("Large Segment count should be 0. Actual: ", largeSegmentStorage.Count())
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestIsCached(t *testing.T) {
	largeSegmentName := "large_segment_test"

	fetcher := fetcherMock.MockLargeSegmentFetcher{}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mocks.MockLargeSegmentStorage{
		ChangeNumberCall: func(name string) int64 {
			if name == largeSegmentName {
				return 100
			}
			return -1
		},
	}

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	if !updater.IsCached(largeSegmentName) {
		t.Error("IsCached shoudl return true")
	}
	if updater.IsCached("another_ls_name") {
		t.Error("IsCached shoudl return false")
	}
}

func TestSynchronizeLargeSegmentDownloadFail(t *testing.T) {
	largeSegmentName := "large_segment_test"

	fetcherCount := 0
	downloadCount := 0
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			fetcherCount++
			return buildLargeSegmentRFDResponseDTO(name, 10), nil
		},
		DownloadFileCall: func(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			downloadCount++
			switch {
			case downloadCount <= 5:
				return nil, dtos.HTTPError{Code: http.StatusUnauthorized, Message: "[401] Unauthorized"}
			case downloadCount > 5:
				return &dtos.LargeSegment{
					Name:         name,
					Keys:         []string{"key_1", "key_2", "key_3"},
					ChangeNumber: lsRFDResponseDTO.ChangeNumber,
				}, nil
			}
			return nil, dtos.HTTPError{Code: http.StatusInternalServerError, Message: "[500]"}
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	cn, err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err.Error())
	}

	if *cn != 10 {
		t.Error("Change Number should be 10. Actual: ", cn)
	}

	if fetcherCount != 6 {
		t.Error("fetcherCount should be 6. Actual: ", fetcherCount)
	}

	if downloadCount != 6 {
		t.Error("downloadCount should be 6. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestLargeSegmentsSync(t *testing.T) {
	ls10 := "large_segment_10"
	ls20 := "large_segment_20"
	ls30 := "large_segment_30"

	splitMockStorage := &mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			ss.Add(ls10)
			ss.Add(ls20)
			ss.Add(ls30)

			return ss
		},
	}

	var fetchCall int32
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt32(&fetchCall, 1)
			switch name {
			case ls10:
				return buildLargeSegmentRFDResponseDTO(name, 10), nil
			case ls20:
				return buildLargeSegmentRFDResponseDTO(name, 20), nil
			case ls30:
				return buildLargeSegmentRFDResponseDTO(name, 30), nil
			}

			return &dtos.LargeSegmentRFDResponseDTO{}, nil
		},
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	result, err := updater.SynchronizeLargeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	lsCount := largeSegmentStorage.Count()
	if lsCount != 3 {
		t.Error("LS Count should be 100. Actual: ", lsCount)
	}

	if *result[ls10] != 10 {
		t.Error("ChangeNumber should be 10. Actual: ", *result[ls10])
	}
	if *result[ls20] != 20 {
		t.Error("ChangeNumber should be 20. Actual: ", *result[ls20])
	}
	if *result[ls30] != 30 {
		t.Error("ChangeNumber should be 30. Actual: ", *result[ls30])
	}

	if atomic.LoadInt64(&eventCall) != 4 {
		t.Error("eventCall should be 4. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestSynchronizeLargeSegmentUpdate(t *testing.T) {
	ls10 := "large_segment_10"
	ls20 := "large_segment_20"
	ls30 := "large_segment_30"
	splitMockStorage := &mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			ss.Add(ls10)
			ss.Add(ls20)
			ss.Add(ls30)

			return ss
		},
	}
	var downloadCall int32
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			atomic.AddInt32(&downloadCall, 1)
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()
	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	dto := buildLargeSegmentRFDResponseDTO(ls10, 10)
	cn, err := updater.SynchronizeLargeSegmentUpdate(dto)
	if err != nil {
		t.Error("It should not return err")
	}

	if *cn != 10 {
		t.Error("CN should be 10. Actual: ", *cn)
	}

	lsCount := largeSegmentStorage.Count()
	if lsCount != 1 {
		t.Error("LS Count should be 1. Actual: ", lsCount)
	}

	if atomic.LoadInt32(&downloadCall) != 1 {
		t.Error("DownloadCall should be 1. Actual: ", atomic.LoadInt32(&downloadCall))
	}
	if atomic.LoadInt64(&eventCall) != 0 {
		t.Error("eventCall should be 0. Actual: ", atomic.LoadInt64(&eventCall))
	}
}

func TestSynchronizeLargeSegmentUpdateListEmpty(t *testing.T) {
	ls10 := "large_segment_10"
	ls20 := "large_segment_20"
	ls30 := "large_segment_30"
	splitMockStorage := &mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			ss.Add(ls10)
			ss.Add(ls20)
			ss.Add(ls30)

			return ss
		},
	}
	var downloadCall int32
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			atomic.AddInt32(&downloadCall, 1)
			return nil, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()
	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	dto := &dtos.LargeSegmentRFDResponseDTO{
		NotificationType: "LS_EMPTY",
		Name:             ls10,
		SpecVersion:      "1.0",
		ChangeNumber:     10,
	}
	cn, err := updater.SynchronizeLargeSegmentUpdate(dto)
	if err != nil {
		t.Error("It should not return err")
	}

	if *cn != 10 {
		t.Error("CN should be 10. Actual: ", *cn)
	}

	lsCount := largeSegmentStorage.Count()
	if lsCount != 1 {
		t.Error("LS Count should be 1. Actual: ", lsCount)
	}

	if atomic.LoadInt32(&downloadCall) != 0 {
		t.Error("DownloadCall should be 0. Actual: ", atomic.LoadInt32(&downloadCall))
	}
}

func TestSynchronizeLargeSegmentUpdateWithUrlExpired(t *testing.T) {
	ls10 := "large_segment_10"
	splitMockStorage := &mocks.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			ss.Add(ls10)

			return ss
		},
	}

	var fetchCall int32
	var downloadCall int32
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt32(&fetchCall, 1)
			switch name {
			case ls10:
				return buildLargeSegmentRFDResponseDTO(ls10, 10), nil
			}

			return &dtos.LargeSegmentRFDResponseDTO{}, nil
		},
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			atomic.AddInt32(&downloadCall, 1)
			return &dtos.LargeSegment{
				Name:         name,
				Keys:         []string{"key_1", "key_2", "key_3"},
				ChangeNumber: rfe.ChangeNumber,
			}, nil
		},
	}
	var eventCall int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&eventCall, 1)
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()
	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	dto := buildLargeSegmentRFDResponseDTO(ls10, 10)
	dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 1000
	cn, err := updater.SynchronizeLargeSegmentUpdate(dto)
	if err != nil {
		t.Error("It should not return err")
	}

	if *cn != 10 {
		t.Error("CN should be 10. Actual: ", *cn)
	}

	lsCount := largeSegmentStorage.Count()
	if lsCount != 1 {
		t.Error("LS Count should be 1. Actual: ", lsCount)
	}

	if atomic.LoadInt32(&downloadCall) != 1 {
		t.Error("DownloadCall should be 0. Actual: ", atomic.LoadInt32(&downloadCall))
	}
	if atomic.LoadInt32(&fetchCall) != 1 {
		t.Error("fetchCall should be 1. Actual: ", atomic.LoadInt32(&fetchCall))
	}

	if atomic.LoadInt64(&eventCall) != 1 {
		t.Error("eventCall should be 1. Actual: ", atomic.LoadInt64(&eventCall))
	}
}
