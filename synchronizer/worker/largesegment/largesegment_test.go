package largesegment

import (
	"fmt"
	"net/http"
	"strconv"
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

func buildLargeSegmentRFDResponseDTO(cn int64) *dtos.LargeSegmentRFDResponseDTO {
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
				return buildLargeSegmentRFDResponseDTO(100), nil
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
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != 100 {
		t.Error("Change Number should be 100. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 2. Actual: ", fetcherCount)
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
				dto := buildLargeSegmentRFDResponseDTO(2)
				dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 10000
				return dto, nil
			case called >= 6:
				validReqParams(t, fetchOptions, true)
				return buildLargeSegmentRFDResponseDTO(3), nil
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
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != 3 {
		t.Error("Change Number should be 3. Actual: ", cn)
	}

	if atomic.LoadInt64(&rfeCount) != 6 {
		t.Error("fetcherCount should be 6. Actual: ", atomic.LoadInt64(&rfeCount))
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
				dto := buildLargeSegmentRFDResponseDTO(2)
				dto.RFD.Data.ExpiresAt = time.Now().UnixMilli() - 10000
				return dto, nil
			case called >= 6:
				validReqParams(t, fetchOptions, true)
				dto := buildLargeSegmentRFDResponseDTO(2)
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
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	err := updater.SynchronizeLargeSegment(largeSegmentName, &till)
	if err != nil {
		t.Error(err)
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != -1 {
		t.Error("Change Number should be -1. Actual: ", cn)
	}

	if atomic.LoadInt64(&rfeCount) != 10 {
		t.Error("fetcherCount should be 10. Actual: ", atomic.LoadInt64(&rfeCount))
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
			return buildLargeSegmentRFDResponseDTO(2), nil
		},
		DownloadFileCall: func(name string, rfe *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
			return &dtos.LargeSegment{
				Name:         name,
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

	err := updater.SynchronizeLargeSegments()
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
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != -1 {
		t.Error("Change Number should be -1. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 1. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 0 {
		t.Error("Large Segment count should be 0. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeSegmentLSEmptyNotification(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			dto := buildLargeSegmentRFDResponseDTO(10)
			dto.RFD = nil
			dto.NotificationType = LargeSegmentEmpty

			return dto, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err)
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != 10 {
		t.Error("Change Number should be 10. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 1. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 1 {
		t.Error("Large Segment count should be 1. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeSegmentNewDefWithRFDnil(t *testing.T) {
	largeSegmentName := "large_segment_test"

	var fetcherCount int64
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			atomic.AddInt64(&fetcherCount, 1)
			dto := buildLargeSegmentRFDResponseDTO(10)
			dto.RFD = nil
			dto.NotificationType = LargeSegmentNewDefinition

			return dto, nil
		},
	}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err == nil {
		t.Error("Error should be something went wrong reading RequestForDownload data large_segment_test")
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != -1 {
		t.Error("Change Number should be -1. Actual: ", cn)
	}

	if fetcherCount != 1 {
		t.Error("fetcherCount should be 1. Actual: ", fetcherCount)
	}

	if largeSegmentStorage.Count() != 0 {
		t.Error("Large Segment count should be 0. Actual: ", largeSegmentStorage.Count())
	}
}

func TestSynchronizeLargeSegmentDownloadFail(t *testing.T) {
	largeSegmentName := "large_segment_test"

	fetcherCount := 0
	downloadCount := 0
	fetcher := fetcherMock.MockLargeSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
			fetcherCount++
			return buildLargeSegmentRFDResponseDTO(10), nil
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
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}
	splitMockStorage := mocks.MockSplitStorage{}
	largeSegmentStorage := mutexmap.NewLargeSegmentsStorage()

	updater := NewLargeSegmentUpdater(splitMockStorage, largeSegmentStorage, fetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)
	updater.onDemandFetchBackoffBase = 1
	updater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	err := updater.SynchronizeLargeSegment(largeSegmentName, nil)
	if err != nil {
		t.Error(err.Error())
	}

	cn := largeSegmentStorage.ChangeNumber(largeSegmentName)
	if cn != 10 {
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
}
