package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	hcMock "github.com/splitio/go-split-commons/v6/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/api"
	httpMocks "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLocalSyncAllError(t *testing.T) {
	var splitFetchCalled int64
	var notifyEventCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return nil, errors.New("Some")
			},
		},
	}
	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
	}
	segmentMockStorage := mocks.MockSegmentStorage{}
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	syncForTest := NewLocal(&LocalConfig{}, &splitAPI, splitMockStorage, segmentMockStorage, logger, telemetryMockStorage, appMonitorMock)
	err := syncForTest.SyncAll()
	if err == nil {
		t.Error("It should return error")
	}
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Errorf("It should be called once. Actual %d", notifyEventCalled)
	}
}

func TestLocalSyncAllOk(t *testing.T) {
	var splitFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
						Since: 3,
						Till:  3},
				}, nil
			},
		},
	}
	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
	}
	var notifyEventCalled int64
	segmentMockStorage := mocks.MockSegmentStorage{}
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	syncForTest := NewLocal(&LocalConfig{}, &splitAPI, splitMockStorage, segmentMockStorage, logger, telemetryMockStorage, appMonitorMock)
	err := syncForTest.SyncAll()
	if err != nil {
		t.Error("It should not return error")
	}
	if splitFetchCalled != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Errorf("It should be called once. Actual %d", notifyEventCalled)
	}
}

func TestLocalPeriodicFetching(t *testing.T) {
	var splitFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
						Since: 3,
						Till:  3},
				}, nil
			},
		},
	}
	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
	}
	segmentMockStorage := mocks.MockSegmentStorage{}
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	var notifyEventCalled int64
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	syncForTest := NewLocal(&LocalConfig{RefreshEnabled: true, SplitPeriod: 1}, &splitAPI, splitMockStorage, segmentMockStorage, logger, telemetryMockStorage, appMonitorMock)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 1500)
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once", splitFetchCalled)
	}
	syncForTest.StopPeriodicFetching()
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Errorf("It should be called once. Actual %d", notifyEventCalled)
	}
}
