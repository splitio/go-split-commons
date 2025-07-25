package split

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/condition"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
	"github.com/splitio/go-split-commons/v6/flagsets"
	hcMock "github.com/splitio/go-split-commons/v6/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v6/service"
	fetcherMock "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/inmemory"
	"github.com/splitio/go-split-commons/v6/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/common"
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

func TestSplitSynchronizerError(t *testing.T) {
	var notifyEventCalled int64

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			if fetchOptions.ChangeNumber() != -1 {
				t.Error("Wrong changenumber passed")
			}
			return nil, &dtos.HTTPError{Code: 500, Message: "some"}
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.SplitSync {
				t.Error("It should be splits")
			}
			if status != 500 {
				t.Error("Status should be 500")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitUpdater := NewSplitUpdater(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	_, err := splitUpdater.SynchronizeSplits(nil)
	if err == nil {
		t.Error("It should return err")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSplitSynchronizerErrorScRequestURITooLong(t *testing.T) {
	var notifyEventCalled int64
	var fetchCall int64

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCall, 1)
			if fetchOptions.ChangeNumber() != -1 {
				t.Error("Wrong changenumber passed")
			}
			return nil, &dtos.HTTPError{Code: 414, Message: "some"}
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.SplitSync {
				t.Error("It should be splits")
			}
			if status != 414 {
				t.Error("Status should be 414")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitUpdater := NewSplitUpdater(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	_, err := splitUpdater.SynchronizeSplits(nil)
	if err == nil {
		t.Error("It should return err")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&fetchCall) != 1 {
		t.Error("fetchCall should be called once")
	}
}

func TestSplitSynchronizer(t *testing.T) {
	before := time.Now().UTC()
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}
	var notifyEventCalled int64

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
			s1 := toAdd[0]
			if s1.Name != "split1" || s1.Killed {
				t.Error("split1 stored/retrieved incorrectly")
				t.Error(s1)
			}
			s2 := toAdd[1]
			if s2.Name != "split2" || !s2.Killed {
				t.Error("split2 stored/retrieved incorrectly")
				t.Error(s2)
			}
		},
		RemoveCall: func(splitname string) {
			if splitname != "split3" {
				t.Error("It should remove split3")
			}
		},
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			if fetchOptions.ChangeNumber() != -1 {
				t.Error("Wrong since")
			}
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
					Since: 3,
					Till:  3},
			}, nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
			if tm.Before(before) {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitUpdater := NewSplitUpdater(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	_, err := splitUpdater.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSplitSyncProcess(t *testing.T) {
	var call int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}
	mockedSplit4 := dtos.SplitDTO{Name: "split1", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}
	mockedSplit5 := dtos.SplitDTO{
		Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "two",
		Conditions: []dtos.ConditionDTO{{MatcherGroup: dtos.MatcherGroupDTO{Matchers: []dtos.MatcherDTO{
			{MatcherType: "IN_SEGMENT", UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{SegmentName: "someSegment"}},
		}}}},
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch call {
			case 1:
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
						Since: 3,
						Till:  3},
				}, nil
			case 2:
				if fetchOptions.ChangeNumber() != 3 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit4, mockedSplit5},
						Since: 3,
						Till:  3},
				}, nil
			default:
				t.Error("Wrong calls")
				return nil, errors.New("some")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	res, err := splitUpdater.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	if len(res.ReferencedSegments) != 0 {
		t.Error("invalid referenced segment names. Got: ", res.ReferencedSegments)
	}

	if !splitStorage.TrafficTypeExists("one") {
		t.Error("It should exists")
	}

	if !splitStorage.TrafficTypeExists("two") {
		t.Error("It should exists")
	}

	res, err = splitUpdater.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	if len(res.ReferencedSegments) != 1 || res.ReferencedSegments[0] != "someSegment" {
		t.Error("invalid referenced segment names. Got: ", res.ReferencedSegments)
	}

	s1 := splitStorage.Split("split1")
	if s1 != nil {
		t.Error("split1 should have been removed")
	}

	s2 := splitStorage.Split("split2")
	if s2 == nil || s2.Name != "split2" || !s2.Killed {
		t.Error("split2 stored/retrieved incorrectly")
		t.Error(s2)
	}

	s3 := splitStorage.Split("split3")
	if s3 != nil {
		t.Error("split3 should have been removed")
	}

	s4 := splitStorage.Split("split4")
	if s4 == nil || s4.Name != "split4" || s4.Killed {
		t.Error("split4 stored/retrieved incorrectly")
		t.Error(s4)
	}

	if splitStorage.TrafficTypeExists("one") {
		t.Error("It should not exists")
	}

	if !splitStorage.TrafficTypeExists("two") {
		t.Error("It should exists")
	}

	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSplitTill(t *testing.T) {
	var call int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
					Since: 2,
					Till:  2},
			}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	var till int64 = 1
	_, err := splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	_, err = splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestByPassingCDN(t *testing.T) {
	var call int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch called := atomic.LoadInt64(&call); {
			case called == 1:
				validReqParams(t, fetchOptions, "")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 1,
						Till:  2},
				}, nil
			case called >= 2 && called <= 11:
				validReqParams(t, fetchOptions, "")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 2,
						Till:  2},
				}, nil
			case called == 12:
				validReqParams(t, fetchOptions, "2")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 3,
						Till:  3},
				}, nil
			}

			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
					Since: 2,
					Till:  2},
			}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))
	splitUpdater.onDemandFetchBackoffBase = 1
	splitUpdater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	_, err := splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 12 {
		t.Error("It should be called twelve times instead of", atomic.LoadInt64(&call))
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called twice instead of", atomic.LoadInt64(&notifyEventCalled))
	}
}

func TestByPassingCDNLimit(t *testing.T) {
	var call int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch called := atomic.LoadInt64(&call); {
			case called == 1:
				validReqParams(t, fetchOptions, "")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 1,
						Till:  2},
				}, nil
			case called >= 2 && called <= 11:
				validReqParams(t, fetchOptions, "")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 2,
						Till:  2},
				}, nil
			case called >= 12:
				validReqParams(t, fetchOptions, "2")
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
						Since: 2,
						Till:  2},
				}, nil
			}

			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
					Since: 2,
					Till:  2},
			}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))
	splitUpdater.onDemandFetchBackoffBase = 1
	splitUpdater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	_, err := splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 21 {
		t.Error("It should be called twenty one times instead of", atomic.LoadInt64(&call))
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called twice instead of", atomic.LoadInt64(&notifyEventCalled))
	}
}

func TestProcessFFChange(t *testing.T) {
	var fetchCallCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 43, nil
		},
	}
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCallCalled, 1)
			return nil, nil
		},
	}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}

	fetcher := NewSplitUpdater(ffStorageMock, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	result, _ := fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 12), nil, nil,
	))
	if result.RequiresFetch {
		t.Error("should be false")
	}
	if atomic.LoadInt64(&fetchCallCalled) != 0 {
		t.Error("Fetch should not be called")
	}
}

func TestAddOrUpdateFeatureFlagNil(t *testing.T) {
	var fetchCallCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {},
	}
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCallCalled, 1)
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Till: 2,
					Since:  2,
					Splits: []dtos.SplitDTO{}},
			}, nil
		},
	}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	fetcher := NewSplitUpdater(ffStorageMock, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2), nil, nil,
	))
	if atomic.LoadInt64(&fetchCallCalled) != 1 {
		t.Error("Fetch should be called once")
	}
}

func TestAddOrUpdateFeatureFlagPcnEquals(t *testing.T) {
	var fetchCallCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&updateCalled, 1)
			if changeNumber != 4 {
				t.Error("changenumber should be the one incomed from dto")
			}
			if len(toAdd) == 0 {
				t.Error("toAdd should have a feature flag")
			}
			if len(toRemove) != 0 {
				t.Error("toRemove shouldn't have a feature flag")
			}
		},
	}
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCallCalled, 1)
			return nil, nil
		},
	}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}
	fetcher := NewSplitUpdater(ffStorageMock, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: Active}

	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4), common.Int64Ref(2), &featureFlag,
	))
	if atomic.LoadInt64(&fetchCallCalled) != 0 {
		t.Error("It should not fetch splits")
	}
	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("It should update the storage")
	}
}

func TestAddOrUpdateFeatureFlagArchive(t *testing.T) {
	var fetchCallCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&updateCalled, 1)
			if changeNumber != 4 {
				t.Error("changenumber should be the one incomed from dto")
			}
			if len(toRemove) == 0 {
				t.Error("toRemove should have a feature flag")
			}
			if len(toAdd) != 0 {
				t.Error("toAdd shouldn't have a feature flag")
			}
		},
	}
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCallCalled, 1)
			return nil, nil
		},
	}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}
	fetcher := NewSplitUpdater(ffStorageMock, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: Archived}
	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4), common.Int64Ref(2), &featureFlag,
	))
	if atomic.LoadInt64(&fetchCallCalled) != 0 {
		t.Error("It should not fetch splits")
	}
	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("It should update the storage")
	}
}

func TestAddOrUpdateFFCNFromStorageError(t *testing.T) {
	var fetchCallCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, errors.New("error geting change number")
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&updateCalled, 1)
			if changeNumber != 2 {
				t.Error("It should be 2")
			}
		},
	}
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&fetchCallCalled, 1)
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Till: 2,
					Since:  2,
					Splits: []dtos.SplitDTO{}},
			}, nil
		},
	}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}
	fetcher := NewSplitUpdater(ffStorageMock, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil))

	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2), nil, nil,
	))
	if atomic.LoadInt64(&fetchCallCalled) != 1 {
		t.Error("It should fetch splits")
	}
	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("It should update the storage")
	}
}

func TestGetActiveFF(t *testing.T) {
	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Active})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Active})
	featureFlagChanges := &dtos.SplitChangesDTO{FeatureFlags: dtos.FeatureFlagsDTO{Splits: featureFlags}}

	s := NewSplitUpdater(mocks.MockSplitStorage{}, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil))
	actives, inactives := s.processFeatureFlagChanges(featureFlagChanges)

	if len(actives) != 2 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 0 {
		t.Error("incative length should be 0")
	}
}

func TestGetInactiveFF(t *testing.T) {
	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Archived})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Archived})
	featureFlagChanges := &dtos.SplitChangesDTO{FeatureFlags: dtos.FeatureFlagsDTO{Splits: featureFlags}}

	s := NewSplitUpdater(mocks.MockSplitStorage{}, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil))
	actives, inactives := s.processFeatureFlagChanges(featureFlagChanges)

	if len(actives) != 0 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 2 {
		t.Error("incative length should be 0")
	}
}

func TestGetActiveAndInactiveFF(t *testing.T) {
	var featureFlags []dtos.SplitDTO
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Active})
	featureFlags = append(featureFlags, dtos.SplitDTO{Status: Archived})
	featureFlagChanges := &dtos.SplitChangesDTO{
		FeatureFlags: dtos.FeatureFlagsDTO{Splits: featureFlags}}

	s := NewSplitUpdater(mocks.MockSplitStorage{}, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil))
	actives, inactives := s.processFeatureFlagChanges(featureFlagChanges)

	if len(actives) != 1 {
		t.Error("active length should be 2")
	}

	if len(inactives) != 1 {
		t.Error("incative length should be 0")
	}
}

func TestSplitSyncWithSets(t *testing.T) {
	var call int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1", "set2"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set1"}}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch call {
			case 1:
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
						Since: 3,
						Till:  3},
				}, nil
			default:
				t.Error("Wrong calls")
				return nil, errors.New("some")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter([]string{"set1", "set2", "set3"}))

	res, err := splitUpdater.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	if len(res.ReferencedSegments) != 0 {
		t.Error("invalid referenced segment names. Got: ", res.ReferencedSegments)
	}

	if splitStorage.Split("split1") == nil {
		t.Error("split1 should be present")
	}
	if splitStorage.Split("split2") != nil {
		t.Error("split2 should not be present")
	}
	if splitStorage.Split("split3") == nil {
		t.Error("split3 should be present")
	}
}

func TestSplitSyncWithSetsInConfig(t *testing.T) {
	var call int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set2"}}
	mockedSplit4 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set2"}}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch call {
			case 1:
				if fetchOptions.ChangeNumber() != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{
						Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3, mockedSplit4},
						Since:  3,
						Till:   3},
				}, nil
			default:
				t.Error("Wrong calls")
				return nil, errors.New("some")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	flagSetFilter := flagsets.NewFlagSetFilter([]string{"set2", "set4"})
	splitStorage := mutexmap.NewMMSplitStorage(flagSetFilter)
	splitStorage.Update([]dtos.SplitDTO{}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	splitUpdater := NewSplitUpdater(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagSetFilter)

	res, err := splitUpdater.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	if len(res.ReferencedSegments) != 0 {
		t.Error("invalid referenced segment names. Got: ", res.ReferencedSegments)
	}

	s1 := splitStorage.Split("split1")
	if s1 != nil {
		t.Error("split1 should not be present")
	}
	s2 := splitStorage.Split("split2")
	if s2 == nil {
		t.Error("split2 should be present")
	}
	s3 := splitStorage.Split("split3")
	if s3 == nil {
		t.Error("split3 should be present")
	}
	s4 := splitStorage.Split("split4")
	if s4 == nil {
		t.Error("split4 should be present")
	}
}

func TestProcessMatchers(t *testing.T) {
	splitUpdater := NewSplitUpdater(mocks.MockSplitStorage{}, fetcherMock.MockSplitFetcher{}, logging.NewLogger(nil), mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil))
	splitChange := &dtos.SplitChangesDTO{
		FeatureFlags: dtos.FeatureFlagsDTO{Till: 1, Since: 1, Splits: []dtos.SplitDTO{
			{
				Name:   "split1",
				Status: Active,
				Conditions: []dtos.ConditionDTO{
					{
						ConditionType: "NEW_MATCHER",
						Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
						MatcherGroup: dtos.MatcherGroupDTO{
							Matchers: []dtos.MatcherDTO{
								{MatcherType: "NEW_MATCHER", KeySelector: nil},
							},
						},
					},
				},
			},
			{
				Name:   "split2",
				Status: Active,
				Conditions: []dtos.ConditionDTO{
					{
						ConditionType: condition.ConditionTypeRollout,
						Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
						MatcherGroup: dtos.MatcherGroupDTO{
							Matchers: []dtos.MatcherDTO{
								{MatcherType: matchers.MatcherTypeAllKeys, KeySelector: nil},
							},
						},
					},
				},
			},
		}}}
	toAdd, _ := splitUpdater.processFeatureFlagChanges(splitChange)

	if toAdd[0].Conditions[0].ConditionType != condition.ConditionTypeWhitelist {
		t.Error("ConditionType should be WHITELIST")
	}
	if toAdd[0].Conditions[0].MatcherGroup.Matchers[0].MatcherType != matchers.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}

	if toAdd[1].Conditions[0].ConditionType != condition.ConditionTypeRollout {
		t.Error("ConditionType should be ROLLOUT")
	}
	if toAdd[1].Conditions[0].MatcherGroup.Matchers[0].MatcherType != matchers.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}
}
