package split

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	hcMock "github.com/splitio/go-split-commons/v4/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v4/service"
	fetcherMock "github.com/splitio/go-split-commons/v4/service/mocks"
	"github.com/splitio/go-split-commons/v4/storage/inmemory"
	"github.com/splitio/go-split-commons/v4/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v4/storage/mocks"
	"github.com/splitio/go-split-commons/v4/telemetry"
	backoffMock "github.com/splitio/go-toolkit/v5/backoff/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSplitSynchronizerError(t *testing.T) {
	var notifyEventCalled int64

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
			if !fetchOptions.CacheControlHeaders {
				t.Error("noCache should be true")
			}
			if changeNumber != -1 {
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

	splitSync := NewSplitFetcher(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	_, err := splitSync.SynchronizeSplits(nil)
	if err == nil {
		t.Error("It should return err")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
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
		FetchCall: func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
			if !fetchOptions.CacheControlHeaders {
				t.Error("noCache should be true")
			}
			if changeNumber != -1 {
				t.Error("Wrong changenumber passed")
			}
			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
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

	splitSync := NewSplitFetcher(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	_, err := splitSync.SynchronizeSplits(nil)
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
		FetchCall: func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch call {
			case 1:
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
					Since:  3,
					Till:   3,
				}, nil
			case 2:
				if changeNumber != 3 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit4, mockedSplit5},
					Since:  3,
					Till:   3,
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

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitSync := NewSplitFetcher(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock)

	res, err := splitSync.SynchronizeSplits(nil)
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

	res, err = splitSync.SynchronizeSplits(nil)
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
		FetchCall: func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitSync := NewSplitFetcher(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock)

	var till int64 = 1
	_, err := splitSync.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	_, err = splitSync.SynchronizeSplits(&till)
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
		FetchCall: func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch atomic.LoadInt64(&call) {
			case 1:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1},
					Since:  1,
					Till:   2,
				}, nil
			case 2, 3, 4, 5, 6, 7, 8, 9, 10, 11:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1},
					Since:  2,
					Till:   2,
				}, nil
			case 12:
				if fetchOptions.ChangeNumber == nil || *fetchOptions.ChangeNumber != 2 {
					t.Error("ChangeNumber flag should be set with value 2")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1},
					Since:  3,
					Till:   3,
				}, nil
			}

			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	splitSync := NewSplitFetcher(splitStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock)

	bMock := backoffMock.BackoffMock{
		NextCall: func() time.Duration {
			return 10 * time.Nanosecond
		},
		ResetCall: func() {},
	}
	splitSync.backoff = &bMock // overriding mock for taking less than expected

	var till int64 = 3
	_, err := splitSync.SynchronizeSplits(&till)
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
