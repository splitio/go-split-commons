package split

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar"
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
	"github.com/stretchr/testify/mock"
)

var syncProxyFeatureFlagsRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString, grammar.MatcherTypeInSplitTreatment,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver, grammar.MatcherTypeInLargeSegment,
	grammar.MatcherTypeInRuleBasedSegment}
var syncProxyRuleBasedSegmentRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver, grammar.MatcherTypeInLargeSegment,
	grammar.MatcherTypeInRuleBasedSegment}

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)

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
	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))

	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(3).Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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
	mockedRuleBased1 := dtos.RuleBasedSegmentDTO{Name: "rb1", Status: "ACTIVE"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{Splits: []dtos.SplitDTO{mockedSplit1},
					Since: 2,
					Till:  2},
				RuleBasedSegments: dtos.RuleBasedSegmentsDTO{RuleBasedSegments: []dtos.RuleBasedSegmentDTO{mockedRuleBased1},
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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(12).Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(12).Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

	var till int64 = 1
	_, err := splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	_, err = splitUpdater.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 2 {
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
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(13).Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(13).Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(22).Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(22).Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)

	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)

	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)

	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder)

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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter([]string{"set1", "set2", "set3"}), ruleBuilder)

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
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagSetFilter, ruleBuilder)

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

func TestSynchronizeSplitsWithLowerTill(t *testing.T) {
	// Mock split storage with higher change number
	currentSince := int64(100)
	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return currentSince, nil },
		UpdateCall:       func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {},
	}

	// Mock rule based segment storage with higher change number
	currentRBSince := int64(150)
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int(currentRBSince))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Return()

	// Mock fetcher
	var fetchCalled bool
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(requestParams *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			fetchCalled = true
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{
					Since:  currentSince,
					Till:   currentSince,
					Splits: []dtos.SplitDTO{},
				},
				RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
					Since:             currentRBSince,
					Till:              currentRBSince,
					RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
				},
			}, nil
		},
	}

	// Mock telemetry storage
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, timestamp time.Time) {},
	}

	// Mock app monitor
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	// Create split updater
	splitUpdater := NewSplitUpdater(
		splitMockStorage,
		ruleBasedSegmentMockStorage,
		splitMockFetcher,
		logging.NewLogger(&logging.LoggerOptions{}),
		telemetryMockStorage,
		appMonitorMock,
		flagsets.NewFlagSetFilter(nil),
		ruleBuilder,
	)

	// Test case 1: till is less than both currentSince and currentRBSince
	till := int64(50)
	result, err := splitUpdater.SynchronizeSplits(&till)

	if err != nil {
		t.Error("Expected no error, got:", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}

	if fetchCalled {
		t.Error("Fetcher should not have been called when till is less than both currentSince and currentRBSince")
	}

	// Test case 2: till is equal to currentSince but less than currentRBSince
	till = currentSince
	result, err = splitUpdater.SynchronizeSplits(&till)

	if err != nil {
		t.Error("Expected no error when till equals currentSince, got:", err)
	}

	if !fetchCalled {
		t.Error("Fetcher should have been called when till equals currentSince (since currentRBSince is higher)")
	}

	// Test case 3: till is equal to currentRBSince but greater than currentSince
	till = currentRBSince
	result, err = splitUpdater.SynchronizeSplits(&till)

	if err != nil {
		t.Error("Expected no error when till equals currentRBSince, got:", err)
	}

	if !fetchCalled {
		t.Error("Fetcher should have been called when till equals currentRBSince")
	}
}

func TestSynchronizeFeatureFlagsRuleBasedUpdate(t *testing.T) {
	// Mock rule based segment storage with testify/mock
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}

	// Mock split storage
	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return 200, nil },
		UpdateCall:       func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {},
		AllCall:          func() []dtos.SplitDTO { return []dtos.SplitDTO{} },
	}

	// Mock fetcher
	var fetchCalled bool
	var fetchCount int
	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(requestParams *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			fetchCalled = true
			fetchCount++
			if fetchCount == 1 {
				return &dtos.SplitChangesDTO{
					FeatureFlags: dtos.FeatureFlagsDTO{
						Since:  100,
						Till:   200,
						Splits: []dtos.SplitDTO{},
					},
					RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
						Since:             100,
						Till:              200,
						RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
					},
				}, nil
			}
			return &dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{
					Since:  200,
					Till:   200,
					Splits: []dtos.SplitDTO{},
				},
				RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
					Since:             200,
					Till:              200,
					RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
				},
			}, nil
		},
	}

	// Mock telemetry storage
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, timestamp time.Time) {},
		RecordSyncErrorCall:      func(resource, status int) {},
	}

	// Mock app monitor
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))

	// Create split updater
	splitUpdater := NewSplitUpdater(
		splitMockStorage,
		ruleBasedSegmentMockStorage,
		splitMockFetcher,
		logging.NewLogger(&logging.LoggerOptions{}),
		telemetryMockStorage,
		appMonitorMock,
		flagsets.NewFlagSetFilter(nil),
		ruleBuilder,
	)

	// Test case 1: When rule-based segment change number is lower than current
	lowerChangeNumber := int64(100)
	ruleBasedSegment := &dtos.RuleBasedSegmentDTO{
		Name:         "test-segment",
		ChangeNumber: lowerChangeNumber,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				ConditionType: "WHITELIST",
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{},
				},
			},
		},
	}
	baseMessage := dtos.NewBaseMessage(time.Now().Unix(), "test-channel")
	baseUpdate := dtos.NewBaseUpdate(baseMessage, lowerChangeNumber)
	ffChange := *dtos.NewRuleBasedSegmentChangeUpdate(baseUpdate, nil, ruleBasedSegment)

	// Reset fetchCalled
	fetchCalled = false

	// Set up expectations for the first test case
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int(200))
	ruleBasedSegmentMockStorage.On("Update",
		mock.MatchedBy(func(toAdd []dtos.RuleBasedSegmentDTO) bool { return len(toAdd) == 0 }),
		mock.MatchedBy(func(toRemove []dtos.RuleBasedSegmentDTO) bool { return len(toRemove) == 0 }),
		int64(200)).Return()

	result, err := splitUpdater.SynchronizeFeatureFlags(&ffChange)

	if err != nil {
		t.Error("Expected no error, got:", err)
	}

	if result.RequiresFetch {
		t.Error("Expected RequiresFetch to be false when change number is lower than current")
	}

	if fetchCalled {
		t.Error("Fetcher should not have been called when change number is lower than current")
	}

	// Test case 2: When rule-based segment change number is higher than current
	higherChangeNumber := int64(300)
	ruleBasedSegment = &dtos.RuleBasedSegmentDTO{
		Name:         "test-segment",
		ChangeNumber: higherChangeNumber,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				ConditionType: "WHITELIST",
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{},
				},
			},
		},
	}
	baseMessage = dtos.NewBaseMessage(time.Now().Unix(), "test-channel")
	baseUpdate = dtos.NewBaseUpdate(baseMessage, higherChangeNumber)
	ffChange = *dtos.NewRuleBasedSegmentChangeUpdate(baseUpdate, nil, ruleBasedSegment)

	// Reset fetchCalled
	fetchCalled = false

	// Set up expectations for the second test case
	ruleBasedSegmentMockStorage = &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int(100))
	ruleBasedSegmentMockStorage.On("Update",
		mock.MatchedBy(func(toAdd []dtos.RuleBasedSegmentDTO) bool {
			return len(toAdd) == 1 && toAdd[0].ChangeNumber == higherChangeNumber
		}),
		mock.MatchedBy(func(toRemove []dtos.RuleBasedSegmentDTO) bool { return len(toRemove) == 0 }),
		higherChangeNumber).Return()

	// Create a new split updater for the second test case
	splitUpdater = NewSplitUpdater(
		splitMockStorage,
		ruleBasedSegmentMockStorage,
		splitMockFetcher,
		logging.NewLogger(&logging.LoggerOptions{}),
		telemetryMockStorage,
		appMonitorMock,
		flagsets.NewFlagSetFilter(nil),
		ruleBuilder,
	)

	result, err = splitUpdater.SynchronizeFeatureFlags(&ffChange)

	if err != nil {
		t.Error("Expected no error, got:", err)
	}

	if result.RequiresFetch {
		t.Error("Expected RequiresFetch to be false when change number is higher than current")
	}

	if fetchCalled {
		t.Error("Fetcher should not have been called when change number is higher than current")
	}

	if result.NewChangeNumber != higherChangeNumber {
		t.Errorf("Expected NewChangeNumber to be %d, got %d", higherChangeNumber, result.NewChangeNumber)
	}

	// Verify that the rule-based segment storage was updated with the higher change number
	ruleBasedSegmentMockStorage.AssertCalled(t, "Update", mock.Anything, mock.Anything, higherChangeNumber)
}

func TestProcessMatchers(t *testing.T) {
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}))
	splitUpdater := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, fetcherMock.MockSplitFetcher{}, logging.NewLogger(nil), mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder)
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
						ConditionType: grammar.ConditionTypeRollout,
						Partitions:    []dtos.PartitionDTO{{Treatment: "on", Size: 100}},
						MatcherGroup: dtos.MatcherGroupDTO{
							Matchers: []dtos.MatcherDTO{
								{MatcherType: grammar.MatcherTypeAllKeys, KeySelector: nil},
							},
						},
					},
				},
			},
		}}}
	toAdd, _ := splitUpdater.processFeatureFlagChanges(splitChange)

	if toAdd[0].Conditions[0].ConditionType != grammar.ConditionTypeWhitelist {
		t.Error("ConditionType should be WHITELIST")
	}
	if toAdd[0].Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}

	if toAdd[1].Conditions[0].ConditionType != grammar.ConditionTypeRollout {
		t.Error("ConditionType should be ROLLOUT")
	}
	if toAdd[1].Conditions[0].MatcherGroup.Matchers[0].MatcherType != grammar.MatcherTypeAllKeys {
		t.Error("MatcherType should be ALL_KEYS")
	}
}
