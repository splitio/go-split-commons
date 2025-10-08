package synchronizer

import (
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar"
	"github.com/splitio/go-split-commons/v7/flagsets"
	hcMock "github.com/splitio/go-split-commons/v7/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/api"
	httpMocks "github.com/splitio/go-split-commons/v7/service/mocks"
	"github.com/splitio/go-split-commons/v7/storage/mocks"
	"github.com/splitio/go-split-commons/v7/synchronizer/worker/split"

	"github.com/splitio/go-toolkit/v5/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var goClientFeatureFlagsRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString, grammar.MatcherTypeInSplitTreatment,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver,
	grammar.MatcherTypeInRuleBasedSegment}
var goClientRuleBasedSegmentRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver,
	grammar.MatcherTypeInRuleBasedSegment}

func TestLocalSyncAllError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitFetcher := &httpMocks.MockSplitFetcher{}
	splitFetcher.On("Fetch", mock.Anything).Return(nil, errors.New("Some")).Once()
	splitAPI := api.SplitAPI{SplitFetcher: splitFetcher}
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)
	telemetryMockStorage := mocks.MockTelemetryStorage{}
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()

	flagSetFilter := flagsets.NewFlagSetFilter(nil)
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, goClientFeatureFlagsRules, goClientRuleBasedSegmentRules, logger, nil)
	splitUpdater := split.NewSplitUpdater(
		splitMockStorage,
		ruleBasedSegmentMockStorage,
		splitAPI.SplitFetcher,
		logger,
		telemetryMockStorage,
		appMonitorMock,
		flagSetFilter,
		ruleBuilder,
	)
	splitUpdater.SetRuleBasedSegmentStorage(ruleBasedSegmentMockStorage)

	workers := Workers{
		SplitUpdater: splitUpdater,
	}

	syncForTest := &Local{
		splitTasks: SplitTasks{},
		workers:    workers,
		logger:     logger,
	}

	err := syncForTest.SyncAll()
	assert.NotNil(t, err)

	splitFetcher.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	splitMockStorage.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}

func TestLocalSyncAllOk(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitFetcher := &httpMocks.MockSplitFetcher{}
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
				Since:  3,
				Till:   3,
			},
		},
	}
	splitFetcher.On("Fetch",
		mock.MatchedBy(func(req *service.FlagRequestParams) bool {
			return req.ChangeNumber() == -1
		})).Return(response, nil).Once()
	splitAPI := api.SplitAPI{SplitFetcher: splitFetcher}
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)
	splitMockStorage.On("Update", []dtos.SplitDTO{mockedSplit1, mockedSplit2}, []dtos.SplitDTO{}, int64(3)).Once()
	segmentMockStorage := mocks.MockSegmentStorage{}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	syncForTest := NewLocal(&LocalConfig{}, &splitAPI, splitMockStorage, segmentMockStorage, largeSegmentStorage, ruleBasedSegmentMockStorage, logger, telemetryMockStorage, appMonitorMock)
	err := syncForTest.SyncAll()
	assert.Nil(t, err)

	splitFetcher.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	splitMockStorage.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}

func TestLocalPeriodicFetching(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitFetcher := &httpMocks.MockSplitFetcher{}
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
				Since:  3,
				Till:   3,
			},
		},
	}

	splitFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()
	splitAPI := api.SplitAPI{SplitFetcher: splitFetcher}
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)
	splitMockStorage.On("Update", []dtos.SplitDTO{mockedSplit1, mockedSplit2}, []dtos.SplitDTO{}, int64(3)).Once()
	segmentMockStorage := mocks.MockSegmentStorage{}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	syncForTest := NewLocal(&LocalConfig{RefreshEnabled: true, SplitPeriod: 1}, &splitAPI, splitMockStorage, segmentMockStorage, largeSegmentStorage, ruleBasedSegmentMockStorage, logger, telemetryMockStorage, appMonitorMock)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 1500)
	syncForTest.StopPeriodicFetching()

	splitFetcher.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	splitMockStorage.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}
