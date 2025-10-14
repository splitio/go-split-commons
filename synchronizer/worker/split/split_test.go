package split

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar"
	"github.com/splitio/go-split-commons/v7/flagsets"
	hcMock "github.com/splitio/go-split-commons/v7/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/api/specs"
	fetcherMock "github.com/splitio/go-split-commons/v7/service/mocks"
	"github.com/splitio/go-split-commons/v7/storage/inmemory"
	"github.com/splitio/go-split-commons/v7/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v7/storage/mocks"
	"github.com/splitio/go-split-commons/v7/telemetry"

	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"

	"github.com/stretchr/testify/assert"
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

func TestSplitSynchronizerError(t *testing.T) {
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.MatchedBy(func(req *service.FlagRequestParams) bool {
		return req.ChangeNumber() == -1
	})).Return(nil, &dtos.HTTPError{Code: 500, Message: "some"}).Once()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			assert.Equal(t, telemetry.SplitSync, resource)
			assert.Equal(t, 500, status)
		},
	}

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)

	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	_, err := splitUpdater.SynchronizeSplits(nil)
	assert.NotNil(t, err)

	splitMockStorage.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}

func TestSplitSynchronizerErrorScRequestURITooLong(t *testing.T) {
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.MatchedBy(func(req *service.FlagRequestParams) bool {
		return req.ChangeNumber() == -1
	})).Return(nil, &dtos.HTTPError{Code: 414, Message: "some"}).Once()

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			assert.Equal(t, telemetry.SplitSync, resource)
			assert.Equal(t, 414, status)
		},
	}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	_, err := splitUpdater.SynchronizeSplits(nil)
	assert.NotNil(t, err)
	splitMockStorage.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestSplitSynchronizer(t *testing.T) {
	before := time.Now().UTC()
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}

	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)
	splitMockStorage.On("Update", []dtos.SplitDTO{mockedSplit1, mockedSplit2}, []dtos.SplitDTO{mockedSplit3}, int64(3)).Once()

	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			assert.Equal(t, telemetry.SplitSync, resource)
			assert.True(t, tm.After(before))
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			assert.Equal(t, telemetry.SplitSync, resource)
		},
	}

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	_, err := splitUpdater.SynchronizeSplits(nil)
	assert.Nil(t, err)

	splitMockStorage.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestSplitSyncProcess(t *testing.T) {
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

	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()

	response1 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit4, mockedSplit5},
				Since:  3,
				Till:   3,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response1, nil).Once()

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Times(2)

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(2).Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	res, err := splitUpdater.SynchronizeSplits(nil)
	assert.Nil(t, err)
	assert.Len(t, res.ReferencedSegments, 0)
	assert.True(t, splitStorage.TrafficTypeExists("one"))
	assert.True(t, splitStorage.TrafficTypeExists("two"))

	res, err = splitUpdater.SynchronizeSplits(nil)
	assert.Nil(t, err)
	assert.Len(t, res.ReferencedSegments, 1)
	assert.Equal(t, res.ReferencedSegments[0], "someSegment")

	assert.Nil(t, splitStorage.Split("split1"))
	assert.Equal(t, mockedSplit2, *splitStorage.Split("split2"))
	assert.Nil(t, splitStorage.Split("split3"))
	assert.Equal(t, mockedSplit5, *splitStorage.Split("split4"))
	assert.False(t, splitStorage.TrafficTypeExists("one"))
	assert.True(t, splitStorage.TrafficTypeExists("two"))

	appMonitorMock.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}

func TestSplitTill(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedRuleBased1 := dtos.RuleBasedSegmentDTO{Name: "rb1", Status: "ACTIVE"}

	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			},
			RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
				RuleBasedSegments: []dtos.RuleBasedSegmentDTO{mockedRuleBased1},
				Since:             -1,
				Till:              -1,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Twice()

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Times(2)

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(4).Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(2).Return(-1)

	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	var till int64 = 1
	_, err := splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)
	_, err = splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)

	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestByPassingCDN(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  1,
				Till:   2,
			},
		},
	}
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithChangeNumber(-1).WithChangeNumberRB(-1).WithSpecVersion(common.StringRef(specs.FLAG_V1_3))).Return(response, nil).Once()

	response1 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			},
		},
	}
	splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithChangeNumber(2).WithChangeNumberRB(-1).WithSpecVersion(common.StringRef(specs.FLAG_V1_3))).Return(response1, nil).Times(10)
	response2 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  3,
				Till:   3,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.MatchedBy(func(params *service.FlagRequestParams) bool {
		return *params.Till() == 2
	})).Return(response2, nil).Once()

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(13).Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(12).Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	splitUpdater.onDemandFetchBackoffBase = 1
	splitUpdater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	_, err := splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)

	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestByPassingCDNLimit(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}

	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  1,
				Till:   2,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithChangeNumber(-1).WithChangeNumberRB(-1).WithSpecVersion(common.StringRef(specs.FLAG_V1_3))).Return(response, nil).Once()
	response1 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			},
		},
	}
	splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithChangeNumber(2).WithChangeNumberRB(-1).WithSpecVersion(common.StringRef(specs.FLAG_V1_3))).Return(response1, nil).Times(10)
	response2 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.MatchedBy(func(params *service.FlagRequestParams) bool {
		return *params.Till() == 2
	})).Return(response2, nil).Times(10)

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{}}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Times(22).Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Times(21).Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	splitUpdater.onDemandFetchBackoffBase = 1
	splitUpdater.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	_, err := splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)

	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestProcessFFChange(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := &mocks.SplitStorageMock{}
	ffStorageMock.On("ChangeNumber").Return(int64(43), nil)
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	result, _ := fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 12), nil, nil,
	))
	assert.False(t, result.RequiresFetch)
	splitMockFetcher.AssertExpectations(t)
	ffStorageMock.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestAddOrUpdateFeatureFlagNil(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := &mocks.SplitStorageMock{}
	ffStorageMock.On("ChangeNumber").Return(int64(-1), nil).Times(3)
	ffStorageMock.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(-1).Once()
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{},
				Since:  2,
				Till:   2,
			},
		},
	}
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2), nil, nil,
	))

	ffStorageMock.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestAddOrUpdateFeatureFlagPcnEquals(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := &mocks.SplitStorageMock{}
	ffStorageMock.On("ChangeNumber").Return(int64(2), nil)
	ffStorageMock.On("Update", mock.Anything, mock.Anything, int64(4))
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: Active}
	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4), common.Int64Ref(2), &featureFlag,
	))
	ffStorageMock.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestAddOrUpdateFeatureFlagArchive(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := &mocks.SplitStorageMock{}
	ffStorageMock.On("ChangeNumber").Return(int64(2), nil).Once()
	ffStorageMock.On("Update", mock.Anything, mock.Anything, int64(4)).Return().Once()
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: Archived}
	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4), common.Int64Ref(2), &featureFlag,
	))

	splitMockFetcher.AssertExpectations(t)
	ffStorageMock.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestAddOrUpdateFFCNFromStorageError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	ffStorageMock := &mocks.SplitStorageMock{}
	ffStorageMock.On("ChangeNumber").Return(int64(0), errors.New("error geting change number")).Times(3)
	ffStorageMock.On("Update", mock.Anything, mock.Anything, int64(2)).Return().Once()
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{},
				Since:  2,
				Till:   2,
			},
		},
	}
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil)
	fetcher := NewSplitUpdater(ffStorageMock, ruleBasedSegmentMockStorage, splitMockFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

	fetcher.SynchronizeFeatureFlags(dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2), nil, nil,
	))
}

func TestGetActiveFF(t *testing.T) {
	featureFlags := []dtos.SplitDTO{{Status: Active}, {Status: Active}}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, &fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	actives, inactives := s.processFeatureFlagChanges(featureFlags)
	assert.Len(t, actives, 2)
	assert.Len(t, inactives, 0)
}

func TestGetInactiveFF(t *testing.T) {
	featureFlags := []dtos.SplitDTO{{Status: Archived}, {Status: Archived}}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, &fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	actives, inactives := s.processFeatureFlagChanges(featureFlags)
	assert.Len(t, actives, 0)
	assert.Len(t, inactives, 2)
}

func TestGetActiveAndInactiveFF(t *testing.T) {
	featureFlags := []dtos.SplitDTO{{Status: Active}, {Status: Archived}}
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	s := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, &fetcherMock.MockSplitFetcher{}, nil, mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	actives, inactives := s.processFeatureFlagChanges(featureFlags)
	assert.Len(t, actives, 1)
	assert.Len(t, inactives, 1)
}

func TestSplitSyncWithSets(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1", "set2"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set1"}}

	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter([]string{"set1", "set2", "set3"}), ruleBuilder, false)

	res, err := splitUpdater.SynchronizeSplits(nil)
	assert.Nil(t, err)
	assert.Len(t, res.ReferencedSegments, 0)
	assert.NotNil(t, splitStorage.Split("split1"))
	assert.Nil(t, splitStorage.Split("split2"))
	assert.NotNil(t, splitStorage.Split("split3"))
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestSplitSyncWithSetsInConfig(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set1"}}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set4"}}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set5", "set2"}}
	mockedSplit4 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "one", Sets: []string{"set2"}}
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3, mockedSplit4},
				Since:  3,
				Till:   3,
			},
		},
	}

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

	flagSetFilter := flagsets.NewFlagSetFilter([]string{"set2", "set4"})
	splitStorage := mutexmap.NewMMSplitStorage(flagSetFilter)
	splitStorage.Update([]dtos.SplitDTO{}, nil, -1)
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(splitStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryStorage, appMonitorMock, flagSetFilter, ruleBuilder, false)

	res, err := splitUpdater.SynchronizeSplits(nil)
	assert.Nil(t, err)
	assert.Len(t, res.ReferencedSegments, 0)
	assert.Nil(t, splitStorage.Split("split1"))
	assert.NotNil(t, splitStorage.Split("split2"))
	assert.NotNil(t, splitStorage.Split("split3"))
	assert.NotNil(t, splitStorage.Split("split4"))
	splitMockFetcher.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
}

func TestSynchronizeSplitsWithLowerTill(t *testing.T) {
	// Mock split storage with higher change number
	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(100), nil)
	splitMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Return()

	// Mock rule based segment storage with higher change number
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(150))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Return()

	// Mock fetcher
	splitMockFetcher := &fetcherMock.MockSplitFetcher{}

	// Mock telemetry storage
	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, timestamp time.Time) {},
	}

	// Mock app monitor
	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Return()
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
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
		false,
	)

	// Test case 1: till is less than both currentSince and currentRBSince
	till := int64(50)
	result, err := splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, &UpdateResult{}, result)

	// Test case 2: till is equal to currentSince but less than currentRBSince
	till = 100
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{},
				Since:  100,
				Till:   100,
			},
			RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
				RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
				Since:             150,
				Till:              150,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()
	result, err = splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.NewChangeNumber)
	assert.Equal(t, int64(150), result.NewRBChangeNumber)

	// Test case 3: till is equal to currentRBSince but greater than currentSince
	till = 150

	response1 := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{},
				Since:  100,
				Till:   100,
			},
			RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
				RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
				Since:             150,
				Till:              150,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response1, nil).Once()
	result, err = splitUpdater.SynchronizeSplits(&till)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.NewChangeNumber)
	assert.Equal(t, int64(150), result.NewRBChangeNumber)

	splitMockFetcher.AssertExpectations(t)
	splitMockStorage.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
}

func TestSynchronizeFeatureFlagsRuleBasedUpdate(t *testing.T) {
	t.Run("Rule-based segment change number lower than current", func(t *testing.T) {
		ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
		splitMockStorage := &mocks.SplitStorageMock{}
		splitMockStorage.On("ChangeNumber").Return(int64(200), nil).Times(1)

		splitMockFetcher := &fetcherMock.MockSplitFetcher{}

		telemetryMockStorage := mocks.MockTelemetryStorage{
			RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
			RecordSuccessfulSyncCall: func(resource int, timestamp time.Time) {},
			RecordSyncErrorCall:      func(resource, status int) {},
		}
		appMonitorMock := &hcMock.ApplicationMonitorMock{}
		appMonitorMock.On("NotifyEvent", mock.Anything).Return().Once()

		largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
		ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)

		splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

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

		ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(200)).Times(2)

		result, err := splitUpdater.SynchronizeFeatureFlags(&ffChange)
		assert.Nil(t, err)
		assert.False(t, result.RequiresFetch)
		splitMockFetcher.AssertExpectations(t)
		splitMockStorage.AssertExpectations(t)
		ruleBasedSegmentMockStorage.AssertExpectations(t)
		appMonitorMock.AssertExpectations(t)
	})

	t.Run("Rule-based segment change number higher than current", func(t *testing.T) {
		ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
		splitMockStorage := &mocks.SplitStorageMock{}
		splitMockFetcher := &fetcherMock.MockSplitFetcher{}

		telemetryMockStorage := mocks.MockTelemetryStorage{
			RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
			RecordSuccessfulSyncCall: func(resource int, timestamp time.Time) {},
			RecordSyncErrorCall:      func(resource, status int) {},
		}
		appMonitorMock := &hcMock.ApplicationMonitorMock{}

		largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
		ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)

		splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)

		changeNumber := int64(300)
		ruleBasedSegment := &dtos.RuleBasedSegmentDTO{
			Name:         "test-segment",
			ChangeNumber: changeNumber,
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
		baseUpdate := dtos.NewBaseUpdate(baseMessage, changeNumber)
		ffChange := *dtos.NewRuleBasedSegmentChangeUpdate(baseUpdate, nil, ruleBasedSegment)

		ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(200)).Once()
		ruleBasedSegmentMockStorage.On("Update", []dtos.RuleBasedSegmentDTO{*ruleBasedSegment}, []dtos.RuleBasedSegmentDTO{}, changeNumber).Return().Once()

		result, err := splitUpdater.SynchronizeFeatureFlags(&ffChange)
		assert.Nil(t, err)
		assert.False(t, result.RequiresFetch)
		assert.Equal(t, changeNumber, result.NewRBChangeNumber)

		splitMockFetcher.AssertExpectations(t)
		splitMockStorage.AssertExpectations(t)
		ruleBasedSegmentMockStorage.AssertExpectations(t)
		appMonitorMock.AssertExpectations(t)
	})
}

func TestProcessMatchers(t *testing.T) {
	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(-1)
	largeSegmentStorage := &mocks.MockLargeSegmentStorage{}

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := NewSplitUpdater(mocks.MockSplitStorage{}, ruleBasedSegmentMockStorage, &fetcherMock.MockSplitFetcher{}, logging.NewLogger(nil), mocks.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil), ruleBuilder, false)
	featureFlags := []dtos.SplitDTO{
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
		}}
	toAdd, _ := splitUpdater.processFeatureFlagChanges(featureFlags)
	assert.Equal(t, grammar.ConditionTypeWhitelist, toAdd[0].Conditions[0].ConditionType)
	assert.Equal(t, grammar.MatcherTypeAllKeys, toAdd[0].Conditions[0].MatcherGroup.Matchers[0].MatcherType)
	assert.Equal(t, grammar.ConditionTypeRollout, toAdd[1].Conditions[0].ConditionType)
	assert.Equal(t, grammar.MatcherTypeAllKeys, toAdd[1].Conditions[0].MatcherGroup.Matchers[0].MatcherType)
}

func TestSplitProxyDowngrade(t *testing.T) {
	t.Run("Error both 1.3 and 1.1", func(t *testing.T) {
		splitMockStorage := &mocks.SplitStorageMock{}
		splitMockStorage.On("ChangeNumber").Return(int64(-1), nil).Times(2)
		ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
		ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(-1)).Times(2)
		largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
		splitMockFetcher := &fetcherMock.MockSplitFetcher{}
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(-1).WithChangeNumberRB(-1)).Return(nil, &dtos.HTTPError{Code: http.StatusBadRequest}).Once()
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_1)).WithChangeNumber(-1).WithChangeNumberRB(-1)).Return(nil, &dtos.HTTPError{Code: http.StatusBadRequest}).Once()
		telemetryMockStorage := mocks.MockTelemetryStorage{
			RecordSyncErrorCall: func(resource, status int) {
				assert.Equal(t, telemetry.SplitSync, resource)
				assert.Equal(t, http.StatusBadRequest, status)
			},
		}
		appMonitorMock := &hcMock.ApplicationMonitorMock{}
		appMonitorMock.On("NotifyEvent", mock.Anything).Return()
		ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
		splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, true)

		_, err := splitUpdater.SynchronizeSplits(nil)
		assert.NotNil(t, err)

		splitMockStorage.AssertExpectations(t)
		splitMockFetcher.AssertExpectations(t)
		ruleBasedSegmentMockStorage.AssertExpectations(t)
		appMonitorMock.AssertExpectations(t)
	})

	t.Run("From 1.3 to 1.1", func(t *testing.T) {
		mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
		response := &dtos.FFResponseLegacy{
			SplitChanges: dtos.SplitsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  3,
				Till:   3,
			},
		}

		splitMockStorage := &mocks.SplitStorageMock{}
		splitMockStorage.On("ChangeNumber").Return(int64(-1), nil).Times(2)
		splitMockStorage.On("Update", []dtos.SplitDTO{mockedSplit1}, []dtos.SplitDTO{}, int64(3)).Once()
		ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
		ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(-1)).Times(2)
		ruleBasedSegmentMockStorage.On("Update", []dtos.RuleBasedSegmentDTO{}, []dtos.RuleBasedSegmentDTO{}, int64(0)).Once().Return(-1)
		largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
		splitMockFetcher := &fetcherMock.MockSplitFetcher{}
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(-1).WithChangeNumberRB(-1)).Return(nil, &dtos.HTTPError{Code: http.StatusBadRequest}).Once()
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_1)).WithChangeNumber(-1).WithChangeNumberRB(-1)).Return(response, nil).Once()
		telemetryMockStorage := mocks.MockTelemetryStorage{
			RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
				assert.Equal(t, telemetry.SplitSync, resource)
			},
			RecordSyncLatencyCall: func(resource int, tm time.Duration) {
				assert.Equal(t, telemetry.SplitSync, resource)
			},
		}
		appMonitorMock := &hcMock.ApplicationMonitorMock{}
		appMonitorMock.On("NotifyEvent", mock.Anything).Return()
		ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
		splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, true)

		result, err := splitUpdater.SynchronizeSplits(nil)
		assert.Nil(t, err)
		assert.Equal(t, int64(3), result.NewChangeNumber)
		assert.Equal(t, int64(0), result.NewRBChangeNumber)
		assert.ElementsMatch(t, []string{"split1"}, result.UpdatedSplits)
		assert.Len(t, result.ReferencedSegments, 0)
		assert.Len(t, result.ReferencedLargeSegments, 0)
		assert.Greater(t, splitUpdater.lastSyncNewSpec, int64(0))
		assert.Equal(t, specs.FLAG_V1_1, splitUpdater.specVersion)
		splitMockStorage.AssertExpectations(t)
		splitMockFetcher.AssertExpectations(t)
		ruleBasedSegmentMockStorage.AssertExpectations(t)
		appMonitorMock.AssertExpectations(t)
	})

	t.Run("1.3 OK", func(t *testing.T) {
		mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
		mockedRuleBased1 := dtos.RuleBasedSegmentDTO{Name: "rb1", Status: "ACTIVE"}
		response := &dtos.FFResponseV13{
			SplitChanges: dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{
					Splits: []dtos.SplitDTO{mockedSplit1},
					Since:  -1,
					Till:   3,
				},
				RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
					Since:             -1,
					Till:              3,
					RuleBasedSegments: []dtos.RuleBasedSegmentDTO{mockedRuleBased1},
				},
			},
		}
		empty := &dtos.FFResponseV13{
			SplitChanges: dtos.SplitChangesDTO{
				FeatureFlags: dtos.FeatureFlagsDTO{
					Splits: []dtos.SplitDTO{},
					Since:  3,
					Till:   3,
				},
				RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
					Since:             3,
					Till:              3,
					RuleBasedSegments: []dtos.RuleBasedSegmentDTO{},
				},
			},
		}

		splitMockStorage := &mocks.SplitStorageMock{}
		splitMockStorage.On("ChangeNumber").Return(int64(3), nil).Times(2)
		splitMockStorage.On("ReplaceAll", []dtos.SplitDTO{mockedSplit1}, int64(3)).Once()
		splitMockStorage.On("Update", []dtos.SplitDTO{}, []dtos.SplitDTO{}, int64(3)).Once()
		ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
		ruleBasedSegmentMockStorage.On("ChangeNumber").Return(int64(3)).Times(2)
		ruleBasedSegmentMockStorage.On("ReplaceAll", []dtos.RuleBasedSegmentDTO{mockedRuleBased1}, int64(3)).Once()
		//ruleBasedSegmentMockStorage.On("Update", []dtos.RuleBasedSegmentDTO{mockedRuleBased1}, []dtos.RuleBasedSegmentDTO{}, int64(3)).Once().Return(3)
		ruleBasedSegmentMockStorage.On("Update", []dtos.RuleBasedSegmentDTO{}, []dtos.RuleBasedSegmentDTO{}, int64(3)).Once().Return(3)
		largeSegmentStorage := &mocks.MockLargeSegmentStorage{}
		splitMockFetcher := &fetcherMock.MockSplitFetcher{}
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(-1).WithChangeNumberRB(-1)).Return(response, nil).Once()
		splitMockFetcher.On("Fetch", service.MakeFlagRequestParams().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(3).WithChangeNumberRB(3)).Return(empty, nil).Once()
		telemetryMockStorage := mocks.MockTelemetryStorage{
			RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
				assert.Equal(t, telemetry.SplitSync, resource)
			},
			RecordSyncLatencyCall: func(resource int, tm time.Duration) {
				assert.Equal(t, telemetry.SplitSync, resource)
			},
		}
		appMonitorMock := &hcMock.ApplicationMonitorMock{}
		appMonitorMock.On("NotifyEvent", mock.Anything).Return()
		ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, largeSegmentStorage, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
		splitUpdater := NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, true)

		result, err := splitUpdater.SynchronizeSplits(nil)
		assert.Nil(t, err)
		assert.Equal(t, int64(3), result.NewChangeNumber)
		assert.Equal(t, int64(3), result.NewRBChangeNumber)
		assert.Len(t, result.ReferencedSegments, 0)
		assert.Len(t, result.ReferencedLargeSegments, 0)
		assert.ElementsMatch(t, []string{"split1"}, result.UpdatedSplits)
		assert.Equal(t, int64(-1), splitUpdater.lastSyncNewSpec)
		assert.Equal(t, specs.FLAG_V1_3, splitUpdater.specVersion)
		splitMockStorage.AssertExpectations(t)
		splitMockFetcher.AssertExpectations(t)
		ruleBasedSegmentMockStorage.AssertExpectations(t)
		appMonitorMock.AssertExpectations(t)
	})
}
