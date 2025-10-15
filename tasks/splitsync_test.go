package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar"
	"github.com/splitio/go-split-commons/v7/flagsets"
	hcMock "github.com/splitio/go-split-commons/v7/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v7/service/api/specs"
	fetcherMock "github.com/splitio/go-split-commons/v7/service/mocks"
	"github.com/splitio/go-split-commons/v7/storage/mocks"
	"github.com/splitio/go-split-commons/v7/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v7/telemetry"
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

func TestSplitSyncTask(t *testing.T) {
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}

	splitMockStorage := &mocks.SplitStorageMock{}
	splitMockStorage.On("ChangeNumber").Return(int64(-1), nil)
	splitMockStorage.On("Update", []dtos.SplitDTO{mockedSplit1, mockedSplit2}, []dtos.SplitDTO{mockedSplit3}, int64(3)).Once()

	splitMockFetcher := &fetcherMock.MockSplitFetcher{}
	response := &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			},
		},
	}
	splitMockFetcher.On("Fetch", mock.Anything).Return(response, nil).Once()

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
	}

	appMonitorMock := &hcMock.ApplicationMonitorMock{}
	appMonitorMock.On("NotifyEvent", mock.Anything).Once()

	ruleBasedSegmentMockStorage := &mocks.MockRuleBasedSegmentStorage{}
	ruleBasedSegmentMockStorage.On("ChangeNumber").Twice().Return(int64(-1))
	ruleBasedSegmentMockStorage.On("Update", mock.Anything, mock.Anything, mock.Anything).Once().Return(-1)

	ruleBuilder := grammar.NewRuleBuilder(nil, ruleBasedSegmentMockStorage, nil, goClientFeatureFlagsRules, goClientRuleBasedSegmentRules, logging.NewLogger(&logging.LoggerOptions{}), nil)
	splitUpdater := split.NewSplitUpdater(splitMockStorage, ruleBasedSegmentMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil), ruleBuilder, false, specs.FLAG_V1_3)

	splitTask := NewFetchSplitsTask(
		splitUpdater,
		1,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	splitTask.Start()
	time.Sleep(2 * time.Second)
	assert.True(t, splitTask.IsRunning(), "Split fetching task should be running")

	splitTask.Stop(false)
	assert.False(t, splitTask.IsRunning(), "Split fetching task should be stopped")

	splitMockStorage.AssertExpectations(t)
	splitMockFetcher.AssertExpectations(t)
	appMonitorMock.AssertExpectations(t)
	ruleBasedSegmentMockStorage.AssertExpectations(t)
}
