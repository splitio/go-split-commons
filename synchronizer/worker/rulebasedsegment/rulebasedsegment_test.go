package rulebasedsegment

import (
	"sync/atomic"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestProcessUpdate(t *testing.T) {
	var updateCalled int64
	mockedRB1 := dtos.RuleBasedSegmentDTO{
		Name:   "rb1",
		Status: Active,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: matcherTypeInSegment,
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
						},
					},
				},
			},
		},
		Excluded: dtos.ExcludedDTO{
			Segments: []dtos.ExcluededSegmentDTO{
				{
					Name: "segment2",
					Type: TypeStandard,
				},
			},
		},
	}

	ruleBasedSegmentMockStorage := mocks.MockRuleBasedSegmentStorage{
		UpdateCall: func(toAdd, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
			atomic.AddInt64(&updateCalled, 1)
		},
	}

	ruleBasedSegmentUpdater := NewRuleBasedSegmentUpdater(ruleBasedSegmentMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	splitChanges := &dtos.SplitChangesDTO{
		RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
			RuleBasedSegments: []dtos.RuleBasedSegmentDTO{mockedRB1},
			Since:             1,
			Till:              2,
		},
	}

	segments := ruleBasedSegmentUpdater.ProcessUpdate(splitChanges)

	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("Update should be called once")
	}

	if len(segments) != 2 {
		t.Error("Should return 2 segments")
	}

	found1, found2 := false, false
	for _, segment := range segments {
		if segment == "segment1" {
			found1 = true
		}
		if segment == "segment2" {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Error("Should return both segments")
	}
}

func TestProcessUpdateArchivedRB(t *testing.T) {
	var updateCalled int64
	mockedRB1 := dtos.RuleBasedSegmentDTO{
		Name:   "rb1",
		Status: Archived,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: matcherTypeInSegment,
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
						},
					},
				},
			},
		},
	}

	ruleBasedSegmentMockStorage := mocks.MockRuleBasedSegmentStorage{
		UpdateCall: func(toAdd, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
			atomic.AddInt64(&updateCalled, 1)
		},
	}

	ruleBasedSegmentUpdater := NewRuleBasedSegmentUpdater(ruleBasedSegmentMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	splitChanges := &dtos.SplitChangesDTO{
		RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
			RuleBasedSegments: []dtos.RuleBasedSegmentDTO{mockedRB1},
			Since:             1,
			Till:              2,
		},
	}

	segments := ruleBasedSegmentUpdater.ProcessUpdate(splitChanges)

	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("Update should be called once")
	}

	if len(segments) != 0 {
		t.Error("Should return no segments")
	}
}

func TestSynchronizeRuleBasedSegment(t *testing.T) {
	var updateCalled int64
	var changeNumberCalled int64

	mockedRB1 := dtos.RuleBasedSegmentDTO{
		Name:   "rb1",
		Status: Active,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: matcherTypeInSegment,
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
						},
					},
				},
			},
		},
	}

	ruleBasedSegmentMockStorage := mocks.MockRuleBasedSegmentStorage{
		UpdateCall: func(toAdd, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
			atomic.AddInt64(&updateCalled, 1)
		},
		ChangeNumberCall: func() int64 {
			atomic.AddInt64(&changeNumberCalled, 1)
			return 0
		},
	}

	ruleBasedSegmentUpdater := NewRuleBasedSegmentUpdater(ruleBasedSegmentMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	var changeNumber int64 = 2
	result, err := ruleBasedSegmentUpdater.SynchronizeRuleBasedSegment(dtos.NewRuleBasedChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2),
		&changeNumber,
		&mockedRB1,
	))

	if err != nil {
		t.Error("Should not return error")
	}

	if atomic.LoadInt64(&updateCalled) != 1 {
		t.Error("Update should be called once")
	}

	if atomic.LoadInt64(&changeNumberCalled) != 1 {
		t.Error("ChangeNumber should be called once")
	}

	if len(result.ReferencedSegments) != 1 {
		t.Error("Should return 1 segment")
	}

	if result.ReferencedSegments[0] != "segment1" {
		t.Error("Should return segment1")
	}

	if result.NewChangeNumber != 2 {
		t.Error("Should return change number 2")
	}

	if result.RequiresFetch {
		t.Error("Should not require fetch")
	}
}

func TestSynchronizeRuleBasedSegmentNoUpdate(t *testing.T) {
	var updateCalled int64
	var changeNumberCalled int64

	mockedRB1 := dtos.RuleBasedSegmentDTO{
		Name:   "rb1",
		Status: Active,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: matcherTypeInSegment,
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
						},
					},
				},
			},
		},
	}

	ruleBasedSegmentMockStorage := mocks.MockRuleBasedSegmentStorage{
		UpdateCall: func(toAdd, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
			atomic.AddInt64(&updateCalled, 1)
		},
		ChangeNumberCall: func() int64 {
			atomic.AddInt64(&changeNumberCalled, 1)
			return 3
		},
	}

	ruleBasedSegmentUpdater := NewRuleBasedSegmentUpdater(ruleBasedSegmentMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	var changeNumber int64 = 2
	result, err := ruleBasedSegmentUpdater.SynchronizeRuleBasedSegment(dtos.NewRuleBasedChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2),
		&changeNumber,
		&mockedRB1,
	))

	if err != nil {
		t.Error("Should not return error")
	}

	if atomic.LoadInt64(&updateCalled) != 0 {
		t.Error("Update should not be called")
	}

	if atomic.LoadInt64(&changeNumberCalled) != 1 {
		t.Error("ChangeNumber should be called once")
	}

	if len(result.ReferencedSegments) != 0 {
		t.Error("Should return no segments")
	}

	if result.NewChangeNumber != 0 {
		t.Error("Should return change number 0")
	}

	if !result.RequiresFetch {
		t.Error("Should require fetch")
	}
}
