package mutexmap

import (
	"sync"
	"testing"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine/grammar/constants"
	"github.com/stretchr/testify/assert"
)

func TestRuleBasedSegmentsStorage(t *testing.T) {
	// Initialize storage
	storage := NewRuleBasedSegmentsStorage()

	// Test initial state
	changeNumber, _ := storage.ChangeNumber()
	assert.Equal(t, int64(-1), changeNumber)
	assert.Empty(t, storage.All())
	assert.Empty(t, storage.RuleBasedSegmentNames())

	// Create test data
	ruleBased1 := dtos.RuleBasedSegmentDTO{
		Name: "rule1",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
							MatcherType: constants.MatcherTypeInSegment,
						},
					},
				},
			},
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{UserDefinedLargeSegment: &dtos.UserDefinedLargeSegmentMatcherDataDTO{LargeSegmentName: "ls1"}, MatcherType: constants.MatcherTypeInLargeSegment}}},
			},
		},
		Excluded: dtos.ExcludedDTO{
			Segments: []dtos.ExcludedSegmentDTO{
				{
					Name: "excluded1",
					Type: dtos.TypeStandard,
				},
				{
					Name: "excluded2",
					Type: dtos.TypeRuleBased,
				},
				{
					Name: "excluded3",
					Type: dtos.TypeLarge,
				},
			},
		},
	}

	ruleBased2 := dtos.RuleBasedSegmentDTO{
		Name: "rule2",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment2",
							},
							MatcherType: constants.MatcherTypeInRuleBasedSegment,
						},
					},
				},
			},
		},
	}

	// Test Update
	storage.Update([]dtos.RuleBasedSegmentDTO{ruleBased1, ruleBased2}, nil, 123)
	changeNumber, _ = storage.ChangeNumber()
	assert.Equal(t, int64(123), changeNumber)
	assert.Len(t, storage.All(), 2)

	// Test RuleBasedSegmentNames
	names := storage.RuleBasedSegmentNames()
	assert.Contains(t, names, "rule1")
	assert.Contains(t, names, "rule2")

	// Test GetSegments
	segments := storage.Segments()
	assert.True(t, segments.Has("segment1"), "segment1 should be in segments")
	assert.True(t, segments.Has("excluded1"), "excluded1 should be in segments")

	ls := storage.LargeSegments()
	assert.True(t, ls.Has("excluded3"), "excluded3 should be in large segments")
	assert.True(t, ls.Has("ls1"), "ls1 should be in large segments")

	// Test Contains
	assert.True(t, storage.Contains([]string{"rule1", "rule2"}), "should contain rule1 and rule2")
	assert.True(t, storage.Contains([]string{"rule1"}), "should contain rule1")
	assert.False(t, storage.Contains([]string{"nonexistent"}))

	// Test Remove
	storage.Update(nil, []dtos.RuleBasedSegmentDTO{ruleBased1}, 124)
	changeNumber, _ = storage.ChangeNumber()
	assert.Equal(t, int64(124), changeNumber)
	assert.Len(t, storage.All(), 1)
	assert.Contains(t, storage.RuleBasedSegmentNames(), "rule2")

	// Test Clear
	storage.Clear()
	assert.Empty(t, storage.All())
	assert.Empty(t, storage.RuleBasedSegmentNames())
}

func TestRuleBasedSegmentsStorageReplaceAll(t *testing.T) {
	// Initialize storage
	storage := NewRuleBasedSegmentsStorage()

	// Create initial test data
	initialRuleBased := dtos.RuleBasedSegmentDTO{
		Name: "initial",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
							MatcherType: constants.MatcherTypeInSegment,
						},
					},
				},
			},
		},
	}

	// Add initial data
	storage.Update([]dtos.RuleBasedSegmentDTO{initialRuleBased}, nil, 100)

	// Create new data for replacement
	newRuleBased := dtos.RuleBasedSegmentDTO{
		Name: "new",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment2",
							},
							MatcherType: constants.MatcherTypeInSegment,
						},
					},
				},
			},
		},
	}

	// Test ReplaceAll
	storage.ReplaceAll([]dtos.RuleBasedSegmentDTO{newRuleBased}, 200)

	// Verify change number was updated
	changeNumber, _ := storage.ChangeNumber()
	assert.Equal(t, int64(200), changeNumber)

	// Verify old data was removed
	oldSegment, err := storage.GetRuleBasedSegmentByName("initial")
	assert.Error(t, err)
	assert.Nil(t, oldSegment)

	// Verify new data was added
	newSegment, err := storage.GetRuleBasedSegmentByName("new")
	assert.NoError(t, err)
	assert.NotNil(t, newSegment)
	assert.Equal(t, "new", newSegment.Name)

	// Verify segments set
	segments := storage.Segments()
	assert.True(t, segments.Has("segment2"))
	assert.False(t, segments.Has("segment1"))

	// Test ReplaceAll with empty slice
	storage.ReplaceAll([]dtos.RuleBasedSegmentDTO{}, 300)

	// Verify storage is empty
	assert.Empty(t, storage.All())
	changeNumber, _ = storage.ChangeNumber()
	assert.Equal(t, int64(300), changeNumber)

	// Test ReplaceAll with multiple segments
	ruleBased1 := dtos.RuleBasedSegmentDTO{
		Name: "rule1",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment3",
							},
							MatcherType: constants.MatcherTypeInSegment,
						},
					},
				},
			},
		},
	}

	ruleBased2 := dtos.RuleBasedSegmentDTO{
		Name: "rule2",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment4",
							},
							MatcherType: constants.MatcherTypeInSegment,
						},
					},
				},
			},
		},
	}

	storage.ReplaceAll([]dtos.RuleBasedSegmentDTO{ruleBased1, ruleBased2}, 400)

	// Verify multiple segments were added
	assert.Len(t, storage.All(), 2)
	changeNumber, _ = storage.ChangeNumber()
	assert.Equal(t, int64(400), changeNumber)
	assert.True(t, storage.Contains([]string{"rule1", "rule2"}))

	// Verify segments set contains both segments
	segments = storage.Segments()
	assert.True(t, segments.Has("segment3"))
	assert.True(t, segments.Has("segment4"))
}

func TestRuleBasedSegmentsStorageEdgeCases(t *testing.T) {
	storage := NewRuleBasedSegmentsStorage()

	// Test SetChangeNumber explicitly
	err := storage.SetChangeNumber(100)
	assert.NoError(t, err)
	changeNumber, _ := storage.ChangeNumber()
	assert.Equal(t, int64(100), changeNumber)

	// Test GetSegments with different segment types
	ruleBased := dtos.RuleBasedSegmentDTO{
		Name: "rule1",
		Excluded: dtos.ExcludedDTO{
			Segments: []dtos.ExcludedSegmentDTO{
				{
					Name: "excluded1",
					Type: dtos.TypeStandard,
				},
				{
					Name: "excluded2",
					Type: dtos.TypeRuleBased, // This should not be included
				},
			},
		},
	}

	storage.Update([]dtos.RuleBasedSegmentDTO{ruleBased}, nil, 101)
	segments := storage.Segments()
	assert.True(t, segments.Has("excluded1"))
	assert.False(t, segments.Has("excluded2")) // Should not include non-standard segments

	// Test Contains with empty list
	assert.False(t, storage.Contains([]string{}))

	// Test Contains with nil
	assert.False(t, storage.Contains(nil))
}

func TestRuleBasedSegmentsStorageConcurrent(t *testing.T) {
	storage := NewRuleBasedSegmentsStorage()
	var wg sync.WaitGroup

	// Test concurrent updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ruleBased := dtos.RuleBasedSegmentDTO{
				Name: "rule1",
				Conditions: []dtos.RuleBasedConditionDTO{
					{
						MatcherGroup: dtos.MatcherGroupDTO{
							Matchers: []dtos.MatcherDTO{
								{
									UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
										SegmentName: "segment1",
									},
									MatcherType: constants.MatcherTypeInSegment,
								},
							},
						},
					},
				},
			}
			storage.Update([]dtos.RuleBasedSegmentDTO{ruleBased}, nil, int64(i))
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = storage.All()
			_ = storage.RuleBasedSegmentNames()
			_ = storage.Segments()
			_ = storage.Contains([]string{"segment1"})
		}()
	}

	wg.Wait()
}
