package mutexmap

import (
	"sync"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/stretchr/testify/assert"
)

func TestRuleBasedSegmentsStorage(t *testing.T) {
	// Initialize storage
	storage := NewRuleBasedSegmentsStorage()

	// Test initial state
	assert.Equal(t, int64(-1), storage.ChangeNumber())
	assert.Empty(t, storage.All())
	assert.Empty(t, storage.RuleBasedSegmentNames())

	// Create test data
	ruleBased1 := dtos.RuleBasedSegment{
		Name: "rule1",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment1",
							},
						},
					},
				},
			},
		},
		Excluded: dtos.Excluded{
			ExcludedSegment: []dtos.ExcludedSegment{
				{
					Name: "excluded1",
					Type: dtos.TypeStandard,
				},
			},
		},
	}

	ruleBased2 := dtos.RuleBasedSegment{
		Name: "rule2",
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment2",
							},
						},
					},
				},
			},
		},
	}

	// Test Update
	storage.Update([]dtos.RuleBasedSegment{ruleBased1, ruleBased2}, nil, 123)
	assert.Equal(t, int64(123), storage.ChangeNumber())
	assert.Len(t, storage.All(), 2)

	// Test RuleBasedSegmentNames
	names := storage.RuleBasedSegmentNames()
	assert.Contains(t, names, "rule1")
	assert.Contains(t, names, "rule2")

	// Test GetSegments
	segments := storage.GetSegments()
	// Print segments for debugging
	t.Logf("Segments in set: %v", segments.List())
	assert.True(t, segments.Has("segment1"), "segment1 should be in segments")
	assert.True(t, segments.Has("segment2"), "segment2 should be in segments")
	assert.True(t, segments.Has("excluded1"), "excluded1 should be in segments")

	// Test Contains
	assert.True(t, storage.Contains([]string{"segment1", "excluded1"}), "should contain segment1 and excluded1")
	assert.True(t, storage.Contains([]string{"segment2"}), "should contain segment2")
	assert.False(t, storage.Contains([]string{"nonexistent"}))

	// Test Remove
	storage.Update(nil, []dtos.RuleBasedSegment{ruleBased1}, 124)
	assert.Equal(t, int64(124), storage.ChangeNumber())
	assert.Len(t, storage.All(), 1)
	assert.Contains(t, storage.RuleBasedSegmentNames(), "rule2")

	// Test Clear
	storage.Clear()
	assert.Empty(t, storage.All())
	assert.Empty(t, storage.RuleBasedSegmentNames())
}

func TestRuleBasedSegmentsStorageEdgeCases(t *testing.T) {
	storage := NewRuleBasedSegmentsStorage()

	// Test SetChangeNumber explicitly
	err := storage.SetChangeNumber(100)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), storage.ChangeNumber())

	// Test GetSegments with different segment types
	ruleBased := dtos.RuleBasedSegment{
		Name: "rule1",
		Excluded: dtos.Excluded{
			ExcludedSegment: []dtos.ExcludedSegment{
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

	storage.Update([]dtos.RuleBasedSegment{ruleBased}, nil, 101)
	segments := storage.GetSegments()
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
			ruleBased := dtos.RuleBasedSegment{
				Name: "rule1",
				Conditions: []dtos.RuleBasedConditionDTO{
					{
						MatcherGroup: dtos.MatcherGroupDTO{
							Matchers: []dtos.MatcherDTO{
								{
									UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
										SegmentName: "segment1",
									},
								},
							},
						},
					},
				},
			}
			storage.Update([]dtos.RuleBasedSegment{ruleBased}, nil, int64(i))
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = storage.All()
			_ = storage.RuleBasedSegmentNames()
			_ = storage.GetSegments()
			_ = storage.Contains([]string{"segment1"})
		}()
	}

	wg.Wait()
}
