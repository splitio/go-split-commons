package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRuleBasedSegmentStorage struct {
	mock.Mock
	storage.RuleBasedSegmentStorageConsumer
}

func (m *mockRuleBasedSegmentStorage) GetRuleBasedSegmentByName(name string) (*dtos.RuleBasedSegmentDTO, error) {
	args := m.Called(name)
	return args.Get(0).(*dtos.RuleBasedSegmentDTO), args.Error(1)
}

func TestNewInRuleBasedSegmentMatcher(t *testing.T) {
	attributeName := "attr1"
	matcher := NewInRuleBasedSegmentMatcher(false, "segment1", &attributeName, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, nil))

	assert.NotNil(t, matcher)
	assert.Equal(t, false, matcher.negate)
	assert.Equal(t, "segment1", matcher.name)
	assert.Equal(t, &attributeName, matcher.attributeName)
}

func TestInRuleBasedSegmentMatcher_Match(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		segment        *dtos.RuleBasedSegmentDTO
		expectedResult bool
	}{
		{
			name: "key in excluded keys",
			key:  "key1",
			segment: &dtos.RuleBasedSegmentDTO{
				Excluded: dtos.ExcludedDTO{
					Keys:     []string{"key1", "key2"},
					Segments: []dtos.ExcludedSegmentDTO{},
				},
			},
			expectedResult: false,
		},
		{
			name: "key in excluded segment",
			key:  "key3",
			segment: &dtos.RuleBasedSegmentDTO{
				Excluded: dtos.ExcludedDTO{
					Keys: []string{},
					Segments: []dtos.ExcludedSegmentDTO{
						{Name: "segment2", Type: dtos.TypeStandard},
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "key in excluded rule-based segment",
			key:  "key4",
			segment: &dtos.RuleBasedSegmentDTO{
				Excluded: dtos.ExcludedDTO{
					Keys: []string{},
					Segments: []dtos.ExcludedSegmentDTO{
						{Name: "segment3", Type: dtos.TypeRuleBased},
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "matches condition",
			key:  "key5",
			segment: &dtos.RuleBasedSegmentDTO{
				Excluded: dtos.ExcludedDTO{},
				Conditions: []dtos.RuleBasedConditionDTO{
					{
						ConditionType: "MATCHES_STRING",
						MatcherGroup: dtos.MatcherGroupDTO{
							Combiner: "AND",
							Matchers: []dtos.MatcherDTO{
								{
									MatcherType: "EQUAL_TO",
									KeySelector: &dtos.KeySelectorDTO{Attribute: nil},
									Whitelist: &dtos.WhitelistMatcherDataDTO{
										Whitelist: []string{"key5"},
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
		},
	}

	logger := logging.NewLogger(&logging.LoggerOptions{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := &mockRuleBasedSegmentStorage{}
			mockStorage.On("GetRuleBasedSegmentByName", "segment1").Return(tt.segment, nil)

			// For recursive rule-based segment test
			if tt.name == "key in excluded rule-based segment" {
				nestedSegment := &dtos.RuleBasedSegmentDTO{
					Excluded: dtos.ExcludedDTO{
						Keys: []string{"key4"},
					},
				}
				mockStorage.On("GetRuleBasedSegmentByName", "segment3").Return(nestedSegment, nil)
			}

			matcher := NewInRuleBasedSegmentMatcher(false, "segment1", nil, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, nil))
			ctx := injection.NewContext()
			ctx.AddDependency("ruleBasedSegmentStorage", mockStorage)
			matcher.Context = ctx
			matcher.logger = logger

			result := matcher.Match(tt.key, nil, nil)
			assert.Equal(t, tt.expectedResult, result)
			mockStorage.AssertExpectations(t)
		})
	}
}

func TestInRuleBasedSegmentMatcher_MissingStorage(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	matcher := NewInRuleBasedSegmentMatcher(false, "segment1", nil, NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, nil))
	matcher.Context = injection.NewContext()
	matcher.logger = logger

	result := matcher.Match("key1", nil, nil)
	assert.False(t, result)
}
