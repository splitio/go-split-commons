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
	matcher := NewInRuleBasedSegmentMatcher(false, "segment1", &attributeName)

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
					Segments: []dtos.ExcluededSegmentDTO{},
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
					Segments: []dtos.ExcluededSegmentDTO{
						{Name: "segment2", Type: dtos.TypeStandard},
					},
				},
			},
			expectedResult: false,
		},
	}

	logger := logging.NewLogger(&logging.LoggerOptions{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := &mockRuleBasedSegmentStorage{}
			mockStorage.On("GetRuleBasedSegmentByName", "segment1").Return(tt.segment, nil)

			matcher := NewInRuleBasedSegmentMatcher(false, "segment1", nil)
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
	matcher := NewInRuleBasedSegmentMatcher(false, "segment1", nil)
	matcher.Context = injection.NewContext()
	matcher.logger = logger

	result := matcher.Match("key1", nil, nil)
	assert.False(t, result)
}
