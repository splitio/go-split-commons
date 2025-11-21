package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDependencyEvaluator struct {
	mock.Mock
}

func (m *mockDependencyEvaluator) EvaluateDependency(key string, bucketingKey *string, featureName string, attributes map[string]interface{}) string {
	args := m.Called(key, bucketingKey, featureName, attributes)
	return args.String(0)
}

func TestPrerequisitesMatcher(t *testing.T) {
	tests := []struct {
		name          string
		prerequisites []dtos.Prerequisite
		key           string
		bucketingKey  *string
		attributes    map[string]interface{}
		expectedCalls []struct {
			feature   string
			treatment string
		}
		expectedResult bool
	}{
		{
			name:           "nil prerequisites should return true",
			prerequisites:  nil,
			key:            "test",
			attributes:     nil,
			expectedResult: true,
		},
		{
			name: "single matching prerequisite should return true",
			prerequisites: []dtos.Prerequisite{
				{
					FeatureFlagName: "feature1",
					Treatments:      []string{"on"},
				},
			},
			key:        "test",
			attributes: map[string]interface{}{"attr": "value"},
			expectedCalls: []struct {
				feature   string
				treatment string
			}{
				{feature: "feature1", treatment: "on"},
			},
			expectedResult: true,
		},
		{
			name: "multiple matching prerequisites should return true",
			prerequisites: []dtos.Prerequisite{
				{
					FeatureFlagName: "feature1",
					Treatments:      []string{"on"},
				},
				{
					FeatureFlagName: "feature2",
					Treatments:      []string{"on", "partial"},
				},
			},
			key:        "test",
			attributes: map[string]interface{}{"attr": "value"},
			expectedCalls: []struct {
				feature   string
				treatment string
			}{
				{feature: "feature1", treatment: "on"},
				{feature: "feature2", treatment: "partial"},
			},
			expectedResult: true,
		},
		{
			name: "non-matching prerequisite should return false",
			prerequisites: []dtos.Prerequisite{
				{
					FeatureFlagName: "feature1",
					Treatments:      []string{"on"},
				},
			},
			key:        "test",
			attributes: map[string]interface{}{"attr": "value"},
			expectedCalls: []struct {
				feature   string
				treatment string
			}{
				{feature: "feature1", treatment: "off"},
			},
			expectedResult: false,
		},
		{
			name: "one non-matching prerequisite among many should return false",
			prerequisites: []dtos.Prerequisite{
				{
					FeatureFlagName: "feature1",
					Treatments:      []string{"on"},
				},
				{
					FeatureFlagName: "feature2",
					Treatments:      []string{"on"},
				},
			},
			key:        "test",
			attributes: map[string]interface{}{"attr": "value"},
			expectedCalls: []struct {
				feature   string
				treatment string
			}{
				{feature: "feature1", treatment: "on"},
				{feature: "feature2", treatment: "off"},
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEvaluator := new(mockDependencyEvaluator)

			// Setup mock expectations
			for _, call := range tt.expectedCalls {
				mockEvaluator.On("EvaluateDependency", tt.key, tt.bucketingKey, call.feature, tt.attributes).Return(call.treatment)
			}

			matcher := NewPrerequisitesMatcher(tt.prerequisites, mockEvaluator)
			result := matcher.Match(tt.key, tt.attributes, tt.bucketingKey)

			assert.Equal(t, tt.expectedResult, result)
			mockEvaluator.AssertExpectations(t)
		})
	}
}
