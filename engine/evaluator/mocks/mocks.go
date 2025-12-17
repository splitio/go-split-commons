package mocks

import (
	"github.com/splitio/go-split-commons/v9/engine/evaluator"
	"github.com/stretchr/testify/mock"
)

// MockEvaluator mock evaluator
type MockEvaluator struct {
	mock.Mock
}

// EvaluateFeature mock
func (m MockEvaluator) EvaluateFeature(key string, bucketingKey *string, feature string, attributes map[string]interface{}) *evaluator.Result {
	args := m.Called(key, bucketingKey, feature, attributes)
	return args.Get(0).(*evaluator.Result)
}

// EvaluateFeatures mock
func (m MockEvaluator) EvaluateFeatures(key string, bucketingKey *string, features []string, attributes map[string]interface{}) evaluator.Results {
	args := m.Called(key, bucketingKey, features, attributes)
	return args.Get(0).(evaluator.Results)
}

// EvaluateFeaturesByFlagSets mock
func (m MockEvaluator) EvaluateFeatureByFlagSets(key string, bucketingKey *string, flagSets []string, attributes map[string]interface{}) evaluator.Results {
	args := m.Called(key, bucketingKey, flagSets, attributes)
	return args.Get(0).(evaluator.Results)
}
