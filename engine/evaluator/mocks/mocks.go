package mocks

import "github.com/splitio/go-split-commons/v8/engine/evaluator"

// MockEvaluator mock evaluator
type MockEvaluator struct {
	EvaluateFeatureCall           func(key string, bucketingKey *string, feature string, attributes map[string]interface{}) *evaluator.Result
	EvaluateFeaturesCall          func(key string, bucketingKey *string, features []string, attributes map[string]interface{}) evaluator.Results
	EvaluateFeatureByFlagSetsCall func(key string, bucketingKey *string, flagSets []string, attributes map[string]interface{}) evaluator.Results
}

// EvaluateFeature mock
func (m MockEvaluator) EvaluateFeature(key string, bucketingKey *string, feature string, attributes map[string]interface{}) *evaluator.Result {
	return m.EvaluateFeatureCall(key, bucketingKey, feature, attributes)
}

// EvaluateFeatures mock
func (m MockEvaluator) EvaluateFeatures(key string, bucketingKey *string, features []string, attributes map[string]interface{}) evaluator.Results {
	return m.EvaluateFeaturesCall(key, bucketingKey, features, attributes)
}

// EvaluateFeaturesByFlagSets mock
func (m MockEvaluator) EvaluateFeatureByFlagSets(key string, bucketingKey *string, flagSets []string, attributes map[string]interface{}) evaluator.Results {
	return m.EvaluateFeatureByFlagSetsCall(key, bucketingKey, flagSets, attributes)
}
