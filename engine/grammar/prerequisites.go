package grammar

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"golang.org/x/exp/slices"
)

type PrerequisitesMatcher struct {
	Matcher
	prerequisistes      []dtos.Prerequisite
	dependencyEvaluator dependencyEvaluator
}

// NewPrerequisitesMatcher will return a new instance of PrerequisitesMatcher
func NewPrerequisitesMatcher(prerequisistes []dtos.Prerequisite, deedependencyEvaluator dependencyEvaluator) *PrerequisitesMatcher {
	return &PrerequisitesMatcher{
		prerequisistes:      prerequisistes,
		dependencyEvaluator: deedependencyEvaluator,
	}
}

// Match returns true if prerequisistes match
func (m *PrerequisitesMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {

	if m.prerequisistes == nil {
		return true
	}

	for _, value := range m.prerequisistes {
		treatment := m.dependencyEvaluator.EvaluateDependency(key, bucketingKey, value.FeatureFlagName, attributes)
		if !slices.Contains(value.Treatments, treatment) {
			return false
		}
	}
	return true
}
