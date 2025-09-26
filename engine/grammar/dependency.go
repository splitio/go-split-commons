package grammar

// DependencyMatcher will match if the evaluation of another split results in one of the treatments defined
// in the feature flag
type DependencyMatcher struct {
	Matcher
	feature             string
	treatments          []string
	dependencyEvaluator dependencyEvaluator
}

// Match will return true if the evaluation of another split results in one of the treatments defined in the
// feature flag
func (m *DependencyMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {

	result := m.dependencyEvaluator.EvaluateDependency(key, bucketingKey, m.feature, attributes)
	for _, treatment := range m.treatments {
		if treatment == result {
			return true
		}
	}

	return false
}

// NewDependencyMatcher will return a new instance of DependencyMatcher
func NewDependencyMatcher(negate bool, feature string, treatments []string, deedependencyEvaluator dependencyEvaluator) *DependencyMatcher {
	return &DependencyMatcher{
		Matcher: Matcher{
			negate: negate,
		},
		feature:             feature,
		treatments:          treatments,
		dependencyEvaluator: deedependencyEvaluator,
	}
}
