package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers/datatypes"
)

type EqualToSemverMatcher struct {
	Matcher
	semver datatypes.Semver
}

// Match will match if the comparisonValue is equal to the matchingValue
func (e *EqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	matchingKey, err := e.matchingKey(key, attributes)
	if err != nil {
		e.logger.Warning(fmt.Sprintf("EqualToSemverMatcher: %s", err.Error()))
		return false
	}

	asString, ok := matchingKey.(string)
	if !ok {
		e.logger.Error("EqualToSemverMatcher: Error type-asserting string")
		return false
	}

	semver, err := datatypes.BuildSemver(asString)
	if err != nil {
		e.logger.Error("EqualToSemverMatcher: Error parsing semver")
		return false
	}

	result := semver.Version() == e.semver.Version()
	e.logger.Debug(fmt.Sprintf("%s >= %s | Result: %t", semver.Version(), e.semver.Version(), result))
	return result
}

// NewEqualToSemverMatcher returns a pointer to a new instance of EqualToSemverMatcher
func NewEqualToSemverMatcher(cmpVal string, negate bool, attributeName *string) *EqualToSemverMatcher {
	semver, _ := datatypes.BuildSemver(cmpVal)
	return &EqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}
}
