package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers/datatypes"
)

type EqualToSemverMatcher struct {
	Matcher
	semver datatypes.Semver
}

type LessThanOrEqualToSemverMatcher struct {
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

// Match will match if the comparisonValue is less or equal to the matchingValue
func (l *LessThanOrEqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	matchingKey, err := l.matchingKey(key, attributes)
	if err != nil {
		l.logger.Warning(fmt.Sprintf("LessThanOrEqualToSemverMatcher: %s", err.Error()))
		return false
	}

	asString, ok := matchingKey.(string)
	if !ok {
		l.logger.Error("LessThanOrEqualToSemverMatcher: Error type-asserting string")
		return false
	}

	semver, err := datatypes.BuildSemver(asString)
	if err != nil {
		l.logger.Error("LessThanOrEqualToSemverMatcher: Error parsing semver")
		return false
	}

	result := semver.Compare(l.semver) <= 0
	l.logger.Debug(fmt.Sprintf("%s >= %s | Result: %t", semver.Version(), l.semver.Version(), result))
	return result
}

// NewLessThanOrEqualToSemverMatcher returns a pointer to a new instance of LessThanOrEqualToSemverMatcher
func NewLessThanOrEqualToSemverMatcher(cmpVal string, negate bool, attributeName *string) *LessThanOrEqualToSemverMatcher {
	semver, _ := datatypes.BuildSemver(cmpVal)
	return &LessThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}
}
