package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers/datatypes"
)

// EqualToSemverMatcher struct to hold the semver to compare
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
	e.logger.Debug(fmt.Sprintf("%s == %s | Result: %t", semver.Version(), e.semver.Version(), result))
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

// GreaterThanOrEqualToSemverMatcher struct to hold the semver to compare
type GreaterThanOrEqualToSemverMatcher struct {
	Matcher
	semver datatypes.Semver
}

// Match compares the semver of the key with the semver in the feature flag
func (g *GreaterThanOrEqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	matchingKey, err := g.matchingKey(key, attributes)
	if err != nil {
		g.logger.Warning(fmt.Sprintf("GreaterThanOrEqualToSemverMatcher: %s", err.Error()))
		return false
	}

	asString, ok := matchingKey.(string)
	if !ok {
		g.logger.Error("GreaterThanOrEqualToSemverMatcher: Error type-asserting string")
		return false
	}

	semver, err := datatypes.BuildSemver(asString)
	if err != nil {
		g.logger.Error("GreaterThanOrEqualToSemverMatcher: Error parsing semver")
		return false
	}

	result := semver.Compare(g.semver) >= 0
	g.logger.Debug(fmt.Sprintf("%s >= %s | Result: %t", semver.Version(), g.semver.Version(), result))
	return result
}

// NewGreaterThanOrEqualToSemverMatcher returns an instance of GreaterThanOrEqualToSemverMatcher
func NewGreaterThanOrEqualToSemverMatcher(negate bool, compareTo string, attributeName *string) *GreaterThanOrEqualToSemverMatcher {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		return nil
	}
	return &GreaterThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}
}
