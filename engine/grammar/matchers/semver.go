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
	semver, err := datatypes.BuildSemver(cmpVal)
	if err != nil {
		return nil
	}
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

// LessThanOrEqualToSemverMatcher struct to hold the semver to compare
type LessThanOrEqualToSemverMatcher struct {
	Matcher
	semver datatypes.Semver
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
func NewLessThanOrEqualToSemverMatcher(compareTo string, negate bool, attributeName *string) *LessThanOrEqualToSemverMatcher {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		return nil
	}
	return &LessThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}
}

// BetweenSemverMatcher struct to hold the semver to compare
type BetweenSemverMatcher struct {
	Matcher
	startSemver datatypes.Semver
	endSemver   datatypes.Semver
}

// NewLessThanOrEqualToSemverMatcher returns a pointer to a new instance of LessThanOrEqualToSemverMatcher
func NewBetweenSemverMatcher(startVal string, endVal string, negate bool, attributeName *string) *BetweenSemverMatcher {
	startSemver, _ := datatypes.BuildSemver(startVal)
	endSemver, _ := datatypes.BuildSemver(endVal)
	return &BetweenSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		startSemver: *startSemver,
		endSemver:   *endSemver,
	}
}
