package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers/datatypes"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
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
func NewEqualToSemverMatcher(cmpVal string, negate bool, attributeName *string) (*EqualToSemverMatcher, error) {
	semver, err := datatypes.BuildSemver(cmpVal)
	if err != nil {
		return nil, err
	}
	return &EqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}, nil
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
func NewGreaterThanOrEqualToSemverMatcher(negate bool, compareTo string, attributeName *string) (*GreaterThanOrEqualToSemverMatcher, error) {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		return nil, err
	}
	return &GreaterThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}, nil
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
func NewLessThanOrEqualToSemverMatcher(compareTo string, negate bool, attributeName *string) (*LessThanOrEqualToSemverMatcher, error) {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		return nil, err
	}
	return &LessThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: *semver,
	}, nil
}

// BetweenSemverMatcher struct to hold the semver to compare
type BetweenSemverMatcher struct {
	Matcher
	startSemver datatypes.Semver
	endSemver   datatypes.Semver
}

// Match will match if the comparisonValue is between to the matchingValue
func (b *BetweenSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	matchingKey, err := b.matchingKey(key, attributes)
	if err != nil {
		b.logger.Warning(fmt.Sprintf("BetweenSemverMatcher: %s", err.Error()))
		return false
	}

	asString, ok := matchingKey.(string)
	if !ok {
		b.logger.Error("BetweenSemverMatcher: Error type-asserting string")
		return false
	}

	semver, err := datatypes.BuildSemver(asString)
	if err != nil {
		b.logger.Error("BetweenSemverMatcher: Error parsing semver")
		return false
	}

	result := semver.Compare(b.startSemver) >= 0 && semver.Compare(b.endSemver) <= 0
	b.logger.Debug(fmt.Sprintf("%s between %s and %s | Result: %t", semver.Version(), b.startSemver.Version(), b.endSemver.Version(), result))
	return result
}

// NewBetweenSemverMatcher returns a pointer to a new instance of BetweenSemverMatcher
func NewBetweenSemverMatcher(startVal string, endVal string, negate bool, attributeName *string) (*BetweenSemverMatcher, error) {
	startSemver, err := datatypes.BuildSemver(startVal)
	if err != nil {
		return nil, err
	}
	endSemver, err := datatypes.BuildSemver(endVal)
	if err != nil {
		return nil, err
	}
	return &BetweenSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		startSemver: *startSemver,
		endSemver:   *endSemver,
	}, nil
}

// InListSemverMatcher struct to hold the semver to compare
type InListSemverMatcher struct {
	Matcher
	semvers *set.ThreadUnsafeSet
}

// Match will match if the comparisonValue is in list to the matchingValue
func (i *InListSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	matchingKey, err := i.matchingKey(key, attributes)
	if err != nil {
		i.logger.Warning(fmt.Sprintf("InListSemverMatcher: %s", err.Error()))
		return false
	}

	asString, ok := matchingKey.(string)
	if !ok {
		i.logger.Error("InListSemverMatcher: Error type-asserting string")
		return false
	}

	semver, err := datatypes.BuildSemver(asString)
	if err != nil {
		i.logger.Error("InListSemverMatcher: Error parsing semver")
		return false
	}
	return i.semvers.Has(semver.Version())
}

// NewInListSemverMatcher returns a pointer to a new instance of InListSemverMatcher
func NewInListSemverMatcher(cmpList []string, negate bool, attributeName *string) (*InListSemverMatcher, []error) {
	semvers := set.NewSet()
	var errs []error
	for _, str := range cmpList {
		semver, err := datatypes.BuildSemver(str)
		if err == nil {
			semvers.Add(semver.Version())
		} else {
			errs = append(errs, err)
		}
	}
	return &InListSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semvers: semvers,
	}, errs
}
