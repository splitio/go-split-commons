package grammar

import (
	"fmt"

	"github.com/splitio/go-split-commons/v8/engine/grammar/datatypes"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

// EqualToSemverMatcher struct to hold the semver to compare
type EqualToSemverMatcher struct {
	Matcher
	semver *datatypes.Semver
}

// Match will match if the comparisonValue is equal to the matchingValue
func (e *EqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	if e.semver == nil {
		return false
	}
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
func NewEqualToSemverMatcher(cmpVal string, negate bool, attributeName *string, logger logging.LoggerInterface) *EqualToSemverMatcher {
	semver, err := datatypes.BuildSemver(cmpVal)
	if err != nil {
		logger.Error(fmt.Sprintf("couldnt't build semver %s for EQUAL_TO_SEMVER matcher: %s", cmpVal, err.Error()))
	}
	return &EqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: semver,
	}
}

// GreaterThanOrEqualToSemverMatcher struct to hold the semver to compare
type GreaterThanOrEqualToSemverMatcher struct {
	Matcher
	semver *datatypes.Semver
}

// Match compares the semver of the key with the semver in the feature flag
func (g *GreaterThanOrEqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	if g.semver == nil {
		return false
	}
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

	result := semver.Compare(*g.semver) >= 0
	g.logger.Debug(fmt.Sprintf("%s >= %s | Result: %t", semver.Version(), g.semver.Version(), result))
	return result
}

// NewGreaterThanOrEqualToSemverMatcher returns an instance of GreaterThanOrEqualToSemverMatcher
func NewGreaterThanOrEqualToSemverMatcher(negate bool, compareTo string, attributeName *string, logger logging.LoggerInterface) *GreaterThanOrEqualToSemverMatcher {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		logger.Error(fmt.Sprintf("couldnt't build semver %s for GREATER_THAN_OR_EQUAL_TO_SEMVER matcher: %s", compareTo, err.Error()))
	}
	return &GreaterThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: semver,
	}
}

// LessThanOrEqualToSemverMatcher struct to hold the semver to compare
type LessThanOrEqualToSemverMatcher struct {
	Matcher
	semver *datatypes.Semver
}

// Match will match if the comparisonValue is less or equal to the matchingValue
func (l *LessThanOrEqualToSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	if l.semver == nil {
		return false
	}
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

	result := semver.Compare(*l.semver) <= 0
	l.logger.Debug(fmt.Sprintf("%s >= %s | Result: %t", semver.Version(), l.semver.Version(), result))
	return result
}

// NewLessThanOrEqualToSemverMatcher returns a pointer to a new instance of LessThanOrEqualToSemverMatcher
func NewLessThanOrEqualToSemverMatcher(compareTo string, negate bool, attributeName *string, logger logging.LoggerInterface) *LessThanOrEqualToSemverMatcher {
	semver, err := datatypes.BuildSemver(compareTo)
	if err != nil {
		logger.Error(fmt.Sprintf("couldnt't build semver %s for LESS_THAN_OR_EQUAL_TO_SEMVER matcher: %s", compareTo, err.Error()))
	}
	return &LessThanOrEqualToSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semver: semver,
	}
}

// BetweenSemverMatcher struct to hold the semver to compare
type BetweenSemverMatcher struct {
	Matcher
	startSemver *datatypes.Semver
	endSemver   *datatypes.Semver
}

// Match will match if the comparisonValue is between to the matchingValue
func (b *BetweenSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	if b.startSemver == nil || b.endSemver == nil {
		return false
	}
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

	result := semver.Compare(*b.startSemver) >= 0 && semver.Compare(*b.endSemver) <= 0
	b.logger.Debug(fmt.Sprintf("%s between %s and %s | Result: %t", semver.Version(), b.startSemver.Version(), b.endSemver.Version(), result))
	return result
}

// NewBetweenSemverMatcher returns a pointer to a new instance of BetweenSemverMatcher
func NewBetweenSemverMatcher(startVal string, endVal string, negate bool, attributeName *string, logger logging.LoggerInterface) *BetweenSemverMatcher {
	startSemver, err := datatypes.BuildSemver(startVal)
	if err != nil {
		logger.Error(fmt.Sprintf("couldnt't build semver %s for BETWEEN_SEMVER matcher, ignoring: %s", startVal, err.Error()))
	}
	endSemver, err := datatypes.BuildSemver(endVal)
	if err != nil {
		logger.Error(fmt.Sprintf("couldnt't build semver %s for BETWEEN_SEMVER matcher, ignoring: %s", endVal, err.Error()))
	}
	return &BetweenSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		startSemver: startSemver,
		endSemver:   endSemver,
	}
}

// InListSemverMatcher struct to hold the semver to compare
type InListSemverMatcher struct {
	Matcher
	semvers *set.ThreadUnsafeSet
}

// Match will match if the comparisonValue is in list to the matchingValue
func (i *InListSemverMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	if i.semvers.IsEmpty() {
		return false
	}
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
func NewInListSemverMatcher(setVersions []string, negate bool, attributeName *string, logger logging.LoggerInterface) *InListSemverMatcher {
	semvers := set.NewSet()
	for _, version := range setVersions {
		semver, err := datatypes.BuildSemver(version)
		if err == nil {
			semvers.Add(semver.Version())
		} else {
			logger.Error(fmt.Sprintf("couldnt't build semver %s for IN_LIST_SEMVER matcher, ignoring: %s", version, err.Error()))
		}
	}
	return &InListSemverMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		semvers: semvers,
	}
}
