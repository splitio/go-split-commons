package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers/datatypes"
)

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
	fmt.Println("original", g.semver.Version(), "comming", asString, " result ", result, "compare ", semver.Compare(g.semver))

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
