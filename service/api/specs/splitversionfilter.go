package specs

import (
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

var MatchersMap = map[string]map[string]bool{
	FLAG_V1_0: {},
	FLAG_V1_1: {
		matchers.MatcherEqualToSemver:                  true,
		matchers.MatcherTypeLessThanOrEqualToSemver:    true,
		matchers.MatcherTypeGreaterThanOrEqualToSemver: true,
		matchers.MatcherTypeBetweenSemver:              true,
		matchers.MatcherTypeInListSemver:               true,
	},
	FLAG_V1_2: {matchers.MatcherInLargeSegment: true},
}

func ShouldFilter(matcher string, apiVersion string) bool {
	for mapVersion, matchers := range MatchersMap {
		// edge case when compare 1.2 < 1.10
		if apiVersion < mapVersion && matchers[matcher] {
			return true
		}
	}

	return false
}

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(version string) *string {
	_, exists := MatchersMap[version]
	if exists {
		return &version
	}

	return nil
}
