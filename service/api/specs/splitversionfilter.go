package specs

import (
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

var V1_1 = map[string]bool{matchers.MatcherInLargeSegment: true}
var V1_0 = mergeMaps(map[string]bool{
	matchers.MatcherEqualToSemver:                  true,
	matchers.MatcherTypeLessThanOrEqualToSemver:    true,
	matchers.MatcherTypeGreaterThanOrEqualToSemver: true,
	matchers.MatcherTypeBetweenSemver:              true,
	matchers.MatcherTypeInListSemver:               true,
}, V1_1)

func ShouldFilter(matcher string, apiVersion string) bool {
	switch apiVersion {
	case FLAG_V1_1:
		return V1_1[matcher]
	case FLAG_V1_0:
		return V1_0[matcher]
	}

	return false
}

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(version string) *string {
	switch version {
	case FLAG_V1_0:
		return &version
	case FLAG_V1_1:
		return &version
	case FLAG_V1_2:
		return &version
	}
	return nil
}

func ParseFlagSpec(spec string) string {
	if len(spec) == 0 {
		// set default flag spec
		return FLAG_V1_0
	}

	return spec
}

func mergeMaps(versionMap map[string]bool, toMergeMap map[string]bool) map[string]bool {
	for key, value := range toMergeMap {
		versionMap[key] = value
	}

	return versionMap
}
