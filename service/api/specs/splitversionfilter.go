package specs

import (
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

type SplitVersionFilter struct {
	v1_0 map[string]bool
	v1_1 map[string]bool
}

func NewSplitVersionFilter() SplitVersionFilter {
	v1_1 := map[string]bool{matchers.MatcherTypeInLargeSegment: true}
	v1_0 := mergeMaps(map[string]bool{
		matchers.MatcherEqualToSemver:                  true,
		matchers.MatcherTypeLessThanOrEqualToSemver:    true,
		matchers.MatcherTypeGreaterThanOrEqualToSemver: true,
		matchers.MatcherTypeBetweenSemver:              true,
		matchers.MatcherTypeInListSemver:               true,
	}, v1_1)

	return SplitVersionFilter{
		v1_0: v1_0,
		v1_1: v1_1,
	}
}

func (f *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	switch apiVersion {
	case FLAG_V1_1:
		return f.v1_1[matcher]
	case FLAG_V1_0:
		return f.v1_0[matcher]
	}

	return false
}

func mergeMaps(versionMap map[string]bool, toMergeMap map[string]bool) map[string]bool {
	for key, value := range toMergeMap {
		versionMap[key] = value
	}

	return versionMap
}
