package specs

import (
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

type SplitVersionFilter struct {
	data map[string]map[string]bool
}

func NewSplitVersionFilter() SplitVersionFilter {
	data := map[string]map[string]bool{
		FLAG_V1_1: {matchers.MatcherTypeInLargeSegment: true},
	}

	data[Default] = mergeMaps(map[string]bool{
		matchers.MatcherEqualToSemver:                  true,
		matchers.MatcherTypeLessThanOrEqualToSemver:    true,
		matchers.MatcherTypeGreaterThanOrEqualToSemver: true,
		matchers.MatcherTypeBetweenSemver:              true,
		matchers.MatcherTypeInListSemver:               true,
	}, data[FLAG_V1_1])

	return SplitVersionFilter{
		data: data,
	}
}

func (f *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	matchers, ok := f.data[apiVersion]
	if !ok {
		return false
	}

	return matchers[matcher]
}

func mergeMaps(versionMap map[string]bool, toMergeMap map[string]bool) map[string]bool {
	for key, value := range toMergeMap {
		versionMap[key] = value
	}

	return versionMap
}
