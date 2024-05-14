package specs

import "github.com/splitio/go-split-commons/v6/engine/grammar/matchers"

type SplitVersionFilter struct {
	excluded map[mkey]struct{}
}

type mkey struct {
	api     string
	matcher string
}

func NewSplitVersionFilter() SplitVersionFilter {
	matchersToExclude := map[mkey]struct{}{
		{FLAG_V1_0, matchers.MatcherEqualToSemver}:                  {},
		{FLAG_V1_0, matchers.MatcherTypeLessThanOrEqualToSemver}:    {},
		{FLAG_V1_0, matchers.MatcherTypeGreaterThanOrEqualToSemver}: {},
		{FLAG_V1_0, matchers.MatcherTypeBetweenSemver}:              {},
		{FLAG_V1_0, matchers.MatcherTypeInListSemver}:               {},
	}

	return SplitVersionFilter{excluded: matchersToExclude}
}

func (s *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	_, ok := s.excluded[mkey{apiVersion, matcher}]
	return ok
}
