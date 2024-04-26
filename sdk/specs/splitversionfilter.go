package specs

import "github.com/splitio/go-split-commons/v5/engine/grammar/matchers"

type SplitVersionFilter struct {
	excluded map[mkey]struct{}
}

type mkey struct {
	api     string
	matcher string
}

func NewSplitVersionFilter() SplitVersionFilter {
	matchersToExclude := map[mkey]struct{}{
		mkey{FLAG_V1_0, matchers.MatcherEqualToSemver}:                  {},
		mkey{FLAG_V1_0, matchers.MatcherTypeLessThanOrEqualToSemver}:    {},
		mkey{FLAG_V1_0, matchers.MatcherTypeGreaterThanOrEqualToSemver}: {},
		mkey{FLAG_V1_0, matchers.MatcherTypeBetweenSemver}:              {},
		mkey{FLAG_V1_0, matchers.MatcherTypeInListSemver}:               {},
	}

	return SplitVersionFilter{excluded: matchersToExclude}
}

func (s *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	_, ok := s.excluded[mkey{apiVersion, matcher}]
	return ok
}
