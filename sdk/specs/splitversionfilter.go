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
		mkey{V0, matchers.MatcherEqualToSemver}:                  {},
		mkey{V0, matchers.MatcherTypeLessThanOrEqualToSemver}:    {},
		mkey{V0, matchers.MatcherTypeGreaterThanOrEqualToSemver}: {},
		mkey{V0, matchers.MatcherTypeBetweenSemver}:              {},
		mkey{V0, matchers.MatcherTypeInListSemver}:               {},
	}

	return SplitVersionFilter{excluded: matchersToExclude}
}

func (s *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	_, ok := s.excluded[mkey{apiVersion, matcher}]
	return ok
}
