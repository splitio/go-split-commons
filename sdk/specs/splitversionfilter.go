package specs

import "github.com/splitio/go-split-commons/v5/engine/grammar/matchers"

const (
	v1 = "1.0"
)

type SplitVersionFilter struct {
	excluded map[mkey]struct{}
}

type mkey struct {
	api     string
	matcher string
}

func NewSplitVersionFilter() SplitVersionFilter {
	matchersToExclude := map[mkey]struct{}{
		mkey{v1, matchers.MatcherEqualToSemver}:                  {},
		mkey{v1, matchers.MatcherTypeLessThanOrEqualToSemver}:    {},
		mkey{v1, matchers.MatcherTypeGreaterThanOrEqualToSemver}: {},
		mkey{v1, matchers.MatcherTypeBetweenSemver}:              {},
		mkey{v1, matchers.MatcherTypeInListSemver}:               {},
	}

	return SplitVersionFilter{excluded: matchersToExclude}
}

func (s *SplitVersionFilter) ShouldFilter(matcher string, apiVersion string) bool {
	_, ok := s.excluded[mkey{apiVersion, matcher}]
	return ok
}
