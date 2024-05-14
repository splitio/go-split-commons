package specs

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

func Test_splitVersionFilter(t *testing.T) {
	filter := NewSplitVersionFilter()
	shouldFilter := filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, FLAG_V1_0)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeEqualTo, FLAG_V1_0)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, FLAG_V1_1)
	if shouldFilter {
		t.Error("It should not filtered")
	}
}
