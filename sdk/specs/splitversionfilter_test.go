package specs

import (
	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers"
	"testing"
)

func Test_splitVersionFilter(t *testing.T) {
	filter := NewSplitVersionFilter()
	shouldFilter := filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, "1.0")
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeEqualTo, "1.0")
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, "1.1")
	if shouldFilter {
		t.Error("It should not filtered")
	}
}
