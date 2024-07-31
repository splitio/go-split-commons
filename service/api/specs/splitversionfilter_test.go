package specs

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

func Test_splitVersionFilter(t *testing.T) {
	shouldFilter := ShouldFilter(matchers.MatcherTypeBetweenSemver, FLAG_V1_0)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherTypeEqualTo, FLAG_V1_0)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherTypeBetweenSemver, FLAG_V1_1)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherInLargeSegment, FLAG_V1_0)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherInLargeSegment, FLAG_V1_1)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherInLargeSegment, FLAG_V1_2)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = ShouldFilter(matchers.MatcherInLargeSegment, "4.3")
	if shouldFilter {
		t.Error("It should not filtered")
	}
}
