package specs

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/engine/grammar"
)

func TestParseAndValidate(t *testing.T) {
	res, err := ParseAndValidate("")
	if err != nil || res != FLAG_V1_0 {
		t.Error("It should be 1.1")
	}

	res, err = ParseAndValidate("1.1")
	if err != nil || res != FLAG_V1_1 {
		t.Error("It should be 1.1")
	}

	res, err = ParseAndValidate("1.2")
	if err != nil || res != FLAG_V1_2 {
		t.Error("It should be 1.2")
	}

	res, err = ParseAndValidate("2.3")
	if err == nil || res != "2.3" {
		t.Error("Should be a unsupported version")
	}
}

func TestsplitVersionFilter(t *testing.T) {
	filter := NewSplitVersionFilter()
	shouldFilter := filter.ShouldFilter(grammar.MatcherTypeBetweenSemver, FLAG_V1_0)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeEqualTo, FLAG_V1_0)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeBetweenSemver, FLAG_V1_1)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeInLargeSegment, FLAG_V1_0)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeInLargeSegment, FLAG_V1_1)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeInLargeSegment, FLAG_V1_2)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(grammar.MatcherTypeInLargeSegment, "4.3")
	if shouldFilter {
		t.Error("It should not filtered")
	}
}
