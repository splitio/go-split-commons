package specs

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
)

func TestParseAndValidate(t *testing.T) {
	res, err := ParseAndValidate("")
	if err != nil || res != Default {
		t.Error("It should be 1.1")
	}

	res, err = ParseAndValidate("1.1")
	if err != nil || res != FlagSpecs[1] {
		t.Error("It should be 1.1")
	}

	res, err = ParseAndValidate("1.2")
	if err != nil || res != FlagSpecs[2] {
		t.Error("It should be 1.2")
	}

	res, err = ParseAndValidate("2.3")
	if err == nil || res != "2.3" {
		t.Error("Should be a unsupported version")
	}
}

func TestSplitVersionFilter(t *testing.T) {
	filter := NewSplitVersionFilter()
	shouldFilter := filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, Default)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeEqualTo, Default)
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeBetweenSemver, FlagSpecs[1])
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeInLargeSegment, Default)
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeInLargeSegment, FlagSpecs[1])
	if !shouldFilter {
		t.Error("It should filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeInLargeSegment, FlagSpecs[2])
	if shouldFilter {
		t.Error("It should not filtered")
	}

	shouldFilter = filter.ShouldFilter(matchers.MatcherTypeInLargeSegment, "4.3")
	if shouldFilter {
		t.Error("It should not filtered")
	}
}
