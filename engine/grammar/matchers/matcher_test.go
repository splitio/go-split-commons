package matchers

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestMatcherConstruction(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto1 := dtos.MatcherDTO{
		Negate:      false,
		MatcherType: "ALL_KEYS",
		KeySelector: &dtos.KeySelectorDTO{
			Attribute:   nil,
			TrafficType: "something",
		},
	}

	matcher1, err := BuildMatcher(&dto1, logger)

	if err != nil {
		t.Error("Matcher construction shouldn't fail")
	}

	if reflect.TypeOf(matcher1).String() != "*matchers.AllKeysMatcher" {
		t.Errorf(
			"Incorrect matcher created, expected: \"*matchers.AllKeysMatcher\", received: \"%s\"",
			reflect.TypeOf(matcher1).String(),
		)
	}

	if matcher1.Negate() {
		t.Error("Matcher shouldn't be negated.")
	}

	if matcher1.base() != matcher1.(*AllKeysMatcher).base() {
		t.Error("base() should point to embedded base matcher struct")
	}

	dto2 := dtos.MatcherDTO{
		Negate:      true,
		MatcherType: "INVALID_MATCHER",
		KeySelector: &dtos.KeySelectorDTO{
			Attribute:   nil,
			TrafficType: "something",
		},
	}

	matcher2, err := BuildMatcher(&dto2, logger)

	if err == nil {
		t.Error("Matcher construction shoul have failed for invalid matcher")
	}

	if matcher2 != nil {
		t.Error("Builder should have returned nil as a result for an invalid matcher")
	}

	dto3 := dtos.MatcherDTO{
		Negate:      true,
		MatcherType: "ALL_KEYS",
		KeySelector: &dtos.KeySelectorDTO{
			Attribute:   nil,
			TrafficType: "something",
		},
	}
	matcher3, err := BuildMatcher(&dto3, logger)

	if err != nil {
		t.Error("There shouldn't have been any errors constructing the matcher")
	}

	if !matcher3.Negate() {
		t.Error("Matcher should be negated")
	}

	attrName := "value"
	dto4 := &dtos.MatcherDTO{
		MatcherType: "IN_SPLIT_TREATMENT",
		Dependency: &dtos.DependencyMatcherDataDTO{
			Split:      "feature1",
			Treatments: []string{"on"},
		},
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher4, err := BuildMatcher(dto4, logger)
	if err != nil {
		t.Error("There shouldn't have been any errors constructing the matcher")
	}

	if matcher4.Negate() {
		t.Error("Matcher shouldn't be negated.")
	}
}
