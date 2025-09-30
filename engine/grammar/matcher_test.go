package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
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

	ruleBuilder := NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil)

	matcher1, err := ruleBuilder.BuildMatcher(&dto1)

	if err != nil {
		t.Error("Matcher construction shouldn't fail")
	}

	if reflect.TypeOf(matcher1).String() != "*grammar.AllKeysMatcher" {
		t.Errorf(
			"Incorrect matcher created, expected: \"*grammar.AllKeysMatcher\", received: \"%s\"",
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

	matcher2, err := ruleBuilder.BuildMatcher(&dto2)

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

	ruleBuilder1 := NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil)

	matcher3, err := ruleBuilder1.BuildMatcher(&dto3)

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

	matcher4, err := ruleBuilder.BuildMatcher(dto4)
	if err != nil {
		t.Error("There shouldn't have been any errors constructing the matcher")
	}

	if matcher4.Negate() {
		t.Error("Matcher shouldn't be negated.")
	}
}
