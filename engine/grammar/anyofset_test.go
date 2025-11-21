package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestAnyOfSetMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "setdata"
	dto := &dtos.MatcherDTO{
		MatcherType: "CONTAINS_ANY_OF_SET",
		Whitelist: &dtos.WhitelistMatcherDataDTO{
			Whitelist: []string{"one", "two", "three", "four"},
		},
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	ruleBuilder := NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil)

	matcher, err := ruleBuilder.BuildMatcher(dto)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	matcherType := reflect.TypeOf(matcher).String()
	if matcherType != "*grammar.ContainsAnyOfSetMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.ContainsAnyOfSetMatcher and was %s", matcherType)
	}

	if !matcher.Match("asd", map[string]interface{}{"setdata": []string{"one", "two", "three", "four"}}, nil) {
		t.Error("Matcher should match an equal set")
	}

	if !matcher.Match("asd", map[string]interface{}{"setdata": []string{"one", "two", "three", "four", "five"}}, nil) {
		t.Error("Matcher should match a superset")
	}

	if matcher.Match("asd", map[string]interface{}{"setdata": []string{}}, nil) {
		t.Error("Matcher should NOT match an empty set")
	}

	if !matcher.Match("asd", map[string]interface{}{"setdata": []string{"one", "two", "three"}}, nil) {
		t.Error("Matcher should match a subset")
	}

	if matcher.Match("asd", map[string]interface{}{"setdata": []string{"five", "six"}}, nil) {
		t.Error("Matcher hsould not match a non-intersecting set")
	}

}
