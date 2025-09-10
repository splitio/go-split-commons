package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestWhitelistMatcherAttr(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "value"
	dto := &dtos.MatcherDTO{
		MatcherType: "WHITELIST",
		Whitelist: &dtos.WhitelistMatcherDataDTO{
			Whitelist: []string{"aaa", "bbb"},
		},
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}

	ruleBuilder := NewRuleBuilder(nil, nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger)

	matcher, err := ruleBuilder.BuildMatcher(dto)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	matcherType := reflect.TypeOf(matcher).String()
	if matcherType != "*grammar.WhitelistMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.WhitelistMatcher and was %s", matcherType)
	}

	if !matcher.Match("asd", map[string]interface{}{"value": "aaa"}, nil) {
		t.Error("Item in whitelist should match")
	}

	if matcher.Match("asd", map[string]interface{}{"value": "ccc"}, nil) {
		t.Error("Item NOT in whitelist should NOT match")
	}
}

func TestWhitelistMatcherKey(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		MatcherType: "WHITELIST",
		Whitelist: &dtos.WhitelistMatcherDataDTO{
			Whitelist: []string{"aaa", "bbb"},
		},
	}

	ruleBuilder := NewRuleBuilder(nil, nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger)

	matcher, err := ruleBuilder.BuildMatcher(dto)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	matcherType := reflect.TypeOf(matcher).String()
	if matcherType != "*grammar.WhitelistMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.WhitelistMatcher and was %s", matcherType)
	}

	if !matcher.Match("aaa", map[string]interface{}{"value": 1}, nil) {
		t.Error("Item in whitelist should match")
	}

	if matcher.Match("asd", map[string]interface{}{"value": 2}, nil) {
		t.Error("Item NOT in whitelist should NOT match")
	}
}
