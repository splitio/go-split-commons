package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestStartsWith(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "value"
	dto := &dtos.MatcherDTO{
		MatcherType: "STARTS_WITH",
		Whitelist: &dtos.WhitelistMatcherDataDTO{
			Whitelist: []string{"abc", "def", "ghi"},
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
	if matcherType != "*grammar.StartsWithMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.StartsWithMatcher and was %s", matcherType)
	}

	if matcher.Match("asd", map[string]interface{}{"value": "zzz"}, nil) {
		t.Errorf("string without any of the prefixes shouldn't match")
	}

	if matcher.Match("asd", map[string]interface{}{"value": ""}, nil) {
		t.Errorf("empty string shouldn't match")
	}

	if !matcher.Match("asd", map[string]interface{}{"value": "abcpp"}, nil) {
		t.Errorf("string containing one of the prefixes should match")
	}

	if matcher.Match("asd", map[string]interface{}{"value": "hdhfabcdefghimklsad"}, nil) {
		t.Errorf("string containing some substrings but not as prefixes should not match")
	}
}
