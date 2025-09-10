package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestAllKeysMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		MatcherType: "ALL_KEYS",
	}
	ruleBuilder := NewRuleBuilder(nil, nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger)

	matcher, err := ruleBuilder.BuildMatcher(dto)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
	}

	matcherType := reflect.TypeOf(matcher).String()
	if matcherType != "*grammar.AllKeysMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.AllKeysMatcher and was %s", matcherType)
	}

	if !matcher.Match("asd", nil, nil) {
		t.Error("Matcher should match ANY string")
	}
}
