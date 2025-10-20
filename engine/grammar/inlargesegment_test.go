package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v7/storage/inmemory/mutexmap"
	"github.com/splitio/go-toolkit/v5/logging"
)

var SyncProxyFeatureFlagsRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString, constants.MatcherTypeInSplitTreatment,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}
var SyncProxyRuleBasedSegmentRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}

func TestInLargeSegmentMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	lsName := "large_segment_test"
	dto := &dtos.MatcherDTO{
		MatcherType: "IN_LARGE_SEGMENT",
		UserDefinedLargeSegment: &dtos.UserDefinedLargeSegmentMatcherDataDTO{
			LargeSegmentName: lsName,
		},
	}

	lsKeys := []string{"item1", "item2"}

	segmentStorage := mutexmap.NewLargeSegmentsStorage()
	segmentStorage.Update(lsName, lsKeys, 123)

	ruleBuilder := NewRuleBuilder(nil, nil, segmentStorage, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil)

	matcher, err := ruleBuilder.BuildMatcher(dto)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	matcherType := reflect.TypeOf(matcher).String()
	if matcherType != "*grammar.InLargeSegmentMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *grammar.InLargeSegmentMatcher and was %s", matcherType)
	}

	if !matcher.Match("item1", nil, nil) {
		t.Error("Should match a key present in the large segment")
	}

	if matcher.Match("item7", nil, nil) {
		t.Error("Should not match a key not present in the large segment")
	}

	segmentStorage.Update(lsName, []string{}, 123)
	if matcher.Match("item1", nil, nil) {
		t.Error("Should return false for a nonexistent large segment")
	}
}
