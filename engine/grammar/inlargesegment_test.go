package grammar

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage/inmemory/mutexmap"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

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

	ctx := injection.NewContext()
	ctx.AddDependency("largeSegmentStorage", segmentStorage)

	matcher, err := BuildMatcher(dto, ctx, logger)
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
