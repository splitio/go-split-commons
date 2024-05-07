package matchers

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestEqualToSemverMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.0.0"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "1.0.0"
	if !matcher.Match("asd", attributes, nil) {
		t.Error("Equal should match")
	}
}

func TestPatchDiffers(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.0.0"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "1.0.0"
	if matcher.Match("sded", map[string]interface{}{}, nil) {
		t.Error("Equal should not match")
	}
}

func TestPreReleaseShouldReturnTrueWhenVersionsAreEqual(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.2.3----RC-SNAPSHOT.12.9.1--.12.88"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "1.2.3----RC-SNAPSHOT.12.9.1--.12.88"
	if !matcher.Match("ass", attributes, nil) {
		t.Error("Equal should match")
	}
}

func TestPreReleaseShouldReturnFalseWhenVersionsDiffer(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.2.3----RC-SNAPSHOT.12.9.1--.12.88"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "1.2.3----RC-SNAPSHOT.12.9.1--.12.99"
	if matcher.Match("asd", attributes, nil) {
		t.Error("Equal should not match")
	}
}

func TestMetadataShouldReturnTrueWhenVersionsAreEqual(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "2.2.2-rc.2+metadata-lalala"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2-rc.2+metadata-lalala"
	if !matcher.Match("asd", attributes, nil) {
		t.Error("Equal should match")
	}
}

func TestMetadataShouldReturnFalseWhenVersionsDiffer(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "2.2.2-rc.2+metadata-lalala"
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
		t.Error(err)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2-rc.2+metadata"
	if matcher.Match("asd", attributes, nil) {
		t.Error("Equal should not match")
	}
}

func TestShouldReturnErrorWithNilSemver(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		MatcherType: "EQUAL_TO_SEMVER",
		String:      nil,
	}
	_, err := BuildMatcher(dto, nil, logger)
	if err == nil {
		t.Error("There should be errors when building the matcher")
	}
}
