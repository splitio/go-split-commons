package matchers

import (
	"encoding/csv"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

type semvers struct {
	semver1 string
	semver2 string
	semver3 string
}

func parseCSVTwoSemvers(file string) ([]semvers, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []semvers
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}
		results = append(results, semvers{
			semver1: row[0],
			semver2: row[1],
		})
	}
}

func parseCSVThreeSemvers(file string) ([]semvers, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []semvers
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}
		results = append(results, semvers{
			semver1: row[0],
			semver2: row[1],
			semver3: row[2],
		})
	}
}

func TestEqualToSemverMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.0.0"
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherEqualToSemver,
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
		MatcherType: MatcherEqualToSemver,
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
		MatcherType: MatcherEqualToSemver,
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

func TestPreReleaseShouldReturnFalseWhenSemverIsNil(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.2-SNAPSHOT"
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherEqualToSemver,
		String:      &str,
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "1.2.3"
	if matcher.Match("ass", attributes, nil) {
		t.Error("Equal should match")
	}
}

func TestPreReleaseShouldReturnFalseWhenVersionsDiffer(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	str := "1.2.3----RC-SNAPSHOT.12.9.1--.12.88"
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherEqualToSemver,
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
		MatcherType: MatcherEqualToSemver,
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
		MatcherType: MatcherEqualToSemver,
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
		MatcherType: MatcherEqualToSemver,
		String:      nil,
	}
	_, err := BuildMatcher(dto, nil, logger)
	if err == nil {
		t.Error("There should be errors when building the matcher")
	}
}

func TestGreaterThanOrEqualToSemverMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	semvers, err := parseCSVTwoSemvers("../../../testdata/valid_semantic_versions.csv")
	if err != nil {
		t.Error(err)
	}

	for _, twoSemvers := range semvers {
		dto := &dtos.MatcherDTO{
			MatcherType: MatcherTypeGreaterThanOrEqualToSemver,
			String:      &twoSemvers.semver1,
			KeySelector: &dtos.KeySelectorDTO{
				Attribute: &attrName,
			},
		}

		matcher, err := BuildMatcher(dto, nil, logger)
		if err != nil {
			t.Error("There should be no errors when building the matcher")
		}
		matcherType := reflect.TypeOf(matcher).String()
		if matcherType != "*matchers.GreaterThanOrEqualToSemverMatcher" {
			t.Errorf("Incorrect matcher constructed. Should be *matchers.GreaterThanOrEqualToSemverMatcher and was %s", matcherType)
		}

		attributes := make(map[string]interface{})
		attributes[attrName] = twoSemvers.semver2
		if matcher.Match("asd", attributes, nil) {
			t.Error(twoSemvers.semver1, " >= ", twoSemvers.semver2, " should match")
		}
	}
}

func TestGreaterThanOrEqualToSemverMatcherWithNilSemver(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	semvers := "1.2-SNAPSHOT"
	dto := &dtos.MatcherDTO{
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
		MatcherType: MatcherTypeGreaterThanOrEqualToSemver,
		String:      &semvers,
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should not be errors when building the matcher")
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.3.4"
	if matcher.Match("asd", attributes, nil) {
		t.Error("2.3.4 should not match")
	}
}

func TestLessThanOrEqualToSemverMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	semvers, err := parseCSVTwoSemvers("../../../testdata/valid_semantic_versions.csv")
	if err != nil {
		t.Error(err)
	}

	for _, twoSemvers := range semvers {
		dto := &dtos.MatcherDTO{
			MatcherType: MatcherTypeLessThanOrEqualToSemver,
			String:      &twoSemvers.semver2,
			KeySelector: &dtos.KeySelectorDTO{
				Attribute: &attrName,
			},
		}
		matcher, err := BuildMatcher(dto, nil, logger)
		if err != nil {
			t.Error("There should be no errors when building the matcher")
		}
		matcherType := reflect.TypeOf(matcher).String()

		if matcherType != "*matchers.LessThanOrEqualToSemverMatcher" {
			t.Errorf("Incorrect matcher constructed. Should be *matchers.LessThanOrEqualToSemverMatcher and was %s", matcherType)
		}

		attributes := make(map[string]interface{})
		attributes[attrName] = twoSemvers.semver1
		if matcher.Match("asd", attributes, nil) {
			t.Error(twoSemvers.semver2, " <= ", twoSemvers.semver1, " should match")
		}
	}
}

func TestLessThanOrEqualToSemverMatcherWithInvalidSemver(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherTypeLessThanOrEqualToSemver,
		String:      nil,
	}
	_, err := BuildMatcher(dto, nil, logger)
	if err == nil {
		t.Error("There should be errors when building the matcher")
	}
}

func TestLessThanOrEqualToSemverMatcherWithNilSemver(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	semvers := "1.2-SNAPSHOT"
	dto := &dtos.MatcherDTO{
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
		MatcherType: MatcherTypeLessThanOrEqualToSemver,
		String:      &semvers,
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should not be errors when building the matcher")
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.3.4"
	if matcher.Match("asd", attributes, nil) {
		t.Error("2.3.4 should not match")
	}
}

func TestBetweenSemverMatcher(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	attrName := "version"
	semvers, err := parseCSVThreeSemvers("../../../testdata/between_semver.csv")
	if err != nil {
		t.Error(err)
	}

	for _, threeSemvers := range semvers {
		dto := &dtos.MatcherDTO{
			MatcherType: MatcherTypeBetweenSemver,
			BetweenString: &dtos.BetweenStringMatcherDataDTO{
				Start: &threeSemvers.semver1,
				End:   &threeSemvers.semver3,
			},
			KeySelector: &dtos.KeySelectorDTO{
				Attribute: &attrName,
			},
		}
		matcher, err := BuildMatcher(dto, nil, logger)
		if err != nil {
			t.Error("There should be no errors when building the matcher")
		}
		matcherType := reflect.TypeOf(matcher).String()

		if matcherType != "*matchers.BetweenSemverMatcher" {
			t.Errorf("Incorrect matcher constructed. Should be *matchers.BetweenSemverMatcher and was %s", matcherType)
		}

		attributes := make(map[string]interface{})
		attributes[attrName] = threeSemvers.semver2
		if !matcher.Match("asd", attributes, nil) {
			t.Error(threeSemvers.semver2, " between ", threeSemvers.semver1, "and", threeSemvers.semver3, " should match")
		}
	}
}

func TestBetweenSemverWithNilSemvers(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherTypeBetweenSemver,
		BetweenString: &dtos.BetweenStringMatcherDataDTO{
			Start: nil,
			End:   nil,
		},
	}
	_, err := BuildMatcher(dto, nil, logger)
	if err == nil {
		t.Error("There should be errors when building the matcher")
	}
}

func TestBetweenSemverWithInvalidSemvers(t *testing.T) {
	attrName := "version"
	logger := logging.NewLogger(&logging.LoggerOptions{})
	start := "1.alpha.2"
	end := "3.4.5"
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherTypeBetweenSemver,
		BetweenString: &dtos.BetweenStringMatcherDataDTO{
			Start: &start,
			End:   &end,
		},
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should not be errors when building the matcher")
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2-rc.1.2"
	if matcher.Match("asd", attributes, nil) {
		t.Error("2.2.2-rc.1.2", " between should not match")
	}
}

func TestInListSemvers(t *testing.T) {
	semvers := make([]string, 0, 3)
	semvers = append(semvers, "1.0.0-rc.1")
	semvers = append(semvers, "2.2.2-rc.1.2")
	semvers = append(semvers, "1.1.2-prerelease+meta")
	attrName := "version"
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
		MatcherType: MatcherTypeInListSemver,
		Whitelist:   &dtos.WhitelistMatcherDataDTO{Whitelist: semvers},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
	}
	matcherType := reflect.TypeOf(matcher).String()

	if matcherType != "*matchers.InListSemverMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *matchers.InListSemverMatcher and was %s", matcherType)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2-rc.1.2"
	if !matcher.Match("asd", attributes, nil) {
		t.Error("2.2.2-rc.1.2", " in list ", semvers, " should match")
	}
}

func TestInListSemversNotMatch(t *testing.T) {
	semvers := make([]string, 0, 3)
	semvers = append(semvers, "1.0.0-rc.1")
	semvers = append(semvers, "2.2.2-rc.1.2")
	semvers = append(semvers, "1.1.2-prerelease+meta")
	attrName := "version"
	logger := logging.NewLogger(&logging.LoggerOptions{})
	dto := &dtos.MatcherDTO{
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
		MatcherType: MatcherTypeInListSemver,
		Whitelist:   &dtos.WhitelistMatcherDataDTO{Whitelist: semvers},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should be no errors when building the matcher")
	}
	matcherType := reflect.TypeOf(matcher).String()

	if matcherType != "*matchers.InListSemverMatcher" {
		t.Errorf("Incorrect matcher constructed. Should be *matchers.InListSemverMatcher and was %s", matcherType)
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2"
	if matcher.Match("asd", attributes, nil) {
		t.Error("2.2.2-rc.1.2", " in list ", semvers, " should not match")
	}
}

func TestInListInvalidSemvers(t *testing.T) {
	attrName := "version"
	logger := logging.NewLogger(&logging.LoggerOptions{})

	semvers := make([]string, 0, 3)
	semvers = append(semvers, "1.alpha.2")
	semvers = append(semvers, "alpha.beta.1")
	semvers = append(semvers, "1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788")
	dto := &dtos.MatcherDTO{
		MatcherType: MatcherTypeInListSemver,
		Whitelist:   &dtos.WhitelistMatcherDataDTO{Whitelist: semvers},
		KeySelector: &dtos.KeySelectorDTO{
			Attribute: &attrName,
		},
	}
	matcher, err := BuildMatcher(dto, nil, logger)
	if err != nil {
		t.Error("There should not be errors when building the matcher")
	}

	attributes := make(map[string]interface{})
	attributes[attrName] = "2.2.2"
	if matcher.Match("asd", attributes, nil) {
		t.Error("2.2.2", " in list ", semvers, " should not match")
	}
}
