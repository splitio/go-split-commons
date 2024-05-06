package matchers

import (
	"encoding/csv"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

type semvers struct {
	semver1 string
	semver2 string
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
