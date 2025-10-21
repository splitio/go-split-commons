package engine

import (
	"encoding/csv"
	"io"
	"math"
	"os"
	"testing"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine/grammar"
	"github.com/splitio/go-split-commons/v8/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v8/engine/hash"

	"github.com/splitio/go-toolkit/v5/hasher"
	"github.com/splitio/go-toolkit/v5/logging"
)

var syncProxyFeatureFlagsRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString, constants.MatcherTypeInSplitTreatment,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}
var syncProxyRuleBasedSegmentRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}

func TestProperHashFunctionIsUsed(t *testing.T) {
	eng := Engine{}

	murmurHash := hasher.Sum32WithSeed([]byte("SOME_TEST"), 12345)
	murmurBucket := int(math.Abs(float64(murmurHash%100)) + 1)
	if murmurBucket != eng.calculateBucket(2, "SOME_TEST", 12345) {
		t.Error("Incorrect hash!")
	}

	legacyHash := hash.Legacy([]byte("SOME_TEST"), 12345)
	legacyBucket := int(math.Abs(float64(legacyHash%100)) + 1)
	if legacyBucket != eng.calculateBucket(1, "SOME_TEST", 12345) {
		t.Error("Incorrect hash!")
	}
}

func TestProperHashFunctionIsUsedWithConstants(t *testing.T) {
	eng := Engine{}

	murmurHash := hasher.Sum32WithSeed([]byte("SOME_TEST"), 12345)
	murmurBucket := int(math.Abs(float64(murmurHash%100)) + 1)
	if murmurBucket != eng.calculateBucket(constants.SplitAlgoMurmur, "SOME_TEST", 12345) {
		t.Error("Incorrect hash!")
	}

	legacyHash := hash.Legacy([]byte("SOME_TEST"), 12345)
	legacyBucket := int(math.Abs(float64(legacyHash%100)) + 1)
	if legacyBucket != eng.calculateBucket(constants.SplitAlgoLegacy, "SOME_TEST", 12345) {
		t.Error("Incorrect hash!")
	}
}

func TestTreatmentOnTrafficAllocation1(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitDTO := dtos.SplitDTO{
		Algo:                  2,
		ChangeNumber:          123,
		DefaultTreatment:      "default",
		Killed:                false,
		Name:                  "split",
		Seed:                  1234,
		Status:                "ACTIVE",
		TrafficAllocation:     1,
		TrafficAllocationSeed: -1667452163,
		TrafficTypeName:       "tt1",
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: "ROLLOUT",
				Label:         "in segment all",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType:        "ALL_KEYS",
							Whitelist:          nil,
							Negate:             false,
							UserDefinedSegment: nil,
						},
					},
				},
				Partitions: []dtos.PartitionDTO{
					{
						Size:      100,
						Treatment: "on",
					},
				},
			},
		},
	}

	split := grammar.NewSplit(&splitDTO, logger, grammar.NewRuleBuilder(nil, nil, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil))

	eng := Engine{}
	eng.logger = logger
	treatment, _ := eng.DoEvaluation(split, "aaaaaaklmnbv", "aaaaaaklmnbv", nil)

	if *treatment == "default" {
		t.Error("It should not return default treatment.")
	}
}

func TestTreatmentOnTrafficAllocation99(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitDTO := dtos.SplitDTO{
		Algo:                  1,
		ChangeNumber:          123,
		DefaultTreatment:      "default",
		Killed:                false,
		Name:                  "split",
		Seed:                  1234,
		Status:                "ACTIVE",
		TrafficAllocation:     99,
		TrafficAllocationSeed: -1667452111935,
		TrafficTypeName:       "tt1",
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: "ROLLOUT",
				Label:         "in segment all",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType:        "ALL_KEYS",
							Whitelist:          nil,
							Negate:             false,
							UserDefinedSegment: nil,
						},
					},
				},
				Partitions: []dtos.PartitionDTO{
					{
						Size:      100,
						Treatment: "on",
					},
				},
			},
		},
	}

	split := grammar.NewSplit(&splitDTO, logger, grammar.NewRuleBuilder(nil, nil, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil))

	eng := Engine{}
	eng.logger = logger
	treatment, _ := eng.DoEvaluation(split, "aaaaaaklmnbv", "aaaaaaklmnbv", nil)

	if *treatment != "default" {
		t.Error("It should return default treatment.")
	}
}

type TreatmentResult struct {
	Key    string
	Result string
}

func parseCSV(file string) ([]TreatmentResult, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []TreatmentResult
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}

		results = append(results, TreatmentResult{

			Key:    row[0],
			Result: row[1],
		})
	}
}

func TestEvaluations(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitDTO := dtos.SplitDTO{
		Algo:                  2,
		ChangeNumber:          1550099287313,
		DefaultTreatment:      "on",
		Killed:                false,
		Name:                  "real_split",
		Seed:                  764645059,
		Status:                "ACTIVE",
		TrafficAllocation:     100,
		TrafficAllocationSeed: -1757484928,
		TrafficTypeName:       "user",
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: "ROLLOUT",
				Label:         "default rule",
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							KeySelector: &dtos.KeySelectorDTO{
								Attribute:   nil,
								TrafficType: "user",
							},
							MatcherType:        "ALL_KEYS",
							Negate:             false,
							UserDefinedSegment: nil,
							Whitelist:          nil,
							UnaryNumeric:       nil,
							Between:            nil,
							Boolean:            nil,
							String:             nil,
						},
					},
				},
				Partitions: []dtos.PartitionDTO{
					{
						Size:      50,
						Treatment: "on",
					},
					{
						Size:      50,
						Treatment: "off",
					},
				},
			},
		},
	}

	treatmentsResults, err := parseCSV("../testdata/expected-treatments.csv")
	if err != nil {
		t.Error(err)
	}

	if len(treatmentsResults) == 0 {
		t.Error("Data was not added for testing consistency")
	}

	split := grammar.NewSplit(&splitDTO, logger, grammar.NewRuleBuilder(nil, nil, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil))

	eng := Engine{}
	eng.logger = logger

	for _, tr := range treatmentsResults {
		treatment, _ := eng.DoEvaluation(split, tr.Key, tr.Key, nil)

		if *treatment != tr.Result {
			t.Error("Checking expected treatment " + tr.Result + " for key: " + tr.Key)
		}
	}
}

func TestNoConditionMatched(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitDTO := dtos.SplitDTO{
		Algo:                  2,
		ChangeNumber:          1550099287313,
		DefaultTreatment:      "on",
		Killed:                false,
		Name:                  "real_split",
		Seed:                  764645059,
		Status:                "ACTIVE",
		TrafficAllocation:     100,
		TrafficAllocationSeed: -1757484928,
		TrafficTypeName:       "user",
		Conditions:            []dtos.ConditionDTO{},
	}

	split := grammar.NewSplit(&splitDTO, logger, grammar.NewRuleBuilder(nil, nil, nil, syncProxyFeatureFlagsRules, syncProxyRuleBasedSegmentRules, logger, nil))

	eng := Engine{}
	eng.logger = logger

	treatment, err := eng.DoEvaluation(split, "aaaaaaklmnbv", "aaaaaaklmnbv", nil)

	if treatment != nil {
		t.Error("It should be nil.")
	}

	if err != "default rule" {
		t.Error("It should return err")
	}
}
