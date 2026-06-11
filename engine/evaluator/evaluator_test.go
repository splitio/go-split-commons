package evaluator

import (
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/engine"
	"github.com/splitio/go-split-commons/v9/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v9/engine/grammar"
	"github.com/splitio/go-split-commons/v9/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v9/flagsets"
	"github.com/splitio/go-split-commons/v9/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v9/storage/mocks"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

var syncProxyFeatureFlagsRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString, constants.MatcherTypeInSplitTreatment,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}
var syncProxyRuleBasedSegmentRules = []string{constants.MatcherTypeAllKeys, constants.MatcherTypeInSegment, constants.MatcherTypeWhitelist, constants.MatcherTypeEqualTo, constants.MatcherTypeGreaterThanOrEqualTo, constants.MatcherTypeLessThanOrEqualTo, constants.MatcherTypeBetween,
	constants.MatcherTypeEqualToSet, constants.MatcherTypePartOfSet, constants.MatcherTypeContainsAllOfSet, constants.MatcherTypeContainsAnyOfSet, constants.MatcherTypeStartsWith, constants.MatcherTypeEndsWith, constants.MatcherTypeContainsString,
	constants.MatcherTypeEqualToBoolean, constants.MatcherTypeMatchesString, constants.MatcherEqualToSemver, constants.MatcherTypeGreaterThanOrEqualToSemver, constants.MatcherTypeLessThanOrEqualToSemver, constants.MatcherTypeBetweenSemver, constants.MatcherTypeInListSemver, constants.MatcherTypeInLargeSegment,
	constants.MatcherTypeInRuleBasedSegment}

type mockStorage struct{}

var mysplittest = &dtos.SplitDTO{
	Algo:                  2,
	ChangeNumber:          1494593336752,
	DefaultTreatment:      "off",
	Killed:                false,
	Name:                  "mysplittest",
	Seed:                  -1992295819,
	Status:                "ACTIVE",
	TrafficAllocation:     100,
	TrafficAllocationSeed: -285565213,
	TrafficTypeName:       "user",
	Configurations:        make(map[string]string),
	Conditions: []dtos.ConditionDTO{
		{
			ConditionType: "ROLLOUT",
			Label:         "default rule",
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{
					{
						KeySelector: &dtos.KeySelectorDTO{
							TrafficType: "user",
							Attribute:   nil,
						},
						MatcherType:        "ALL_KEYS",
						Whitelist:          nil,
						Negate:             false,
						UserDefinedSegment: nil,
					},
				},
			},
			Partitions: []dtos.PartitionDTO{
				{
					Size:      0,
					Treatment: "on",
				}, {
					Size:      100,
					Treatment: "off",
				},
			},
		},
	},
}

var mysplittest2 = &dtos.SplitDTO{
	Algo:                  2,
	ChangeNumber:          1494593336752,
	DefaultTreatment:      "off",
	Killed:                false,
	Name:                  "mysplittest2",
	Seed:                  -1992295819,
	Status:                "ACTIVE",
	TrafficAllocation:     100,
	TrafficAllocationSeed: -285565213,
	TrafficTypeName:       "user",
	Configurations:        map[string]string{"on": "{\"color\": \"blue\",\"size\": 13}"},
	Conditions: []dtos.ConditionDTO{
		{
			ConditionType: "ROLLOUT",
			Label:         "default rule",
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{
					{
						KeySelector: &dtos.KeySelectorDTO{
							TrafficType: "user",
							Attribute:   nil,
						},
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
				}, {
					Size:      0,
					Treatment: "off",
				},
			},
		},
	},
}

var mysplittest3 = &dtos.SplitDTO{
	Algo:                  2,
	ChangeNumber:          1494593336752,
	DefaultTreatment:      "killed",
	Killed:                true,
	Name:                  "mysplittest3",
	Seed:                  -1992295819,
	Status:                "ACTIVE",
	TrafficAllocation:     100,
	TrafficAllocationSeed: -285565213,
	TrafficTypeName:       "user",
	Configurations:        map[string]string{"on": "{\"color\": \"blue\",\"size\": 13}"},
	Conditions: []dtos.ConditionDTO{
		{
			ConditionType: "ROLLOUT",
			Label:         "default rule",
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{
					{
						KeySelector: &dtos.KeySelectorDTO{
							TrafficType: "user",
							Attribute:   nil,
						},
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
				}, {
					Size:      0,
					Treatment: "off",
				},
			},
		},
	},
}

var mysplittestWithPrerequisites = &dtos.SplitDTO{
	Algo:                  2,
	ChangeNumber:          1494593336752,
	DefaultTreatment:      "off",
	Killed:                false,
	Name:                  "mysplittestWithPrerequisites",
	Seed:                  -1992295819,
	Status:                "ACTIVE",
	TrafficAllocation:     100,
	TrafficAllocationSeed: -285565213,
	TrafficTypeName:       "user",
	Configurations:        map[string]string{"on": "{\"color\": \"blue\",\"size\": 13}"},
	Prerequisites: []dtos.Prerequisite{
		{
			FeatureFlagName: "prereq1",
			Treatments:      []string{"on"},
		},
		{
			FeatureFlagName: "prereq2",
			Treatments:      []string{"on", "partial"},
		},
	},
	Conditions: []dtos.ConditionDTO{
		{
			ConditionType: "ROLLOUT",
			Label:         "default rule",
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{
					{
						KeySelector: &dtos.KeySelectorDTO{
							TrafficType: "user",
							Attribute:   nil,
						},
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
				{
					Size:      0,
					Treatment: "off",
				},
			},
		},
	},
}

var mysplittest4 = &dtos.SplitDTO{
	Algo:                  2,
	ChangeNumber:          1494593336752,
	DefaultTreatment:      "killed",
	Killed:                true,
	Name:                  "mysplittest4",
	Seed:                  -1992295819,
	Status:                "ACTIVE",
	TrafficAllocation:     100,
	TrafficAllocationSeed: -285565213,
	TrafficTypeName:       "user",
	Configurations:        map[string]string{"on": "{\"color\": \"blue\",\"size\": 13}", "killed": "{\"color\": \"orange\",\"size\": 13}"},
	Conditions: []dtos.ConditionDTO{
		{
			ConditionType: "ROLLOUT",
			Label:         "default rule",
			MatcherGroup: dtos.MatcherGroupDTO{
				Combiner: "AND",
				Matchers: []dtos.MatcherDTO{
					{
						KeySelector: &dtos.KeySelectorDTO{
							TrafficType: "user",
							Attribute:   nil,
						},
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
				}, {
					Size:      0,
					Treatment: "off",
				},
			},
		},
	},
}

func (s *mockStorage) Split(
	feature string,
) *dtos.SplitDTO {
	switch feature {
	default:
	case "mysplittest":
		return mysplittest
	case "mysplittest2":
		return mysplittest2
	case "mysplittest3":
		return mysplittest3
	case "mysplittest4":
		return mysplittest4
	}
	return nil
}
func (s *mockStorage) FetchMany(
	feature []string,
) map[string]*dtos.SplitDTO {
	splits := make(map[string]*dtos.SplitDTO)
	splits["mysplittest"] = mysplittest
	splits["mysplittest2"] = mysplittest2
	splits["mysplittest3"] = mysplittest3
	splits["mysplittest4"] = mysplittest4
	splits["mysplittest5"] = nil
	return splits
}
func (s *mockStorage) All() []dtos.SplitDTO                        { return make([]dtos.SplitDTO, 0) }
func (s *mockStorage) SegmentNames() *set.ThreadUnsafeSet          { return nil }
func (s *mockStorage) LargeSegmentNames() *set.ThreadUnsafeSet     { return nil }
func (s *mockStorage) RuleBasedSegmentNames() *set.ThreadUnsafeSet { return nil }
func (s *mockStorage) SplitNames() []string                        { return make([]string, 0) }
func (s *mockStorage) TrafficTypeExists(trafficType string) bool   { return true }
func (s *mockStorage) ChangeNumber() (int64, error)                { return 0, nil }
func (s *mockStorage) GetNamesByFlagSets(sets []string) map[string][]string {
	return make(map[string][]string)
}
func (s *mockStorage) GetAllFlagSetNames() []string { return make([]string, 0) }

func TestSplitWithoutConfigurations(t *testing.T) {
	logger := logging.NewLogger(nil)

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	key := "test"
	result := evaluator.EvaluateFeature(key, &key, "mysplittest", nil)

	if result.Treatment != "off" {
		t.Error("Wrong treatment result")
	}

	if result.Config != nil {
		t.Error("Unexpected configs")
	}
}

func TestSplitWithtConfigurations(t *testing.T) {
	logger := logging.NewLogger(nil)

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	key := "test"
	result := evaluator.EvaluateFeature(key, &key, "mysplittest2", nil)

	if result.Treatment != "on" {
		t.Error("Wrong treatment result")
	}

	if result.Config == nil && *result.Config != "{\"color\": \"blue\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}
}

func TestSplitWithtConfigurationsButKilled(t *testing.T) {
	logger := logging.NewLogger(nil)

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	key := "test"
	result := evaluator.EvaluateFeature(key, &key, "mysplittest3", nil)

	if result.Treatment != "killed" {
		t.Error("Wrong treatment result")
	}

	if result.Config != nil {
		t.Error("Unexpected configs")
	}
}

func TestSplitWithConfigurationsButKilledWithConfigsOnDefault(t *testing.T) {
	logger := logging.NewLogger(nil)

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	key := "test"
	result := evaluator.EvaluateFeature(key, &key, "mysplittest4", nil)

	if result.Treatment != "killed" {
		t.Error("Wrong treatment result")
	}

	if result.Config == nil && *result.Config != "{\"color\": \"orange\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}
}

func TestMultipleEvaluations(t *testing.T) {
	logger := logging.NewLogger(nil)
	fallback := "fallback"
	on := "on"
	fallbackTreatmentConfig := dtos.FallbackTreatmentConfig{GlobalFallbackTreatment: &dtos.FallbackTreatment{
		Treatment: &fallback,
	},
		ByFlagFallbackTreatment: map[string]dtos.FallbackTreatment{
			"flag1": {
				Treatment: &on,
			},
		}}

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		dtos.NewFallbackTreatmentCalculatorImp(&fallbackTreatmentConfig))

	key := "test"
	splits := []string{"mysplittest", "mysplittest2", "mysplittest3", "mysplittest4", "mysplittest5"}
	result := evaluator.EvaluateFeatures(key, &key, splits, nil)

	if result.Evaluations["mysplittest"].Treatment != "off" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest2"].Treatment != "on" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest2"].Config == nil && *result.Evaluations["mysplittest2"].Config != "{\"color\": \"blue\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest3"].Treatment != "killed" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest3"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest4"].Treatment != "killed" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest4"].Config == nil && *result.Evaluations["mysplittest4"].Config != "{\"color\": \"orange\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest5"].Treatment != "fallback" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest5"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.EvaluationTime <= 0 {
		t.Error("It should be greater than 0")
	}
}

func TestNoConditionMatched(t *testing.T) {
	logger := logging.NewLogger(nil)
	splitStorage := mutexmap.NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splitStorage.Update([]dtos.SplitDTO{{Name: "some", ChangeNumber: 123456789, DefaultTreatment: "off"}}, []dtos.SplitDTO{}, 123456789)

	evaluator := NewEvaluator(
		splitStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	key := "test"
	result := evaluator.EvaluateFeature(key, &key, "some", nil)
	if result == nil {
		t.Error("It shouldn't be nil")
	}
	if result != nil && result.Label != "default rule" {
		t.Error("It should return default rule")
	}
	if result != nil && result.Treatment != "off" {
		t.Error("It should return off")
	}
}

func TestEvaluationByFlagSets(t *testing.T) {
	logger := logging.NewLogger(nil)
	key := "test"

	mockedStorage := mocks.MockSplitStorage{
		GetNamesByFlagSetsCall: func(sets []string) map[string][]string {
			if len(sets) != 3 {
				t.Error("sets size should be 3")
			}
			flagsBySets := make(map[string][]string)
			flagsBySets["set1"] = []string{"mysplittest", "mysplittest2"}
			flagsBySets["set2"] = []string{"mysplittest2"}
			flagsBySets["set3"] = []string{"mysplittest3", "mysplittest4", "mysplittest5"}
			return flagsBySets
		},
		FetchManyCall: func(splitNames []string) map[string]*dtos.SplitDTO {
			splits := make(map[string]*dtos.SplitDTO)
			splits["mysplittest"] = mysplittest
			splits["mysplittest2"] = mysplittest2
			splits["mysplittest3"] = mysplittest3
			splits["mysplittest4"] = mysplittest4
			splits["mysplittest5"] = nil
			return splits
		},
	}

	fallback := "fallback"
	on := "on"
	fallbackTreatmentConfig := dtos.FallbackTreatmentConfig{GlobalFallbackTreatment: &dtos.FallbackTreatment{
		Treatment: &fallback,
	},
		ByFlagFallbackTreatment: map[string]dtos.FallbackTreatment{
			"mysplittest5": {
				Treatment: &on,
			},
		}}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		dtos.NewFallbackTreatmentCalculatorImp(&fallbackTreatmentConfig))
	result := evaluator.EvaluateFeatureByFlagSets(key, &key, []string{"set1", "set2", "set3"}, nil)

	if result.Evaluations["mysplittest"].Treatment != "off" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest2"].Treatment != "on" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest2"].Config == nil && *result.Evaluations["mysplittest2"].Config != "{\"color\": \"blue\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest3"].Treatment != "killed" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest3"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest4"].Treatment != "killed" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest4"].Config == nil && *result.Evaluations["mysplittest4"].Config != "{\"color\": \"orange\",\"size\": 13}" {
		t.Error("Unexpected configs")
	}

	if result.Evaluations["mysplittest5"].Treatment != "on" {
		t.Error("Wrong treatment result")
	}
	if result.Evaluations["mysplittest5"].Config != nil {
		t.Error("Unexpected configs")
	}

	if result.EvaluationTime <= 0 {
		t.Error("It should be greater than 0")
	}
}

// mockDependencyEvaluator implements a mock dependency evaluator for testing
type mockDependencyEvaluator struct {
	EvaluateDependencyCall func(key string, bucketingKey *string, featureFlag string, attributes map[string]interface{}) string
}

func (m *mockDependencyEvaluator) EvaluateDependency(key string, bucketingKey *string, featureFlag string, attributes map[string]interface{}) string {
	return m.EvaluateDependencyCall(key, bucketingKey, featureFlag, attributes)
}

// mockEngine implements a mock version of the engine for testing
type mockEngine struct {
	*engine.Engine
	doEvaluationFunc func(split *grammar.Split, key string, bucketingKey string, attributes map[string]interface{}) (*string, string)
}

func newMockEngine(evalFunc func(split *grammar.Split, key string, bucketingKey string, attributes map[string]interface{}) (*string, string)) *engine.Engine {
	mock := &mockEngine{
		Engine:           engine.NewEngine(logging.NewLogger(nil)),
		doEvaluationFunc: evalFunc,
	}
	return mock.Engine
}

func (m *mockEngine) DoEvaluation(split *grammar.Split, key string, bucketingKey string, attributes map[string]interface{}) (*string, string) {
	return m.doEvaluationFunc(split, key, bucketingKey, attributes)
}

func TestPrerequisitesMatching(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		bucketingKey   string
		attributes     map[string]interface{}
		prereqMatches  bool
		killed         bool
		expectedResult Result
	}{
		{
			name:          "prerequisites not met",
			key:           "test_key",
			bucketingKey:  "test_key",
			attributes:    nil,
			prereqMatches: false,
			killed:        false,
			expectedResult: Result{
				Treatment: "off",
				Label:     impressionlabels.PrerequisitesNotMet,
			},
		},
		{
			name:          "prerequisites met",
			key:           "test_key",
			bucketingKey:  "test_key",
			attributes:    nil,
			prereqMatches: true,
			killed:        false,
			expectedResult: Result{
				Treatment: "on",
				Label:     "default rule",
			},
		},
		{
			name:          "killed split returns default treatment",
			key:           "test_key",
			bucketingKey:  "test_key",
			attributes:    nil,
			prereqMatches: true, // prerequisites should not be checked for killed splits
			killed:        true,
			expectedResult: Result{
				Treatment: "off",
				Label:     impressionlabels.Killed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock engine that returns our test treatments
			mockEngine := newMockEngine(func(split *grammar.Split, key string, bucketingKey string, attributes map[string]interface{}) (*string, string) {
				treatment := "on"
				return &treatment, "default rule"
			})

			// Setup evaluator with mock dependencies
			evaluator := NewEvaluator(
				&mocks.MockSplitStorage{},
				&mocks.MockSegmentStorage{},
				&mocks.MockRuleBasedSegmentStorage{},
				&mocks.MockLargeSegmentStorage{},
				mockEngine,
				logging.NewLogger(nil),
				syncProxyFeatureFlagsRules,
				syncProxyRuleBasedSegmentRules,
				nil,
			)

			// Create split DTO with prerequisites
			splitDTO := &dtos.SplitDTO{
				Name:                  "test_split",
				DefaultTreatment:      "off",
				Killed:                tt.killed,
				Seed:                  12345,
				Status:                "ACTIVE",
				TrafficAllocation:     100,
				TrafficAllocationSeed: -285565213,
				Algo:                  2,
				Prerequisites: []dtos.Prerequisite{
					{
						FeatureFlagName: "prereq1",
						Treatments:      []string{"on"},
					},
				},
				Conditions: []dtos.ConditionDTO{
					{
						ConditionType: "ROLLOUT",
						Label:         "default rule",
						MatcherGroup: dtos.MatcherGroupDTO{
							Combiner: "AND",
							Matchers: []dtos.MatcherDTO{
								{
									KeySelector: &dtos.KeySelectorDTO{
										TrafficType: "user",
									},
									MatcherType: "ALL_KEYS",
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

			// Create mock dependency evaluator for prerequisites
			mockDependencyEvaluator := &mockDependencyEvaluator{
				EvaluateDependencyCall: func(key string, bucketingKey *string, featureFlag string, attributes map[string]interface{}) string {
					if tt.prereqMatches {
						return "on"
					}
					return "off"
				},
			}

			// Create rule builder with mock dependency evaluator
			evaluator.ruleBuilder = grammar.NewRuleBuilder(
				&mocks.MockSegmentStorage{},
				&mocks.MockRuleBasedSegmentStorage{},
				&mocks.MockLargeSegmentStorage{},
				syncProxyFeatureFlagsRules,
				syncProxyRuleBasedSegmentRules,
				logging.NewLogger(nil),
				mockDependencyEvaluator,
			)

			result := evaluator.evaluateTreatment(tt.key, tt.bucketingKey, "test_split", splitDTO, tt.attributes)

			// Verify only the treatment and label
			assert.Equal(t, tt.expectedResult.Treatment, result.Treatment, "Treatment mismatch for test: %s", tt.name)
			assert.Equal(t, tt.expectedResult.Label, result.Label, "Label mismatch for test: %s", tt.name)
		})
	}
}
func TestEvaluationByFlagSetsASetEmpty(t *testing.T) {
	logger := logging.NewLogger(nil)
	key := "test"

	mockedStorage := mocks.MockSplitStorage{
		GetNamesByFlagSetsCall: func(sets []string) map[string][]string {
			if len(sets) != 1 {
				t.Error("sets size should be 1")
			}
			flagsBySets := make(map[string][]string)
			flagsBySets["set2"] = []string{}
			return flagsBySets
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)
	result := evaluator.EvaluateFeatureByFlagSets(key, &key, []string{"set2"}, nil)

	if len(result.Evaluations) != 0 {
		t.Error("evaluations size should be 0")
	}
}

func TestEvaluateDefaultDefinitionNotFound(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Setup fallback treatment calculator
	fallbackTreatment := "control"
	fallbackTreatmentConfig := dtos.FallbackTreatmentConfig{
		GlobalFallbackTreatment: &dtos.FallbackTreatment{
			Treatment: &fallbackTreatment,
		},
	}

	// Mock storage that returns nil for unknown definition
	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		dtos.NewFallbackTreatmentCalculatorImp(&fallbackTreatmentConfig))

	result := evaluator.EvaluateDefault("nonexistent_definition")

	// Verify fallback treatment is returned
	assert.Equal(t, "control", result.Treatment, "Should return fallback treatment")
	assert.Equal(t, "fallback - "+impressionlabels.SplitNotFound, result.Label, "Should return prefixed SplitNotFound label")
	assert.Nil(t, result.Config, "Config should be nil for definition not found")
}

func TestEvaluateDefaultDefinitionExistsWithConfig(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create a definition with config for default treatment
	definitionWithConfig := &dtos.SplitDTO{
		Name:             "definition_with_config",
		DefaultTreatment: "on",
		Configurations: map[string]string{
			"on":  "{\"color\": \"red\", \"size\": 15}",
			"off": "{\"color\": \"blue\", \"size\": 10}",
		},
		Status: "ACTIVE",
	}

	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			if splitName == "definition_with_config" {
				return definitionWithConfig
			}
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	result := evaluator.EvaluateDefault("definition_with_config")

	// Verify default treatment and config are returned
	assert.Equal(t, "on", result.Treatment, "Should return default treatment")
	assert.Equal(t, impressionlabels.NoConditionMatched, result.Label, "Should return NoConditionMatched label")
	assert.NotNil(t, result.Config, "Config should not be nil")
	assert.Equal(t, "{\"color\": \"red\", \"size\": 15}", *result.Config, "Should return config for default treatment")
}

func TestEvaluateDefaultDefinitionExistsWithoutConfig(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create a definition without any configurations
	definitionWithoutConfig := &dtos.SplitDTO{
		Name:             "definition_without_config",
		DefaultTreatment: "off",
		Configurations:   nil,
		Status:           "ACTIVE",
	}

	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			if splitName == "definition_without_config" {
				return definitionWithoutConfig
			}
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	result := evaluator.EvaluateDefault("definition_without_config")

	// Verify default treatment is returned without config
	assert.Equal(t, "off", result.Treatment, "Should return default treatment")
	assert.Equal(t, impressionlabels.NoConditionMatched, result.Label, "Should return NoConditionMatched label")
	assert.Nil(t, result.Config, "Config should be nil when Configurations map is nil")
}

func TestEvaluateDefaultDefinitionExistsConfigMapMissingDefaultTreatmentKey(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create a definition with configs but missing the default treatment key
	definitionMissingKey := &dtos.SplitDTO{
		Name:             "definition_missing_key",
		DefaultTreatment: "default",
		Configurations: map[string]string{
			"on":  "{\"color\": \"green\", \"size\": 20}",
			"off": "{\"color\": \"yellow\", \"size\": 5}",
		},
		Status: "ACTIVE",
	}

	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			if splitName == "definition_missing_key" {
				return definitionMissingKey
			}
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	result := evaluator.EvaluateDefault("definition_missing_key")

	// Verify default treatment is returned without config
	assert.Equal(t, "default", result.Treatment, "Should return default treatment")
	assert.Equal(t, impressionlabels.NoConditionMatched, result.Label, "Should return NoConditionMatched label")
	assert.Nil(t, result.Config, "Config should be nil when default treatment key is not in Configurations map")
}

func TestEvaluateDefaultDefinitionExistsEmptyConfigMap(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create a definition with empty configurations map
	definitionEmptyMap := &dtos.SplitDTO{
		Name:             "definition_empty_map",
		DefaultTreatment: "control",
		Configurations:   map[string]string{},
		Status:           "ACTIVE",
	}

	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			if splitName == "definition_empty_map" {
				return definitionEmptyMap
			}
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		nil)

	result := evaluator.EvaluateDefault("definition_empty_map")

	// Verify default treatment is returned without config
	assert.Equal(t, "control", result.Treatment, "Should return default treatment")
	assert.Equal(t, impressionlabels.NoConditionMatched, result.Label, "Should return NoConditionMatched label")
	assert.Nil(t, result.Config, "Config should be nil when Configurations map is empty")
}

func TestEvaluateDefaultWithPerFlagFallback(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Setup fallback treatment calculator with per-flag fallback
	globalFallback := "control"
	perFlagFallback := "special_fallback"
	fallbackTreatmentConfig := dtos.FallbackTreatmentConfig{
		GlobalFallbackTreatment: &dtos.FallbackTreatment{
			Treatment: &globalFallback,
		},
		ByFlagFallbackTreatment: map[string]dtos.FallbackTreatment{
			"special_definition": {
				Treatment: &perFlagFallback,
			},
		},
	}

	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		dtos.NewFallbackTreatmentCalculatorImp(&fallbackTreatmentConfig))

	result := evaluator.EvaluateDefault("special_definition")

	// Verify per-flag fallback is used
	assert.Equal(t, "special_fallback", result.Treatment, "Should return per-flag fallback treatment")
	assert.Equal(t, "fallback - "+impressionlabels.SplitNotFound, result.Label, "Should return prefixed SplitNotFound label")
	assert.Nil(t, result.Config, "Config should be nil for definition not found")
}

// TestEvaluateFeatureWithUnsupportedMatcherPreservesChangeNumber tests that when a split
// has an unsupported matcher and returns Control treatment with fallback, the SplitChangeNumber
// from the original split is preserved in the evaluation result for proper impression tracking
func TestEvaluateFeatureWithUnsupportedMatcherPreservesChangeNumber(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create a split with an unsupported matcher that will return Control
	// This simulates what happens when validator replaces unsupported matchers
	splitWithUnsupportedMatcher := &dtos.SplitDTO{
		Name:             "split_with_unsupported_matcher",
		ChangeNumber:     123456789, // This should be preserved in the result
		DefaultTreatment: "off",
		Status:           "ACTIVE",
		Killed:           false,
		TrafficAllocation: 100,
		Conditions: []dtos.ConditionDTO{
			{
				ConditionType: "WHITELIST",
				Label:         impressionlabels.UnsupportedMatcherType,
				MatcherGroup: dtos.MatcherGroupDTO{
					Combiner: "AND",
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: constants.MatcherTypeAllKeys,
							Negate:      false,
						},
					},
				},
				Partitions: []dtos.PartitionDTO{
					{
						Treatment: Control, // This will trigger the fallback path
						Size:      100,
					},
				},
			},
		},
	}

	// Setup fallback treatment calculator with a global fallback
	fallbackTreatment := "fallback_treatment"
	fallbackConfig := "{\"fallback\": true}"
	fallbackTreatmentConfig := dtos.FallbackTreatmentConfig{
		GlobalFallbackTreatment: &dtos.FallbackTreatment{
			Treatment: &fallbackTreatment,
			Config:    &fallbackConfig,
		},
	}

	// Mock storage that returns our split
	mockedStorage := mocks.MockSplitStorage{
		SplitCall: func(splitName string) *dtos.SplitDTO {
			if splitName == "split_with_unsupported_matcher" {
				return splitWithUnsupportedMatcher
			}
			return nil
		},
	}

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules,
		dtos.NewFallbackTreatmentCalculatorImp(&fallbackTreatmentConfig))

	// Evaluate with a key and bucketing key
	bucketingKey := "bucketing_key"
	result := evaluator.EvaluateFeature("test_key", &bucketingKey, "split_with_unsupported_matcher", nil)

	// Verify the fallback treatment is returned
	assert.Equal(t, "fallback_treatment", result.Treatment, "Should return fallback treatment")
	assert.Equal(t, "fallback - "+impressionlabels.UnsupportedMatcherType, result.Label, "Should return prefixed UnsupportedMatcherType label")
	assert.NotNil(t, result.Config, "Config should be set from fallback")

	// CRITICAL: Verify that SplitChangeNumber is preserved from the original split
	// This is essential for proper impression tracking - even when using fallback,
	// the impression should record the actual split's change number, not 0
	assert.Equal(t, int64(123456789), result.SplitChangeNumber, "SplitChangeNumber should be preserved from the original split, not default to 0")
}

