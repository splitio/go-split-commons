package evaluator

import (
	"testing"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine"
	"github.com/splitio/go-split-commons/v8/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v8/engine/grammar"
	"github.com/splitio/go-split-commons/v8/flagsets"
	"github.com/splitio/go-split-commons/v8/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v8/storage/mocks"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

var syncProxyFeatureFlagsRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString, grammar.MatcherTypeInSplitTreatment,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver, grammar.MatcherTypeInLargeSegment,
	grammar.MatcherTypeInRuleBasedSegment}
var syncProxyRuleBasedSegmentRules = []string{grammar.MatcherTypeAllKeys, grammar.MatcherTypeInSegment, grammar.MatcherTypeWhitelist, grammar.MatcherTypeEqualTo, grammar.MatcherTypeGreaterThanOrEqualTo, grammar.MatcherTypeLessThanOrEqualTo, grammar.MatcherTypeBetween,
	grammar.MatcherTypeEqualToSet, grammar.MatcherTypePartOfSet, grammar.MatcherTypeContainsAllOfSet, grammar.MatcherTypeContainsAnyOfSet, grammar.MatcherTypeStartsWith, grammar.MatcherTypeEndsWith, grammar.MatcherTypeContainsString,
	grammar.MatcherTypeEqualToBoolean, grammar.MatcherTypeMatchesString, grammar.MatcherEqualToSemver, grammar.MatcherTypeGreaterThanOrEqualToSemver, grammar.MatcherTypeLessThanOrEqualToSemver, grammar.MatcherTypeBetweenSemver, grammar.MatcherTypeInListSemver, grammar.MatcherTypeInLargeSegment,
	grammar.MatcherTypeInRuleBasedSegment}

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
func (s *mockStorage) All() []dtos.SplitDTO                      { return make([]dtos.SplitDTO, 0) }
func (s *mockStorage) SegmentNames() *set.ThreadUnsafeSet        { return nil }
func (s *mockStorage) LargeSegmentNames() *set.ThreadUnsafeSet   { return nil }
func (s *mockStorage) SplitNames() []string                      { return make([]string, 0) }
func (s *mockStorage) TrafficTypeExists(trafficType string) bool { return true }
func (s *mockStorage) ChangeNumber() (int64, error)              { return 0, nil }
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
		syncProxyRuleBasedSegmentRules)

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
		syncProxyRuleBasedSegmentRules)

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
		syncProxyRuleBasedSegmentRules)

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
		syncProxyRuleBasedSegmentRules)

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

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules)

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

	if result.Evaluations["mysplittest5"].Treatment != "control" {
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
		syncProxyRuleBasedSegmentRules)

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

	evaluator := NewEvaluator(
		mockedStorage,
		nil,
		nil,
		nil,
		nil,
		logger,
		syncProxyFeatureFlagsRules,
		syncProxyRuleBasedSegmentRules)
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

	if result.Evaluations["mysplittest5"].Treatment != "control" {
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
		syncProxyRuleBasedSegmentRules)
	result := evaluator.EvaluateFeatureByFlagSets(key, &key, []string{"set2"}, nil)

	if len(result.Evaluations) != 0 {
		t.Error("evaluations size should be 0")
	}
}
