package evaluator

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
	"github.com/splitio/go-split-commons/v5/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v5/storage/mocks"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

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
func (s *mockStorage) SplitNames() []string                      { return make([]string, 0) }
func (s *mockStorage) TrafficTypeExists(trafficType string) bool { return true }
func (s *mockStorage) ChangeNumber() (int64, error)              { return 0, nil }
func (s *mockStorage) GetNamesByFlagSets(sets []string) map[string][]string {
	return make(map[string][]string)
}

func TestSplitWithoutConfigurations(t *testing.T) {
	logger := logging.NewLogger(nil)

	evaluator := NewEvaluator(
		&mockStorage{},
		nil,
		nil,
		logger)

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
		logger)

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
		logger)

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
		logger)

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
		logger)

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
		logger)

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
		logger)
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
		logger)
	result := evaluator.EvaluateFeatureByFlagSets(key, &key, []string{"set2"}, nil)

	if len(result.Evaluations) != 0 {
		t.Error("evaluations size should be 0")
	}
}
