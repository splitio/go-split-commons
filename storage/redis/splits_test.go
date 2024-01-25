package redis

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/conf"
	"golang.org/x/exp/slices"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func createSampleSplit(name string, sets []string) dtos.SplitDTO {
	return dtos.SplitDTO{
		Name: name,
		Conditions: []dtos.ConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment",
							},
						},
					},
				},
			},
		},
		Sets:            sets,
		TrafficTypeName: "user",
	}
}

func marshalSplit(split dtos.SplitDTO) string {
	json, _ := json.Marshal(split)
	return string(json)
}

func TestAll(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.split1", "SPLITIO.split2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != "someprefix.SPLITIO.split1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.split2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{})),
						marshalSplit(createSampleSplit("split2", []string{})),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splits := splitStorage.All()
	if len(splits) != 2 {
		t.Error("Unexpected size")
	}
	if splits[0].Name != "split1" || splits[1].Name != "split2" {
		t.Error("Unexpected returned splits")
	}
}

func TestAllClusterMode(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		KeysCall: func(pattern string) redis.Result {
			t.Error("keys should notbe called.")
			return nil
		},
		MGetCall: func(keys []string) redis.Result {
			if len(keys) > 2 {
				t.Error("there should be 2 keys only")
			}
			if keys[0] != "someprefix.SPLITIO.split.split1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split.split1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.split.split2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split.split2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{})),
						marshalSplit(createSampleSplit("split2", []string{})),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return true },
		ClusterSlotForKeyCall: func(key string) redis.Result {
			if key != "someprefix.__DUMMY__" {
				t.Error("invalid key. Got: ", key)
			}
			return &mocks.MockResultOutput{ResultCall: func() (int64, error) { return 123, nil }}
		},
		ClusterCountKeysInSlotCall: func(slot int) redis.Result {
			if slot != 123 {
				t.Error("slot should be 123")
			}
			return &mocks.MockResultOutput{ResultCall: func() (int64, error) { return 4, nil }}
		},
		ClusterKeysInSlotCall: func(slot int, count int) redis.Result {
			if slot != 123 || count != 4 {
				t.Error("invalid slot or count. Got: ", slot, count)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{
						"someprefix.SPLITIO.split.split1",
						"someprefix.SPLITIO.split.split2",
						"someprefix.SPLITIO.splits.changeNumber",
						"someprefix.SPLITIO.segment.segment1",
					}, nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splits := splitStorage.All()
	if len(splits) != 2 {
		t.Error("Unexpected size")
	}
	if splits[0].Name != "split1" || splits[1].Name != "split2" {
		t.Error("Unexpected returned splits")
	}
}

func TestChangeNumberError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.splits.till"

	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) { return "", errors.New("Some Error") },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	till, _ := splitStorage.ChangeNumber()
	if till != -1 {
		t.Error("Unexpected till")
	}
}

func TestChangeNumber(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.splits.till"

	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) { return "123456789", nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	till, _ := splitStorage.ChangeNumber()
	if till != 123456789 {
		t.Error("Unexpected till")
	}
}

func TestFetchManyError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.someSplit"
	expectedKey2 := "someprefix.SPLITIO.split.someSplit2"

	mockedRedisClient := mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, keys[0])
			}
			if keys[1] != expectedKey2 {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey2, keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{}, errors.New("Some Error")
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splits := splitStorage.FetchMany([]string{"someSplit", "someSplit2"})
	if splits != nil {
		t.Error("It  should be nil")
	}
}

func TestFetchMany(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.someSplit"
	expectedKey2 := "someprefix.SPLITIO.split.someSplit2"

	mockedRedisClient := mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, keys[0])
			}
			if keys[1] != expectedKey2 {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey2, keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("someSplit", []string{})),
						marshalSplit(createSampleSplit("someSplit2", []string{})),
					}, nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splits := splitStorage.FetchMany([]string{"someSplit", "someSplit2"})
	if len(splits) != 2 {
		t.Error("It should be 2")
	}
	if splits["someSplit"] == nil {
		t.Error("It should return someSplit")
	}
	if splits["someSplit2"] == nil {
		t.Error("It should return someSplit2")
	}
}

// TESTPUTMANY
// TESTREMOVE
func TestSegmentNames(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.split1", "SPLITIO.split2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != "someprefix.SPLITIO.split1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.split2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.split2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{})),
						marshalSplit(createSampleSplit("split2", []string{})),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	segments := splitStorage.SegmentNames()
	if segments == nil || !segments.IsEqual(set.NewSet("segment")) {
		t.Error("Incorrect segments")
		t.Error(segments)
	}
}

func TestSplitError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.someSplit"

	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) { return "", errors.New("Some Error") },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	split := splitStorage.Split("someSplit")
	if split != nil {
		t.Error("Unexpected result")
	}
}

func TestSplit(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.someSplit"

	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) { return marshalSplit(createSampleSplit("someSplit", []string{})), nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	split := splitStorage.Split("someSplit")
	if split.Name != "someSplit" {
		t.Error("Unexpected result")
	}
}

func TestSplitNamesError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall: func() int64 { return 0 },
				MultiCall: func() ([]string, error) {
					return []string{"SPLITIO.split1", "SPLITIO.split2"}, errors.New("Some Error")
				},
				ErrCall: func() error { return nil },
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splitNames := splitStorage.SplitNames()
	if len(splitNames) != 0 {
		t.Error("Unexpected result")
	}
}

func TestSplitNames(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall: func() int64 { return 0 },
				MultiCall: func() ([]string, error) {
					return []string{"SPLITIO.split.someKey", "SPLITIO.split.someKey2"}, nil
				},
				ErrCall: func() error { return nil },
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	splitNames := splitStorage.SplitNames()
	if len(splitNames) != 2 {
		t.Error("Unexpected len of splitNames")
	}
	if splitNames[0] != "someKey" || splitNames[1] != "someKey2" {
		t.Error("Unexpected result")
	}
}

func TestTrafficTypeExists(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			switch key {
			case "someprefix.SPLITIO.trafficType.nonExistantTT":
				return &mocks.MockResultOutput{
					ResultStringCall: func() (string, error) { return "", errors.New("Some Error") },
				}
			case "someprefix.SPLITIO.trafficType.errorTT":
				return &mocks.MockResultOutput{
					ResultStringCall: func() (string, error) { return "errorParsing", nil },
				}
			case "someprefix.SPLITIO.trafficType.someTT":
				return &mocks.MockResultOutput{
					ResultStringCall: func() (string, error) { return "3", nil },
				}
			default:
				t.Error("Unexpected call made")
				return nil
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	exists := splitStorage.TrafficTypeExists("nonExistantTT")
	if exists {
		t.Error("It should be false")
	}

	exists = splitStorage.TrafficTypeExists("errorTT")
	if exists {
		t.Error("It should be false")
	}

	exists = splitStorage.TrafficTypeExists("someTT")
	if !exists {
		t.Error("It should be true")
	}
}

func TestSplitUpdateWithErrors(t *testing.T) {
	pipelineCall := int64(0)
	mockClient := &mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			return &mocks.MockResultOutput{
				ErrCall:            func() error { return nil },
				MultiInterfaceCall: func() ([]interface{}, error) { return []interface{}{}, nil },
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				IncrCall: func(key string) {},
				SetCall:  func(key string, value interface{}, expiration time.Duration) {},
				DelCall:  func(keys ...string) {},
				ExecCall: func() ([]redis.Result, error) {
					atomic.AddInt64(&pipelineCall, 1)
					if atomic.LoadInt64(&pipelineCall) == 1 {
						return []redis.Result{
							&mocks.MockResultOutput{ErrCall: func() error { return nil }},
							&mocks.MockResultOutput{ErrCall: func() error { return errors.New("something") }},
						}, nil
					}
					return []redis.Result{}, nil
				},
			}
		},
	}
	mockRedis, _ := redis.NewPrefixedRedisClient(mockClient, "")
	logger := logging.NewLogger(nil)
	splitStorage := NewSplitStorage(mockRedis, logger, flagsets.NewFlagSetFilter(nil))

	toAdd := []dtos.SplitDTO{{Name: "split1", TrafficTypeName: "tt1"}, {Name: "split2", TrafficTypeName: "tt2"}}
	toRemove := []dtos.SplitDTO{{Name: "split3", TrafficTypeName: "tt3"}}
	err := splitStorage.UpdateWithErrors(toAdd, toRemove, 0)
	if err == nil {
		t.Error("there should have been an error", err)
	}

	var updateErr *UpdateError
	if !errors.As(err, &updateErr) {
		t.Error("the error should have been of type UpdateError")
		return
	}

	if len(updateErr.FailedToAdd) != 1 || len(updateErr.FailedToRemove) != 0 {
		t.Error("there should be 1 element failed for adding and 1 failed for removal")
	}

	if _, ok := updateErr.FailedToAdd["split2"]; !ok {
		t.Error("split2 should have failed")
	}

	mockClient.SetCall = func(key string, value interface{}, expiration time.Duration) redis.Result {
		if key == KeySplitTill {
			return &mocks.MockResultOutput{ErrCall: func() error { return errors.New("sarasa") }}
		}
		return &mocks.MockResultOutput{ErrCall: func() error { return nil }}
	}
	mockRedis, _ = redis.NewPrefixedRedisClient(mockClient, "")
	splitStorage = NewSplitStorage(mockRedis, logger, flagsets.NewFlagSetFilter(nil))
	err = splitStorage.UpdateWithErrors(toAdd, toRemove, 0)
	if !errors.Is(err, ErrChangeNumberUpdateFailed) {
		t.Error("wrong error type")
	}

}

func TestUpdateRedis(t *testing.T) {
	logger := logging.NewLogger(nil)
	prefix := "commons_update_prefix"

	redisClient, err := NewRedisClient(&conf.RedisConfig{
		Host:     "localhost",
		Port:     6379,
		Prefix:   prefix,
		Database: 1,
	}, logger)
	if err != nil {
		t.Error("It should be nil")
	}

	toAdd := []dtos.SplitDTO{createSampleSplit("split1", []string{"set1"}), createSampleSplit("split2", []string{"set1", "set2"}), createSampleSplit("split3", []string{"set3"})}

	splitStorage := NewSplitStorage(redisClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))
	splitStorage.Update(toAdd, []dtos.SplitDTO{}, 1)
	splits := splitStorage.All()
	if len(splits) != 3 {
		t.Error("Unexpected amount of splits")
	}
	set1, err := redisClient.SMembers("SPLITIO.flagSet.set1")
	if len(set1) != 2 {
		t.Error("set size should be 2")
	}
	if !slices.Contains(set1, "split1") || !slices.Contains(set1, "split2") {
		t.Error("Split missing in set")
	}
	tt, err := redisClient.Get("SPLITIO.trafficType.user")
	ttCount, _ := strconv.ParseFloat(tt, 10)
	if ttCount != 3 {
		t.Error("Split should exist")
	}
	till, err := redisClient.Get("SPLITIO.splits.till")
	tillInt, _ := strconv.ParseFloat(till, 10)
	if tillInt != 1 {
		t.Error("ChangeNumber should be 1")
	}

	toRemove := []dtos.SplitDTO{createSampleSplit("split1", []string{"set1"}), createSampleSplit("split2", []string{"set1", "set2"})}
	toAdd = []dtos.SplitDTO{createSampleSplit("split4", []string{"set3"}), createSampleSplit("split5", []string{"set3"})}
	splitStorage.Update(toAdd, toRemove, 2)
	splits = splitStorage.All()
	if len(splits) != 3 {
		t.Error("Unexpected size")
	}
	set1, err = redisClient.SMembers("SPLITIO.flagSet.set1")
	if len(set1) != 0 {
		t.Error("set size should be 0")
	}
	set3, err := redisClient.SMembers("SPLITIO.flagSet.set3")
	if len(set3) != 3 {
		t.Error("set size should be 3")
	}
	if !slices.Contains(set3, "split3") || !slices.Contains(set3, "split4") || !slices.Contains(set3, "split5") {
		t.Error("Split missing in set")
	}
	tt, err = redisClient.Get("SPLITIO.trafficType.user")
	ttCount, _ = strconv.ParseFloat(tt, 10)
	if ttCount != 3 {
		t.Error("Unexpected trafficType occurrences")
	}

	split1, err := redisClient.Get("SPLITIO.split.split1")
	if split1 != "" {
		t.Error("Split should not exist")
	}
	till, err = redisClient.Get("SPLITIO.splits.till")
	tillInt, _ = strconv.ParseFloat(till, 10)
	if tillInt != 2 {
		t.Error("ChangeNumber should be 2")
	}
	keys := []string{
		"SPLITIO.split.split1",
		"SPLITIO.split.split2",
		"SPLITIO.split.split3",
		"SPLITIO.split.split4",
		"SPLITIO.split.split5",
		"SPLITIO.flagSet.set1",
		"SPLITIO.flagSet.set2",
		"SPLITIO.flagSet.set3",
		"SPLITIO.splits.till",
		"SPLITIO.trafficType.user",
	}
	redisClient.Del(keys...)
}

func TestUpdateWithFlagSetFiltersRedis(t *testing.T) {
	logger := logging.NewLogger(nil)
	prefix := "commons_update_filter_prefix"

	redisClient, err := NewRedisClient(&conf.RedisConfig{
		Host:     "localhost",
		Port:     6379,
		Prefix:   prefix,
		Database: 1,
	}, logger)
	if err != nil {
		t.Error("It should be nil")
	}

	toAdd := []dtos.SplitDTO{createSampleSplit("split1", []string{"set1"}), createSampleSplit("split2", []string{"set1", "set2"}), createSampleSplit("split3", []string{"set3"})}
	splitStorage := NewSplitStorage(redisClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter([]string{"set1", "set2"}))
	splitStorage.Update(toAdd, []dtos.SplitDTO{}, 1)
	splits := splitStorage.All()
	if len(splits) != 3 {
		t.Error("Unexpected amount of splits")
	}
	set1, err := redisClient.SMembers("SPLITIO.flagSet.set1")
	if len(set1) != 2 {
		t.Error("set size should be 2")
	}
	set2, err := redisClient.SMembers("SPLITIO.flagSet.set2")
	if len(set2) != 1 {
		t.Error("set size should be 1")
	}
	set3, err := redisClient.SMembers("SPLITIO.flagSet.set3")
	if len(set3) != 0 {
		t.Error("set size should be 0")
	}
	keys := []string{
		"SPLITIO.split.split1",
		"SPLITIO.split.split2",
		"SPLITIO.split.split3",
		"SPLITIO.flagSet.set1",
		"SPLITIO.flagSet.set2",
		"SPLITIO.splits.till",
		"SPLITIO.trafficType.user",
	}
	redisClient.Del(keys...)
}

func TestFetchCurrentFeatureFlags(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			keysToGet := set.NewSet()
			for _, key := range keys {
				keysToGet.Add(key)
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split1") {
				t.Error("It should have split1")
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split2") {
				t.Error("It should have split2")
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{"set1"})),
						marshalSplit(createSampleSplit("split2", []string{"set3"})),
					}, nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, &logging.Logger{}, flagsets.NewFlagSetFilter(nil))
	all, err := splitStorage.fetchCurrentFeatureFlags(
		[]dtos.SplitDTO{createSampleSplit("split1", []string{"set1"})},
		[]dtos.SplitDTO{createSampleSplit("split2", []string{"set2"})})
	if err != nil {
		t.Error("It shouldn't return err")
	}
	if len(all) != 2 {
		t.Error("It should return 2 featureFlags")
	}
}

func TestFlagSetsLogic(t *testing.T) {
	setCall := int64(0)
	mockedRedisClient := mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			keysToGet := set.NewSet()
			for _, key := range keys {
				keysToGet.Add(key)
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split1") {
				t.Error("It should have split1")
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split2") {
				t.Error("It should have split2")
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split3") {
				t.Error("It should have split3")
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split4") {
				t.Error("It should have split4")
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split5") {
				t.Error("It should have split5")
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{"set1"})),
						marshalSplit(createSampleSplit("split2", []string{"set3"})),
						marshalSplit(createSampleSplit("split3", []string{})),
						marshalSplit(createSampleSplit("split4", []string{"set2"})),
						marshalSplit(createSampleSplit("split5", []string{"set4"})),
					}, nil
				},
			}
		},
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			if key != "someprefix.SPLITIO.splits.till" {
				t.Error("It should call set changeNumber", key)
			}
			cn, _ := value.(int64)
			if cn != 2 {
				t.Error("It should be 2", cn)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error {
					return nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{}, nil
				},
				SetCall: func(key string, value interface{}, expiration time.Duration) {
					atomic.AddInt64(&setCall, 1)
					switch atomic.LoadInt64(&setCall) {
					case 1:
						if key != "someprefix.SPLITIO.split.split1" {
							t.Error("Expected set for split1")
						}
					case 2:
						if key != "someprefix.SPLITIO.split.split2" {
							t.Error("Expected set for split2")
						}
					case 3:
						if key != "someprefix.SPLITIO.split.split3" {
							t.Error("Expected set for split3")
						}
					case 4:
						if key != "someprefix.SPLITIO.split.split5" {
							t.Error("Expected set for split5")
						}
					default:
						t.Error("Unexpected key")
					}
				},
				DelCall: func(keys ...string) {
					if len(keys) != 1 {
						t.Error("Unexpected size of keys to be removed")
					}
					if keys[0] != "someprefix.SPLITIO.split.split4" {
						t.Error("split4 should be deleted")
					}
				},
				SAddCall: func(key string, members ...interface{}) {
					switch key {
					case "someprefix.SPLITIO.flagSet.set1":
						if len(members) != 2 {
							t.Error("Only 2 elements should be added")
						}
						splits := set.NewSet(members...)
						if !splits.Has("split2") {
							t.Error("split2 should be present")
						}
						if !splits.Has("split3") {
							t.Error("split3 should be present")
						}
					case "someprefix.SPLITIO.flagSet.set2":
						if len(members) != 1 {
							t.Error("Only 1 element should be added")
						}
						splits := set.NewSet(members...)
						if !splits.Has("split2") {
							t.Error("split2 should be present")
						}
					default:
						t.Error("Unexpected key received", key)
					}
				},
				SRemCall: func(key string, members ...interface{}) {
					switch key {
					case "someprefix.SPLITIO.flagSet.set2":
						if len(members) != 1 {
							t.Error("Only 1 element should be removed")
						}
						splits := set.NewSet(members...)
						if !splits.Has("split4") {
							t.Error("split4 should be present")
						}
					case "someprefix.SPLITIO.flagSet.set3":
						if len(members) != 1 {
							t.Error("Only 1 element should be removed")
						}
						splits := set.NewSet(members...)
						if !splits.Has("split2") {
							t.Error("split2 should be present")
						}
					case "someprefix.SPLITIO.flagSet.set4":
						if len(members) != 1 {
							t.Error("Only 1 element should be removed")
						}
						splits := set.NewSet(members...)
						if !splits.Has("split5") {
							t.Error("split5 should be present")
						}
					default:
						t.Error("Unexpected key received", key)
					}
				},
				DecrCall: func(key string) {
					if key != "someprefix.SPLITIO.trafficType.user" {
						t.Error("Invalid key to be decremented")
					}
				},
				IncrCall: func(key string) {
					if key != "someprefix.SPLITIO.trafficType.user" {
						t.Error("Invalid key to be incremented")
					}
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, &logging.Logger{}, flagsets.NewFlagSetFilter(nil))

	// set1 -> split1
	// set2 -> split4
	// set3 -> split2
	// set4 -> split5

	// set1 -> split1, split2, split3
	// set2 -> split2
	// set3 ->
	// set4 ->

	// add split2 split3 into set1
	// add split2 into set2
	// remove split4 from set2
	// remove split2 from set3
	// remove split5 from set4
	err := splitStorage.UpdateWithErrors(
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1"}),
			createSampleSplit("split2", []string{"set1", "set2"}),
			createSampleSplit("split3", []string{"set1"}),
			createSampleSplit("split5", []string{}),
		},
		[]dtos.SplitDTO{
			createSampleSplit("split4", []string{"set2"}),
		}, 2)
	if err != nil {
		t.Error("It shouldn't return err")
	}

	mockedRedisClient2 := mocks.MockClient{
		MGetCall: func(keys []string) redis.Result {
			keysToGet := set.NewSet()
			for _, key := range keys {
				keysToGet.Add(key)
			}
			if !keysToGet.Has("someprefix.SPLITIO.split.split1") {
				t.Error("It should have split1")
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalSplit(createSampleSplit("split1", []string{"set1"})),
					}, nil
				},
			}
		},
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			if key != "someprefix.SPLITIO.splits.till" {
				t.Error("It should call set changeNumber", key)
			}
			cn, _ := value.(int64)
			if cn != 3 {
				t.Error("It should be 3", cn)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error {
					return nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{}, nil
				},
				SetCall: func(key string, value interface{}, expiration time.Duration) {
					t.Error("Nothing should be set")
				},
				DelCall: func(keys ...string) {
					if len(keys) != 1 {
						t.Error("Unexpected size of keys to be removed")
					}
					if keys[0] != "someprefix.SPLITIO.split.split1" {
						t.Error("split1 should be deleted")
					}
				},
				SAddCall: func(key string, members ...interface{}) {
					t.Error("Nothing should be added")
				},
				SRemCall: func(key string, members ...interface{}) {
					if key != "someprefix.SPLITIO.flagSet.set1" {
						t.Error("It should be updated set1")
					}
					if len(members) != 1 {
						t.Error("Only 1 element should be removed")
					}
					splits := set.NewSet(members...)
					if !splits.Has("split1") {
						t.Error("split1 should be present")
					}
				},
				DecrCall: func(key string) {
					if key != "someprefix.SPLITIO.trafficType.user" {
						t.Error("wrong tt to be decremented")
					}
				},
				IncrCall: func(key string) {
					t.Error("Nothing should be incremented")
				},
			}
		},
	}
	mockPrefixedClient2, _ := redis.NewPrefixedRedisClient(&mockedRedisClient2, "someprefix")
	splitStorage2 := NewSplitStorage(mockPrefixedClient2, &logging.Logger{}, flagsets.NewFlagSetFilter(nil))

	err = splitStorage2.UpdateWithErrors(
		[]dtos.SplitDTO{},
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1"}),
		}, 3)
	if err != nil {
		t.Error("It shouldn't return err")
	}

}

func TestCalculateSets(t *testing.T) {
	mockedRedisClient := mocks.MockClient{}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, &logging.Logger{}, flagsets.NewFlagSetFilter(nil))

	currentSets := flagsets.NewFeaturesBySet(nil)
	currentSets.Add("set1", "split1")
	currentSets.Add("set1", "split2")
	currentSets.Add("set1", "split3")
	currentSets.Add("set2", "split2")
	currentSets.Add("set3", "split2")
	currentSets.Add("set4", "split3")

	toAdd, toRemove := splitStorage.calculateSets(
		currentSets,
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1", "set4"}),
			createSampleSplit("split5", []string{"set1"}),
		},
		[]dtos.SplitDTO{
			createSampleSplit("split2", []string{"set2"}),
			createSampleSplit("split3", []string{"set1", "set4"}),
		},
	)

	if len(toAdd.Sets()) != 2 {
		t.Error("It should add two sets")
	}
	if len(toAdd.FlagsFromSet("set1")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	asSet := set.NewSet()
	for _, flag := range toAdd.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if !asSet.Has("split5") {
		t.Error("split5 should be present in set1")
	}
	if len(toAdd.FlagsFromSet("set4")) != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	asSet = set.NewSet()
	for _, flag := range toAdd.FlagsFromSet("set4") {
		asSet.Add(flag)
	}
	if !asSet.Has("split1") {
		t.Error("split1 should be present in set4")
	}

	// CurrentSets is updated and tracks the featureFlags to be removed
	if len(toRemove.Sets()) != 4 {
		t.Error("It should consider 4 sets to remove")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set1") {
		asSet.Add(flag)
	}
	if asSet.Size() != 2 {
		t.Error("It should have only 2 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set1")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should be present in set1")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set2") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set2")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set3") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split2") {
		t.Error("split2 should be present in set3")
	}
	asSet = set.NewSet()
	for _, flag := range toRemove.FlagsFromSet("set4") {
		asSet.Add(flag)
	}
	if asSet.Size() != 1 {
		t.Error("It should have only 1 featureFlags")
	}
	if !asSet.Has("split3") {
		t.Error("split3 should be present in set4")
	}

	currentSets = flagsets.NewFeaturesBySet(nil)
	currentSets.Add("set1", "split1")
	currentSets.Add("set1", "split2")
	currentSets.Add("set1", "split3")
	currentSets.Add("set2", "split2")
	currentSets.Add("set3", "split2")
	currentSets.Add("set4", "split3")
	splitStorage = NewSplitStorage(mockPrefixedClient, &logging.Logger{}, flagsets.NewFlagSetFilter([]string{"set1", "set2", "set3", "set4"}))
	toAdd, toRemove = splitStorage.calculateSets(
		currentSets,
		[]dtos.SplitDTO{
			createSampleSplit("split1", []string{"set1", "set5"}),
			createSampleSplit("split5", []string{"set6"}),
		},
		[]dtos.SplitDTO{},
	)

	if len(toAdd.Sets()) != 0 {
		t.Error("It should not add sets")
	}
}

func TestGetNamesByFlagSets(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		SMembersCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.split.split1" {
				t.Error("Expected set for split1")
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					flags := [1]string{"split1"}
					return flags[:], nil
				},
				ErrCall: func() error {
					return nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{
						&mocks.MockResultOutput{
							MultiCall: func() ([]string, error) {
								return []string{}, errors.New("Simulating error")
							},
						},
						&mocks.MockResultOutput{
							MultiCall: func() ([]string, error) {
								return []string{"flag1", "flag2"}, nil
							},
						},
						&mocks.MockResultOutput{
							MultiCall: func() ([]string, error) {
								return []string{"flag2", "flag3"}, nil
							},
						},
					}, nil
				},
				SMembersCall: func(key string) {
					switch key {
					case "someprefix.SPLITIO.flagSet.set1":
						if key != "someprefix.SPLITIO.flagSet.set1" {
							t.Error("Expected set for split1")
						}
					case "someprefix.SPLITIO.flagSet.set2":
						if key != "someprefix.SPLITIO.flagSet.set2" {
							t.Error("Expected set for split2")
						}
					case "someprefix.SPLITIO.flagSet.set3":
						if key != "someprefix.SPLITIO.flagSet.set3" {
							t.Error("Expected set for split3")
						}
					default:
						t.Error("Unexpected key received", key)
					}

				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	flagsBySets := splitStorage.GetNamesByFlagSets([]string{"set1", "set2", "set3"})
	sSet1, existSet1 := flagsBySets["set1"]
	if !existSet1 {
		t.Error("set1 should exist")
	}
	if len(sSet1) != 0 {
		t.Error("size of names by set1 should be 0, but was: ", len(sSet1))
	}

	sSet2, existSet2 := flagsBySets["set2"]
	if !existSet2 {
		t.Error("set2 should exist")
	}
	if len(sSet2) != 2 {
		t.Error("size of names by set2 should be 2, but was: ", len(sSet2))
	}

	sSet3, existSet3 := flagsBySets["set3"]
	if !existSet3 {
		t.Error("set3 should exist")
	}
	if len(sSet3) != 2 {
		t.Error("size of names by set3 should be 2, but was: ", len(sSet3))
	}
}

func TestGetNamesByFlagSetsPipelineExecError(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		SMembersCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.split.split1" {
				t.Error("Expected set for split1")
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					flags := [1]string{"split1"}
					return flags[:], nil
				},
				ErrCall: func() error {
					return nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				ExecCall: func() ([]redis.Result, error) {
					return nil, errors.New("Exec error")
				},
				SMembersCall: func(key string) {
					switch key {
					case "someprefix.SPLITIO.flagSet.set1":
						if key != "someprefix.SPLITIO.flagSet.set1" {
							t.Error("Expected set for split1")
						}
					case "someprefix.SPLITIO.flagSet.set2":
						if key != "someprefix.SPLITIO.flagSet.set2" {
							t.Error("Expected set for split2")
						}
					case "someprefix.SPLITIO.flagSet.set3":
						if key != "someprefix.SPLITIO.flagSet.set3" {
							t.Error("Expected set for split3")
						}
					default:
						t.Error("Unexpected key received", key)
					}

				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	flagsBySets := splitStorage.GetNamesByFlagSets([]string{"set1", "set2", "set3"})
	if len(flagsBySets) != 0 {
		t.Error("Flags should be 0")
	}
}

func TestGetAllFlagSetNames(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != "someprefix.SPLITIO.flagSet.*" {
				t.Error("Unexpected key received", match)
			}

			if cursor == 0 {
				return &mocks.MockResultOutput{
					IntCall: func() int64 {
						return 10
					},
					MultiCall: func() ([]string, error) {
						return []string{"set_1", "set_2", "set_3", "set_4", "set_5"}, nil
					},
					ErrCall: func() error {
						return nil
					},
				}
			}

			return &mocks.MockResultOutput{
				IntCall: func() int64 {
					return 0
				},
				MultiCall: func() ([]string, error) {
					return []string{"set_1", "set_2", "set_3", "set_4", "set_5"}, nil
				},
				ErrCall: func() error {
					return nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	names := splitStorage.GetAllFlagSetNames()
	if len(names) != 10 {
		t.Error("should be 10 keys. Got: ", len(names))
	}
}

func TestGetAllSplitKeys(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		ClusterModeCall: func() bool {
			return false
		},
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != "someprefix.SPLITIO.split.*" {
				t.Error("Unexpected key received", match)
			}

			if cursor == 0 {
				return &mocks.MockResultOutput{
					IntCall:   func() int64 { return 10 },
					MultiCall: func() ([]string, error) { return []string{"SPLITIO.split.split1"}, nil },
					ErrCall:   func() error { return nil },
				}
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.split.split2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), flagsets.NewFlagSetFilter(nil))

	keys, err := splitStorage.getAllSplitKeys()
	if err != nil {
		t.Error("Error should be nil")
	}

	if len(keys) != 2 {
		t.Errorf("Keys len should be 2. Actual: %v", len(keys))
	}

	if keys[0] != "SPLITIO.split.split1" {
		t.Errorf("Key should be 'SPLITIO.split.split1'. Actual: %s", keys[0])
	}

	if keys[1] != "SPLITIO.split.split2" {
		t.Errorf("Key should be 'SPLITIO.split.split2'. Actual: %s", keys[1])
	}
}
