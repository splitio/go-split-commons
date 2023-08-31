package redis

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
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
		KeysCall: func(pattern string) redis.Result {
			if pattern != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, pattern)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.split1", "SPLITIO.split2"}, nil },
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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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
		KeysCall: func(pattern string) redis.Result {
			if pattern != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, pattern)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.split1", "SPLITIO.split2"}, nil },
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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	split := splitStorage.Split("someSplit")
	if split.Name != "someSplit" {
		t.Error("Unexpected result")
	}
}

func TestSplitNamesError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		KeysCall: func(pattern string) redis.Result {
			if pattern != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, pattern)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{}, errors.New("Some Error") },
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	splitNames := splitStorage.SplitNames()
	if len(splitNames) != 0 {
		t.Error("Unexpected result")
	}
}

func TestSplitNames(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"

	mockedRedisClient := mocks.MockClient{
		KeysCall: func(pattern string) redis.Result {
			if pattern != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, pattern)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{"someKey", "someKey2"}, nil },
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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

	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

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
		IncrCall: func(key string) redis.Result {
			return &mocks.MockResultOutput{
				ErrCall:    func() error { return nil },
				ResultCall: func() (int64, error) { return 1, nil },
			}
		},
		SetCall: func(key string, v interface{}, t time.Duration) redis.Result {
			return &mocks.MockResultOutput{
				ErrCall: func() error {
					if key == strings.Replace(KeySplit, "{split}", "split2", 1) {
						return errors.New("something")
					}
					return nil
				},
			}
		},
		DelCall: func(keys ...string) redis.Result {
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) {
					return int64(len(keys)), nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				IncrCall: func(key string) {},
				SetCall:  func(key string, value interface{}, expiration time.Duration) {},
				ExecCall: func() ([]redis.Result, error) {
					atomic.AddInt64(&pipelineCall, 1)
					switch atomic.LoadInt64(&pipelineCall) {
					case 1:
						return []redis.Result{}, nil
					case 2:
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
	splitStorage := NewSplitStorage(mockRedis, logger)

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
	splitStorage = NewSplitStorage(mockRedis, logger)
	err = splitStorage.UpdateWithErrors(toAdd, toRemove, 0)
	if !errors.Is(err, ErrChangeNumberUpdateFailed) {
		t.Error("wrong error type")
	}

}

func TestFlagSetsLogic(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.split.*"
	pipelineCall := int64(0)
	mockedRedisClient := mocks.MockClient{
		KeysCall: func(pattern string) redis.Result {
			if pattern != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, pattern)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{
						"SPLITIO.split1",
						"SPLITIO.split2",
						"SPLITIO.split3",
						"SPLITIO.split4",
						"SPLITIO.split5",
					}, nil
				},
			}
		},
		MGetCall: func(keys []string) redis.Result {
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
		ClusterModeCall: func() bool { return false },
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			return &mocks.MockResultOutput{
				ErrCall: func() error {
					return nil
				},
			}
		},
		DelCall: func(keys ...string) redis.Result {
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 1, nil },
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				ExecCall: func() ([]redis.Result, error) {
					atomic.AddInt64(&pipelineCall, 1)
					switch atomic.LoadInt64(&pipelineCall) {
					case 1:
						return []redis.Result{}, nil
					case 2:
						errCall := &mocks.MockResultOutput{
							ErrCall: func() error { return nil },
						}
						return []redis.Result{errCall, errCall, errCall, errCall, errCall}, nil
					}
					return []redis.Result{}, nil
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
				SetCall: func(key string, value interface{}, expiration time.Duration) {},
				SAddCall: func(key string, members ...interface{}) {
					switch key {
					case "someprefix.SPLITIO.set.set2":
						splits := set.NewSet(members...)
						if !splits.Has("split2") {
							t.Error("split2 should be present")
						}
					case "someprefix.SPLITIO.set.set1":
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
					case "someprefix.SPLITIO.set.set2":
						splits := set.NewSet(members...)
						if !splits.Has("split4") {
							t.Error("split4 should be present")
						}
					case "someprefix.SPLITIO.set.set3":
						splits := set.NewSet(members...)
						if !splits.Has("split2") {
							t.Error("split2 should be present")
						}
					case "someprefix.SPLITIO.set.set4":
						splits := set.NewSet(members...)
						if !splits.Has("split5") {
							t.Error("split5 should be present")
						}
					default:
						t.Error("Unexpected key received", key)
					}
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	splitStorage := NewSplitStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	splits := splitStorage.All()
	if len(splits) != 5 {
		t.Error("Unexpected size")
	}

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
		t.Error(err)
	}
}
