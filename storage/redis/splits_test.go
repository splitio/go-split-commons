package redis

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/go-toolkit/redis/mocks"
)

func createSampleSplit(name string) dtos.SplitDTO {
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
						marshalSplit(createSampleSplit("split1")),
						marshalSplit(createSampleSplit("split2")),
					}, nil
				},
			}
		},
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
						marshalSplit(createSampleSplit("someSplit")),
						marshalSplit(createSampleSplit("someSplit2")),
					}, nil
				},
			}
		},
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
						marshalSplit(createSampleSplit("split1")),
						marshalSplit(createSampleSplit("split2")),
					}, nil
				},
			}
		},
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
				ResultStringCall: func() (string, error) { return marshalSplit(createSampleSplit("someSplit")), nil },
			}
		},
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	}
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

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
