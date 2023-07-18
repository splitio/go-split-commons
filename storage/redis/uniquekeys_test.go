package redis

import (
	"encoding/json"
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func marshalUniqueKeys(uniques dtos.Uniques) string {
	json, _ := json.Marshal(uniques.Keys)
	return string(json)
}

func TestCount(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.uniquekeys"

	mockedRedisClient := mocks.MockClient{
		LLenCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}

			return &mocks.MockResultOutput{
				ErrCall:    func() error { return nil },
				ResultCall: func() (int64, error) { return 8, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	uniqueStorage := NewUniqueKeysMultiSdkConsumer(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	count := uniqueStorage.Count()
	if count != 8 {
		t.Error("Count should be 8")
	}
}

func TestPopNRaw(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.uniquekeys"

	uniques := dtos.Uniques{
		Keys: []dtos.Key{
			{
				Feature: "feature-test-1",
				Keys:    []string{"key-1", "key-2", "key-3"},
			},
			{
				Feature: "feature-test-2",
				Keys:    []string{"key-1", "key-2", "key-3"},
			},
			{
				Feature: "feature-test-3",
				Keys:    []string{"key-1", "key-2", "key-3"},
			},
		},
	}

	mockedRedisClient := mocks.MockClient{
		LRangeCall: func(key string, start, stop int64) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if start != 0 {
				t.Errorf("Unexpected start. Expected: %d Actual: %d", 0, start)
			}
			if stop != 9 {
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", 9, stop)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{marshalUniqueKeys(uniques), marshalUniqueKeys(uniques)}, nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				LTrimCall: func(key string, start, stop int64) {
					if key != expectedKey {
						t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
					}
					if start != 2 {
						t.Errorf("Unexpected start. Expected: %d Actual: %d", 2, start)
					}
					if stop != -1 {
						t.Errorf("Unexpected stop. Expected: %d Actual: %d", -1, stop)
					}
				},
				LLenCall: func(key string) {
					if key != expectedKey {
						t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
					}
				},
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{
						&mocks.MockResultOutput{},
						&mocks.MockResultOutput{
							ErrCall: func() error { return nil },
							IntCall: func() int64 { return 3 },
						}}, nil
				},
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	uniqueStorage := NewUniqueKeysMultiSdkConsumer(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	storedUniques, left, err := uniqueStorage.PopNRaw(10)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(storedUniques) != 2 {
		t.Error("Unexpected returned uniques")
	}
	if left != 3 {
		t.Error("Left should be 3")
	}
}
