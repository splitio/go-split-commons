package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v8/conf"
	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine/grammar/constants"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
	"github.com/stretchr/testify/assert"
)

func createSampleRBSegment(name string) dtos.RuleBasedSegmentDTO {
	return dtos.RuleBasedSegmentDTO{
		Name: name,
		Conditions: []dtos.RuleBasedConditionDTO{
			{
				MatcherGroup: dtos.MatcherGroupDTO{
					Matchers: []dtos.MatcherDTO{
						{
							MatcherType: constants.MatcherTypeInSegment,
							UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
								SegmentName: "segment",
							},
						},
						{
							MatcherType: constants.MatcherTypeInLargeSegment,
							UserDefinedLargeSegment: &dtos.UserDefinedLargeSegmentMatcherDataDTO{
								LargeSegmentName: "largeSegment",
							},
						},
					},
				},
			},
		},
		TrafficTypeName: "user",
	}
}

func TestNewRuleBasedStorage(t *testing.T) {
	mockedRedisClient := mocks.MockClient{}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	logger := logging.NewLogger(&logging.LoggerOptions{})

	storage := NewRuleBasedStorage(mockPrefixedClient, logger)

	assert.NotNil(t, storage)
	assert.Equal(t, mockPrefixedClient, storage.client)
	assert.Equal(t, logger, storage.logger)
	assert.NotNil(t, storage.mutext)
}

func TestRuleBasedSegmentSetChangeNumber(t *testing.T) {
	prefix := "someprefix"
	expectedKey := fmt.Sprintf("%s.%s", prefix, KeyRuleBasedSegmentTill)

	mockedRedisClient := mocks.MockClient{
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			if key != expectedKey {
				t.Error("Unexpected key. Expected:", expectedKey, "Got:", key)
			}
			if value != int64(123) {
				t.Error("Unexpected value. Expected: 123, Got:", value)
			}
			if expiration != 0 {
				t.Error("Unexpected expiration. Expected: 0, Got:", expiration)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, prefix)
	logger := logging.NewLogger(&logging.LoggerOptions{})
	storage := NewRuleBasedStorage(mockPrefixedClient, logger)

	// Test successful case
	err := storage.SetChangeNumber(123)
	assert.NoError(t, err)

	// Test redis error
	mockedRedisClient.SetCall = func(key string, value interface{}, expiration time.Duration) redis.Result {
		return &mocks.MockResultOutput{
			ErrCall: func() error { return assert.AnError },
		}
	}
	err = storage.SetChangeNumber(123)
	assert.Error(t, err)
}

func TestSegmentNamesForRulebased(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.rbsegment.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.rbsegment1", "SPLITIO.rbsegment2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != "someprefix.SPLITIO.rbsegment1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.rbsegment2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment1")),
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment2")),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	ruleBasedStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	segments := ruleBasedStorage.Segments()
	assert.NotNil(t, segments)
	assert.Equal(t, set.NewSet("segment"), segments)
}

func TestLargeNamesForRulebased(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.rbsegment.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.rbsegment1", "SPLITIO.rbsegment2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != "someprefix.SPLITIO.rbsegment1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.rbsegment2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment1")),
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment2")),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	ruleBasedStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	largeSegments := ruleBasedStorage.LargeSegments()
	assert.NotNil(t, largeSegments, "large segments should be not nil")
	assert.Equal(t, set.NewSet("largeSegment"), largeSegments, "incorrect large segments")
}

func TestRuleBasedSegmentChangeNumber(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.rbsegments.till" {
				t.Error("Unexpected key")
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) {
					return "123", nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	logger := logging.NewLogger(&logging.LoggerOptions{})
	storage := NewRuleBasedStorage(mockPrefixedClient, logger)

	// Test successful case
	changeNumber, err := storage.ChangeNumber()
	assert.NoError(t, err)
	assert.Equal(t, int64(123), changeNumber)

	// Test redis error
	mockedRedisClient.GetCall = func(key string) redis.Result {
		return &mocks.MockResultOutput{
			ResultStringCall: func() (string, error) {
				return "", assert.AnError
			},
		}
	}
	changeNumber, err = storage.ChangeNumber()
	assert.Error(t, err)
	assert.Equal(t, int64(-1), changeNumber)

	// Test parse error
	mockedRedisClient.GetCall = func(key string) redis.Result {
		return &mocks.MockResultOutput{
			ResultStringCall: func() (string, error) {
				return "invalid", nil
			},
		}
	}
	changeNumber, err = storage.ChangeNumber()
	assert.Error(t, err)
	assert.Equal(t, int64(-1), changeNumber)
}

func TestGetRuleBasedSegmentByName(t *testing.T) {
	rbs := &dtos.RuleBasedSegmentDTO{
		Name: "test-segment",
	}

	rbsJSON, _ := json.Marshal(rbs)

	mockedRedisClient := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.rbsegment.test-segment" {
				t.Error("Unexpected key")
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) {
					return string(rbsJSON), nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	logger := logging.NewLogger(&logging.LoggerOptions{})
	storage := NewRuleBasedStorage(mockPrefixedClient, logger)

	// Test successful case
	result, err := storage.GetRuleBasedSegmentByName("test-segment")
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, rbs.Name, result.Name)

	// Test redis error
	mockedRedisClient.GetCall = func(key string) redis.Result {
		return &mocks.MockResultOutput{
			ResultStringCall: func() (string, error) {
				return "", assert.AnError
			},
		}
	}
	result, _ = storage.GetRuleBasedSegmentByName("test-segment")
	assert.Nil(t, err)
	assert.Nil(t, result)

	// Test unmarshal error
	mockedRedisClient.GetCall = func(key string) redis.Result {
		return &mocks.MockResultOutput{
			ResultStringCall: func() (string, error) {
				return "invalid json", nil
			},
		}
	}
	result, _ = storage.GetRuleBasedSegmentByName("test-segment")
	assert.Nil(t, err)
	assert.Nil(t, result)
}

func TestRuleBasedSegmentAll(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.rbsegment.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall:   func() int64 { return 0 },
				MultiCall: func() ([]string, error) { return []string{"SPLITIO.rbsegment1", "SPLITIO.rbsegment2"}, nil },
				ErrCall:   func() error { return nil },
			}
		},
		MGetCall: func(keys []string) redis.Result {
			if keys[0] != "someprefix.SPLITIO.rbsegment1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.rbsegment2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment1")),
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment2")),
					}, nil
				},
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	ruleBasedStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	ruleBasedSegments := ruleBasedStorage.All()
	assert.Equal(t, 2, len(ruleBasedSegments), "Unexpected len of rule-based segment")
	assert.False(t, ruleBasedSegments[0].Name != "rbsegment1" || ruleBasedSegments[1].Name != "rbsegment2", "Unexpected returned rule-based segments")
}

func TestRuleBasedSegmentAllClusterMode(t *testing.T) {
	mockedRedisClient := mocks.MockClient{
		KeysCall: func(pattern string) redis.Result {
			t.Error("keys should notbe called.")
			return nil
		},
		MGetCall: func(keys []string) redis.Result {
			if len(keys) > 2 {
				t.Error("there should be 2 keys only")
			}
			if keys[0] != "someprefix.SPLITIO.rbsegment.rbsegment1" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment.rbsegment1", keys[0])
			}
			if keys[1] != "someprefix.SPLITIO.rbsegment.rbsegment2" {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", "someprefix.SPLITIO.rbsegment.rbsegment2", keys[1])
			}
			return &mocks.MockResultOutput{
				MultiInterfaceCall: func() ([]interface{}, error) {
					return []interface{}{
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment1")),
						marshalRuleBasedSegment(createSampleRBSegment("rbsegment2")),
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
						"someprefix.SPLITIO.rbsegment.rbsegment1",
						"someprefix.SPLITIO.rbsegment.rbsegment2",
						"someprefix.SPLITIO.rbsegments.changeNumber",
						"someprefix.SPLITIO.segment.segment1",
					}, nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	ruleBasedStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	ruleBasedSegments := ruleBasedStorage.All()
	assert.Equal(t, 2, len(ruleBasedSegments), "Unexpected len of rule-based segment")
	assert.False(t, ruleBasedSegments[0].Name != "rbsegment1" || ruleBasedSegments[1].Name != "rbsegment2", "Unexpected returned rule-based segments")
}

func TestRuleBasedNamesError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.rbsegment.*"

	mockedRedisClient := mocks.MockClient{
		ScanCall: func(cursor uint64, match string, count int64) redis.Result {
			if match != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, match)
			}

			return &mocks.MockResultOutput{
				IntCall: func() int64 { return 0 },
				MultiCall: func() ([]string, error) {
					return []string{"SPLITIO.rbsegment1", "SPLITIO.rbsegment2"}, errors.New("Some Error")
				},
				ErrCall: func() error { return nil },
			}
		},
		ClusterModeCall: func() bool { return false },
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	ruleBasedStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	ruleBasedNames, err := ruleBasedStorage.RuleBasedSegmentNames()
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(ruleBasedNames), "the rule-based segment len should be 0")
}

func TestRuleBasedSegmentContains(t *testing.T) {
	tests := []struct {
		name                  string
		setupMock             func(*mocks.MockClient)
		ruleBasedSegmentNames []string
		expectedResult        bool
	}{
		{
			name: "all segments exist",
			setupMock: func(mc *mocks.MockClient) {
				mc.ScanCall = func(cursor uint64, match string, count int64) redis.Result {
					return &mocks.MockResultOutput{
						IntCall: func() int64 { return 0 },
						MultiCall: func() ([]string, error) {
							return []string{"someprefix.SPLITIO.rbsegment.segment1", "someprefix.SPLITIO.rbsegment.segment2"}, nil
						},
						ErrCall: func() error { return nil },
					}
				}
				mc.ClusterModeCall = func() bool {
					return false
				}
			},
			ruleBasedSegmentNames: []string{"segment1", "segment2"},
			expectedResult:        true,
		},
		{
			name: "some segments don't exist",
			setupMock: func(mc *mocks.MockClient) {
				mc.ScanCall = func(cursor uint64, match string, count int64) redis.Result {
					return &mocks.MockResultOutput{
						IntCall: func() int64 { return 0 },
						MultiCall: func() ([]string, error) {
							return []string{"someprefix.SPLITIO.rbsegment.segment1"}, nil
						},
						ErrCall: func() error { return nil },
					}
				}
				mc.ClusterModeCall = func() bool {
					return false
				}
			},
			ruleBasedSegmentNames: []string{"segment1", "segment2"},
			expectedResult:        false,
		},
		{
			name: "error getting segment names",
			setupMock: func(mc *mocks.MockClient) {
				mc.ScanCall = func(cursor uint64, match string, count int64) redis.Result {
					return &mocks.MockResultOutput{
						IntCall: func() int64 { return 0 },
						MultiCall: func() ([]string, error) {
							return nil, errors.New("redis error")
						},
						ErrCall: func() error { return errors.New("redis error") },
					}
				}
				mc.ClusterModeCall = func() bool {
					return false
				}
			},
			ruleBasedSegmentNames: []string{"segment1"},
			expectedResult:        false,
		},
		{
			name: "empty segment list",
			setupMock: func(mc *mocks.MockClient) {
				mc.ScanCall = func(cursor uint64, match string, count int64) redis.Result {
					return &mocks.MockResultOutput{
						IntCall: func() int64 { return 0 },
						MultiCall: func() ([]string, error) {
							return []string{"someprefix.SPLITIO.rbsegment.segment1", "someprefix.SPLITIO.rbsegment.segment2"}, nil
						},
						ErrCall: func() error { return nil },
					}
				}
				mc.ClusterModeCall = func() bool {
					return false
				}
			},
			ruleBasedSegmentNames: []string{},
			expectedResult:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockedRedisClient := &mocks.MockClient{}
			tt.setupMock(mockedRedisClient)
			mockPrefixedClient, _ := redis.NewPrefixedRedisClient(mockedRedisClient, "someprefix")
			logger := logging.NewLogger(&logging.LoggerOptions{})
			storage := NewRuleBasedStorage(mockPrefixedClient, logger)

			result := storage.Contains(tt.ruleBasedSegmentNames)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestUpdateRuleBased(t *testing.T) {
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

	toAdd := []dtos.RuleBasedSegmentDTO{createSampleRBSegment("rulebased1"), createSampleRBSegment("rulebased2"), createSampleRBSegment("rulebased3")}

	ruleBasedStorage := NewRuleBasedStorage(redisClient, logging.NewLogger(&logging.LoggerOptions{}))
	ruleBasedStorage.Update(toAdd, []dtos.RuleBasedSegmentDTO{}, 1)
	ruleBasedSegments := ruleBasedStorage.All()
	assert.Equal(t, 3, len(ruleBasedSegments), "Unexpected amount of rule-based")
	till, _ := redisClient.Get("SPLITIO.rbsegments.till")
	tillInt, _ := strconv.ParseInt(till, 0, 64)
	assert.Equal(t, int64(1), tillInt, "ChangeNumber should be 1")

	toRemove := []dtos.RuleBasedSegmentDTO{createSampleRBSegment("rulebased1"), createSampleRBSegment("rulebased2")}
	toAdd = []dtos.RuleBasedSegmentDTO{createSampleRBSegment("rulebased4"), createSampleRBSegment("rulebased5")}
	ruleBasedStorage.Update(toAdd, toRemove, 2)
	ruleBasedSegments = ruleBasedStorage.All()
	assert.Equal(t, 3, len(ruleBasedSegments), "Unexpected size")

	rulebased1, _ := redisClient.Get("SPLITIO.rbsegment.rulebased1")
	assert.Equal(t, "", rulebased1, "Split should not exist")
	till, _ = redisClient.Get("SPLITIO.rbsegments.till")
	tillInt, _ = strconv.ParseInt(till, 0, 64)
	assert.Equal(t, int64(2), tillInt, "ChangeNumber should be 2")
	keys := []string{
		"SPLITIO.rbsegment.rulebased1",
		"SPLITIO.rbsegment.rulebased2",
		"SPLITIO.rbsegment.rulebased3",
		"SPLITIO.rbsegment.rulebased4",
		"SPLITIO.rbsegment.rulebased5",
		"SPLITIO.rbsegments.till",
	}
	redisClient.Del(keys...)
}

func TestReplaceAllRuleBased(t *testing.T) {
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
	toAdd := []dtos.RuleBasedSegmentDTO{createSampleRBSegment("rulebased1"), createSampleRBSegment("rulebased2"), createSampleRBSegment("rulebased3")}

	ruleBasedStorage := NewRuleBasedStorage(redisClient, logging.NewLogger(&logging.LoggerOptions{}))
	ruleBasedStorage.Update(toAdd, []dtos.RuleBasedSegmentDTO{}, 1)

	ruleBasedSegments := ruleBasedStorage.All()
	assert.Equal(t, 3, len(ruleBasedSegments), "Unexpected amount of rule-based")
	till, _ := redisClient.Get("SPLITIO.rbsegments.till")
	tillInt, _ := strconv.ParseInt(till, 0, 64)
	assert.Equal(t, int64(1), tillInt, "ChangeNumber should be 1")

	toReplace := []dtos.RuleBasedSegmentDTO{createSampleRBSegment("rulebased4"), createSampleRBSegment("rulebased5")}

	ruleBasedStorage.ReplaceAll(toReplace, 1)

	ruleBasedSegments = ruleBasedStorage.All()
	assert.Equal(t, 2, len(ruleBasedSegments), "Unexpected size")

	till, _ = redisClient.Get("SPLITIO.rbsegments.till")
	tillInt, _ = strconv.ParseInt(till, 0, 64)
	assert.Equal(t, int64(1), tillInt, "ChangeNumber should be 1")

	keys := []string{
		"SPLITIO.rbsegment.rulebased1",
		"SPLITIO.rbsegment.rulebased2",
		"SPLITIO.rbsegment.rulebased3",
		"SPLITIO.rbsegment.rulebased4",
		"SPLITIO.rbsegment.rulebased5",
		"SPLITIO.rbsegments.till",
	}
	redisClient.Del(keys...)
}

func TestRBFetchMany(t *testing.T) {
	t.Run("FetchMany Error", func(t *testing.T) {
		expectedKey := "someprefix.SPLITIO.rbsegment.someRB1"
		expectedKey2 := "someprefix.SPLITIO.rbsegment.someRB2"

		mockedRedisClient := mocks.MockClient{
			MGetCall: func(keys []string) redis.Result {
				assert.ElementsMatch(t, []string{expectedKey, expectedKey2}, keys)
				return &mocks.MockResultOutput{
					MultiInterfaceCall: func() ([]interface{}, error) {
						return []interface{}{}, errors.New("Some Error")
					},
				}
			},
		}
		mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
		rbStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))
		rbs := rbStorage.FetchMany([]string{"someRB1", "someRB2"})
		assert.Nil(t, rbs)
	})

	t.Run("FetchMany Success", func(t *testing.T) {
		expectedKey := "someprefix.SPLITIO.rbsegment.someRB1"
		expectedKey2 := "someprefix.SPLITIO.rbsegment.someRB2"

		mockedRedisClient := mocks.MockClient{
			MGetCall: func(keys []string) redis.Result {
				assert.ElementsMatch(t, []string{expectedKey, expectedKey2}, keys)
				return &mocks.MockResultOutput{
					MultiInterfaceCall: func() ([]interface{}, error) {
						return []interface{}{
							marshalRuleBasedSegment(createSampleRBSegment("someRB1")),
							marshalRuleBasedSegment(createSampleRBSegment("someRB2")),
						}, nil
					},
				}
			},
		}
		mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

		rbStorage := NewRuleBasedStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))
		rbs := rbStorage.FetchMany([]string{"someRB1", "someRB2"})
		assert.Equal(t, 2, len(rbs))
		assert.NotNil(t, rbs["someRB1"])
		assert.NotNil(t, rbs["someRB2"])
	})
}

func marshalRuleBasedSegment(rbSegment dtos.RuleBasedSegmentDTO) string {
	json, _ := json.Marshal(rbSegment)
	return string(json)
}
