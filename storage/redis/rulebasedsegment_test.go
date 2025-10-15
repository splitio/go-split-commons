package redis

import (
	"encoding/json"
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
	"github.com/stretchr/testify/assert"
)

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
	result := storage.GetRuleBasedSegmentByName("test-segment")
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
	result = storage.GetRuleBasedSegmentByName("test-segment")
	assert.Nil(t, result)

	// Test unmarshal error
	mockedRedisClient.GetCall = func(key string) redis.Result {
		return &mocks.MockResultOutput{
			ResultStringCall: func() (string, error) {
				return "invalid json", nil
			},
		}
	}
	result = storage.GetRuleBasedSegmentByName("test-segment")
	assert.Nil(t, result)
}
