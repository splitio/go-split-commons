package redis

import (
	"errors"
	"testing"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestSegmentKeysErrort(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.segment.someSegment"

	mockedRedisClient := mocks.MockClient{
		SMembersCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return nil, nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	segmentStorage := NewSegmentStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	segment := segmentStorage.Keys("someSegment")
	if segment != nil {
		t.Error("Unexpected result")
	}
}

func TestSegmentKeysNonExistent(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.segment.someSegment"

	mockedRedisClient := mocks.MockClient{
		SMembersCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{}, nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	segmentStorage := NewSegmentStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	segment := segmentStorage.Keys("someSegment")
	if segment != nil {
		t.Error("Unexpected result")
	}
}
func TestSegmentKeys(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.segment.someSegment"

	mockedRedisClient := mocks.MockClient{
		SMembersCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) { return []string{"key1", "key2"}, nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	segmentStorage := NewSegmentStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	segment := segmentStorage.Keys("someSegment")
	if segment == nil || !segment.IsEqual(set.NewSet("key1", "key2")) {
		t.Error("Incorrect segment")
		t.Error(segment)
	}
}

func TestSegmentTillError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.segment.someSegment.till"

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

	segmentStorage := NewSegmentStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	till, _ := segmentStorage.ChangeNumber("someSegment")
	if till != -1 {
		t.Error("Unexpected till")
	}
}

func TestSegmentTill(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.segment.someSegment.till"

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

	segmentStorage := NewSegmentStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	till, _ := segmentStorage.ChangeNumber("someSegment")
	if till != 123456789 {
		t.Error("Unexpected till")
	}
}
