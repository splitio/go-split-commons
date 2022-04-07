package redis

import (
	"errors"
	"testing"
	"time"

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

func TestSegmentUpdateWithErrors(t *testing.T) {

	var someError = errors.New("some")
	mockClient := &mocks.MockClient{
		SAddCall: func(key string, members ...interface{}) redis.Result {
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 1, someError },
			}
		},
		SRemCall: func(key string, members ...interface{}) redis.Result {
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 0, nil },
			}
		},
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}
	mockRedis, _ := redis.NewPrefixedRedisClient(mockClient, "")
	logger := logging.NewLogger(nil)

	// type assertionr equired because the constructor for some reason is returning the basic interface type
	segmentStorage := NewSegmentStorage(mockRedis, logger).(*SegmentStorage)

	if added, removed, err := segmentStorage.UpdateWithSummary("seg", set.NewSet("key1", "key2"), set.NewSet(), 123); err != nil {
		if suErr, ok := err.(*SegmentUpdateError); ok {
			if suErr.FailureToAdd != someError {
				t.Error("wrong error")
			}

			if e := suErr.FailureToRemove; e != nil {
				t.Error("there should be no failures when removing elements. Got: ", e)
			}
		}

		if added != 1 {
			t.Error("added should be 1")
		}
		if removed != 0 {
			t.Error("added should be 0")
		}
	} else {
		t.Error("there should have been an error")
	}

}
