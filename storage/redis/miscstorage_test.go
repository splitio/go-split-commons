package redis

import (
	"strings"
	"testing"
	"time"

	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestGetApikeyHash(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	client := mocks.MockClient{
		GetCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.hash" {
				t.Error("Unexpected key")
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) {
					return "3376912823", nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&client, "someprefix")
	miscStorage := NewMiscStorage(mockPrefixedClient, logger)
	if str, _ := miscStorage.GetApikeyHash(); str != "3376912823" {
		t.Error("Invalid hash fetched!")
	}
}

func TestSetApikeyHash(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	client := mocks.MockClient{
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			if key != "someprefix.SPLITIO.hash" {
				t.Error("Unexpected key")
			}
			if value != "12345678" {
				t.Error("Unexpected Value")
			}
			if expiration != 0 {
				t.Error("Wrong expiration")
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
		GetCall: func(key string) redis.Result {
			if key != "someprefix.SPLITIO.hash" {
				t.Error("Unexpected key")
			}
			return &mocks.MockResultOutput{
				ResultStringCall: func() (string, error) {
					return "3376912823", nil
				},
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&client, "someprefix")
	miscStorage := NewMiscStorage(mockPrefixedClient, logger)
	err := miscStorage.SetApikeyHash("12345678")
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestClearAll(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	client := mocks.MockClient{
		EvalCall: func(script string, keys []string, args ...interface{}) redis.Result {
			if !strings.Contains(script, "redis.call('KEYS', 'someprefix.SPLITIO.*')") {
				t.Error("It should perform a delete with someprefix")
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&client, "someprefix")
	miscStorage := NewMiscStorage(mockPrefixedClient, logger)

	err := miscStorage.ClearAll()
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestClearAllNoPrefix(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	client := mocks.MockClient{
		EvalCall: func(script string, keys []string, args ...interface{}) redis.Result {
			if !strings.Contains(script, "redis.call('KEYS', 'SPLITIO.*')") {
				t.Error("It should perform a delete with someprefix. GOT: ", script)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}
	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&client, "")
	miscStorage := NewMiscStorage(mockPrefixedClient, logger)

	err := miscStorage.ClearAll()
	if err != nil {
		t.Error("It should not return err")
	}
}
