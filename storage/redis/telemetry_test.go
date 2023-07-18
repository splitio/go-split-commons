package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestRecordLatency(t *testing.T) {
	call := 0
	expectedKey := "someprefix.SPLITIO.telemetry.latencies"
	exceptedField := "go-test-1/test/1.1.1.1/treatment/19"

	mockedRedisClient := mocks.MockClient{
		HIncrByCall: func(key, field string, value int64) redis.Result {
			call++
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if field != exceptedField {
				t.Errorf("Unexpected field. Expected: %s Actual: %s", exceptedField, field)
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 0, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	telemetryStorage := NewTelemetryStorage(
		mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{SDKVersion: "go-test-1", MachineIP: "1.1.1.1", MachineName: "test"},
	)

	telemetryStorage.RecordLatency(telemetry.Treatment, (1500 * time.Millisecond))
	if call != 1 {
		t.Error("It should call redis storage")
	}
}

func TestRecordException(t *testing.T) {
	call := 0
	expectedKey := "someprefix.SPLITIO.telemetry.exceptions"
	exceptedField := "go-test-1/test/1.1.1.1/treatment"

	mockedRedisClient := mocks.MockClient{
		HIncrByCall: func(key, field string, value int64) redis.Result {
			call++
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if field != exceptedField {
				t.Errorf("Unexpected field. Expected: %s Actual: %s", exceptedField, field)
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 0, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	telemetryStorage := NewTelemetryStorage(
		mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{SDKVersion: "go-test-1", MachineIP: "1.1.1.1", MachineName: "test"},
	)

	telemetryStorage.RecordException(telemetry.Treatment)
	if call != 1 {
		t.Error("It should call redis storage")
	}
}

func TestRecordConfigDataError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.telemetry.init"
	expectedHashKey := "a/b/c"
	var called bool
	mockedRedisClient := mocks.MockClient{
		HSetCall: func(key string, hashKey string, value interface{}) redis.Result {
			called = true
			if key != expectedKey {
				t.Errorf("Unexpected key event passeed for push. Expected: %s Actual: %s", expectedKey, key)
			}

			if hashKey != expectedHashKey {
				t.Error("wrong hash key: ", hashKey)
			}

			return &mocks.MockResultOutput{
				ErrCall: func() error { return errors.New("some error") },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	telemetryStorage := NewTelemetryStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{
		SDKVersion: "a", MachineName: "b", MachineIP: "c",
	})

	err := telemetryStorage.RecordConfigData(dtos.Config{
		OperationMode:      telemetry.Consumer,
		Storage:            "REDIS",
		ActiveFactories:    1,
		RedundantFactories: 0,
	})
	if err == nil {
		t.Error("It should return error")
	}

	if !called {
		t.Error("hset not called")
	}
}

func TestRecordConfigData(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.telemetry.init"
	expectedHashKey := "a/b/c"
	var called bool
	mockedRedisClient := mocks.MockClient{
		HSetCall: func(key string, hashKey string, value interface{}) redis.Result {
			called = true
			if key != expectedKey {
				t.Errorf("Unexpected key event passeed for push. Expected: %s Actual: %s", expectedKey, key)
			}

			if hashKey != expectedHashKey {
				t.Error("wrong hash key: ", hashKey)
			}

			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	telemetryStorage := NewTelemetryStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{
		SDKVersion: "a", MachineName: "b", MachineIP: "c",
	})

	err := telemetryStorage.RecordConfigData(dtos.Config{
		OperationMode:      telemetry.Consumer,
		Storage:            "REDIS",
		ActiveFactories:    1,
		RedundantFactories: 0,
	})
	if err != nil {
		t.Error("It should not return error")
	}

	if !called {
		t.Error("hset not called")
	}
}

func TestRecordUniqueKeys(t *testing.T) {
	expectedKey := "uniquekeyprefix.SPLITIO.uniquekeys"

	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}

			if len(values) != 2 {
				t.Errorf("Unexpected values len, Expected: 2 Actual: %d", len(values))
			}

			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 1, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "uniquekeyprefix")

	telemetryStorage := NewTelemetryStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{
		SDKVersion: "a", MachineName: "b", MachineIP: "c",
	})

	err := telemetryStorage.RecordUniqueKeys(dtos.Uniques{
		Keys: []dtos.Key{
			{
				Feature: "feature-1",
				Keys:    []string{"key-1", "key-2"},
			},
			{
				Feature: "feature-2",
				Keys:    []string{"key-1", "key-2"},
			},
		},
	})

	if err != nil {
		t.Error("It should not return error")
	}
}
