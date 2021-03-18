package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/logging"
	"github.com/splitio/go-toolkit/v4/redis"
	"github.com/splitio/go-toolkit/v4/redis/mocks"
)

func TestRecordLatency(t *testing.T) {
	call := 0
	expectedKey := "someprefix.SPLITIO.telemetry.latencies"
	exceptedField := "go-test-1/test/1.1.1.1/treatment/1"

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

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	telemetryStorage := NewTelemetryStorage(
		mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{SDKVersion: "go-test-1", MachineIP: "1.1.1.1", MachineName: "test"},
	)

	telemetryStorage.RecordLatency(telemetry.Treatment, 1)
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

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	telemetryStorage := NewTelemetryStorage(
		mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{SDKVersion: "go-test-1", MachineIP: "1.1.1.1", MachineName: "test"},
	)

	telemetryStorage.RecordException(telemetry.Treatment)
	if call != 1 {
		t.Error("It should call redis storage")
	}
}

func TestRecordInitDataError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.telemetry.init"
	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key event passeed for push. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 0, errors.New("Some Error") },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	telemetryStorage := NewTelemetryStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{})

	err := telemetryStorage.RecordInitData(dtos.Init{
		OperationMode:      telemetry.Consumer,
		Storage:            "REDIS",
		ActiveFactories:    1,
		RedundantFactories: 0,
	})
	if err == nil {
		t.Error("It should return error")
	}
}

func TestRecordInitData(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.telemetry.init"

	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if len(values) != 1 {
				t.Error("It should sent one init data", len(values))
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 1, nil },
			}
		},
		ExpireCall: func(key string, value time.Duration) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				BoolCall: func() bool { return true },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	telemetryStorage := NewTelemetryStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{})

	err := telemetryStorage.RecordInitData(dtos.Init{
		OperationMode:      telemetry.Consumer,
		Storage:            "REDIS",
		ActiveFactories:    1,
		RedundantFactories: 0,
	})
	if err != nil {
		t.Error("It should not return error")
	}
}
