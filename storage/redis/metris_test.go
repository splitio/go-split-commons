package redis

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/go-toolkit/redis/mocks"
)

func TestPutGauge(t *testing.T) {
	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}

	expectedKey := "someprefix.SPLITIO/go-test/test/gauge.g1"
	expectedValue := 3.345

	mockedRedisClient := mocks.MockClient{
		SetCall: func(key string, value interface{}, expiration time.Duration) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if value != expectedValue {
				t.Errorf("Unexpected value. Expected: %f Actual: %f", expectedValue, value)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	metricStorage := NewMetricsStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	metricStorage.PutGauge("g1", 3.345)
}

func TestIncLatency(t *testing.T) {
	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}

	expectedKey := "someprefix.SPLITIO/go-test/test/latency.m1.bucket.13"

	mockedRedisClient := mocks.MockClient{
		IncrCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	metricStorage := NewMetricsStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	metricStorage.IncLatency("m1", 13)
}

func TestIncCounter(t *testing.T) {
	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}

	expectedKey := "someprefix.SPLITIO/go-test/test/count.count1"

	mockedRedisClient := mocks.MockClient{
		IncrCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	metricStorage := NewMetricsStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	metricStorage.IncCounter("count1")
}
