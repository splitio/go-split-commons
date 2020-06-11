package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/go-toolkit/redis/mocks"
)

func TestLogImpressionsError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions"
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

	impressionStorage := NewImpressionStorage(mockPrefixedClient, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}))

	impression := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment",
	}

	err := impressionStorage.LogImpressions([]dtos.Impression{impression})
	if err == nil {
		t.Error("It should return error")
	}
}

func TestLogImpressions(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions"
	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if len(values) != 1 {
				t.Error("It should sent one impression", len(values))
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

	impressionStorage := NewImpressionStorage(mockPrefixedClient, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}))

	impression := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment",
	}

	err := impressionStorage.LogImpressions([]dtos.Impression{impression})
	if err != nil {
		t.Error("It should not return error")
	}
}
