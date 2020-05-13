package redis

import (
	"encoding/json"
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

func marshalImpression(impresion dtos.ImpressionQueueObject) string {
	json, _ := json.Marshal(impresion)
	return string(json)
}

func TestPopNImpressions(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions"
	expectedStop := 2

	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}
	impression := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment",
	}
	queueImpression := dtos.ImpressionQueueObject{
		Metadata:   metadata,
		Impression: impression,
	}
	queueImpression2 := dtos.ImpressionQueueObject{
		Metadata: dtos.Metadata{
			SDKVersion:  "go-test-2",
			MachineIP:   "1.2.3.4",
			MachineName: "test",
		},
		Impression: impression,
	}

	mockedRedisClient := mocks.MockClient{
		LRangeCall: func(key string, start int64, stop int64) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if start != 0 {
				t.Errorf("Unexpected start. Expected: %d Actual: %d", 0, start)
			}
			if stop != 2 {
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", expectedStop, stop)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{marshalImpression(queueImpression), marshalImpression(queueImpression), marshalImpression(queueImpression2)}, nil
				},
			}
		},
		LTrimCall: func(key string, start, stop int64) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if start != 3 {
				t.Errorf("Unexpected start. Expected: %d Actual: %d", 3, start)
			}
			if stop != -1 {
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", -1, stop)
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

	impressionStorage := NewImpressionStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	impressions, err := impressionStorage.PopN(3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(impressions) != 2 {
		t.Error("It should return 2 impressions")
	}
}
