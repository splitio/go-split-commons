package redis

import (
	"encoding/json"
	"errors"
	"fmt"
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

func TestFetchImpressionsFromQueueWithLock(t *testing.T) {
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
		ExpireCall: func(key string, value time.Duration) redis.Result {
			return &mocks.MockResultOutput{
				BoolCall: func() bool { return true },
			}
		},
	}

	mockPrefixedClient := &redis.PrefixedRedisClient{
		Client: &mockedRedisClient,
		Prefix: "someprefix",
	}

	impressionStorage := NewImpressionStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	impressions, err := impressionStorage.fetchImpressionsFromQueueWithLock(3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(impressions) != 3 {
		t.Error("It should return 3 impressions")
	}
}

func findImpressionsForFeature(bulk []dtos.ImpressionsDTO, featureName string) (*dtos.ImpressionsDTO, error) {
	for _, feature := range bulk {
		if feature.TestName == featureName {
			return &feature, nil
		}
	}
	return nil, fmt.Errorf("Feature %s not found", featureName)
}
func TestRetrieveImpressions(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions"
	expectedStop := 2

	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}
	queueImpression := dtos.ImpressionQueueObject{
		Metadata: metadata,
		Impression: dtos.Impression{
			BucketingKey: "someBuck",
			ChangeNumber: 123456789,
			FeatureName:  "someFeature",
			KeyName:      "someKey",
			Label:        "someLabel",
			Time:         123456789,
			Treatment:    "someTreatment",
		},
	}
	metadata2 := dtos.Metadata{
		SDKVersion:  "php-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}
	queueImpression2 := dtos.ImpressionQueueObject{
		Metadata: metadata2,
		Impression: dtos.Impression{
			BucketingKey: "someBuck",
			ChangeNumber: 123456789,
			FeatureName:  "someFeature2",
			KeyName:      "someKey",
			Label:        "someLabel",
			Time:         123456789,
			Treatment:    "someTreatment",
		},
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

	impressionStorage := NewImpressionStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	impressions, err := impressionStorage.RetrieveImpressions(3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(impressions) != 2 {
		t.Error("It should return 2 keyImpressions")
	}
	if len(impressions[metadata]) != 1 {
		t.Error("It should return 1 impression")
	}
	if len(impressions[metadata2]) != 1 {
		t.Error("It should be 1 impression")
	}
	feature1Impressions, err := findImpressionsForFeature(impressions[metadata], "someFeature")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if len(feature1Impressions.KeyImpressions) != 2 {
		t.Errorf("It should have 2 impressions for someFeature, it have %d", len(feature1Impressions.KeyImpressions))
	}
	feature1Impressions2, err := findImpressionsForFeature(impressions[metadata2], "someFeature2")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if len(feature1Impressions2.KeyImpressions) != 1 {
		t.Errorf("It should have 1 impression for someFeature, it has %d", len(feature1Impressions2.KeyImpressions))
	}
}
