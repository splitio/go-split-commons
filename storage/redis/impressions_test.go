package redis

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
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

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

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

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

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

func wrapImpression(feature string) dtos.Impression {
	return dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment",
		FeatureName:  feature,
	}
}

func marshalImpression(impression dtos.ImpressionQueueObject) string {
	json, _ := json.Marshal(impression)
	return string(json)
}

func TestPopNImpressionsWithMetadata(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions"

	queueImpression := dtos.ImpressionQueueObject{
		Metadata: dtos.Metadata{
			SDKVersion:  "go-test",
			MachineIP:   "1.2.3.4",
			MachineName: "test",
		},
		Impression: wrapImpression("imp1"),
	}
	queueImpression2 := dtos.ImpressionQueueObject{
		Metadata: dtos.Metadata{
			SDKVersion:  "go-test-2",
			MachineIP:   "1.2.3.4",
			MachineName: "test",
		},
		Impression: wrapImpression("imp2"),
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
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", 2, stop)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{marshalImpression(queueImpression), marshalImpression(queueImpression2), marshalImpression(queueImpression)}, nil
				},
			}
		},
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				LTrimCall: func(key string, start, stop int64) {
					if key != expectedKey {
						t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
					}
					if start != 3 {
						t.Errorf("Unexpected start. Expected: %d Actual: %d", 3, start)
					}
					if stop != -1 {
						t.Errorf("Unexpected stop. Expected: %d Actual: %d", -1, stop)
					}
				},
				LLenCall: func(key string) {
					if key != expectedKey {
						t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
					}
				},
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{
						&mocks.MockResultOutput{},
						&mocks.MockResultOutput{
							ErrCall: func() error { return nil },
							IntCall: func() int64 { return 3 },
						}}, nil
				},
			}
		},

		ExpireCall: func(key string, value time.Duration) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ErrCall:  func() error { return nil },
				BoolCall: func() bool { return true },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")

	impressionStorage := NewImpressionStorage(mockPrefixedClient, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}))

	storedImpressions, err := impressionStorage.PopNWithMetadata(3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(storedImpressions) != 3 {
		t.Error("Unexpected returned impressions")
	}
}
