package redis

import (
	"testing"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestRecordImpressionsCount(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions.count"

	mockedRedisClient := mocks.MockClient{
		PipelineCall: func() redis.Pipeline {
			return &mocks.MockPipeline{
				HIncrByCall: func(key, field string, value int64) {
					if key != expectedKey {
						t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
					}
					if field == "feature-name-test::22222" && value != 3 {
						t.Errorf("Unexpected value. Expected: %d Actual: %d", 3, value)
					}
					if field == "other-feature-name::3333" && value != 5 {
						t.Errorf("Unexpected value. Expected: %d Actual: %d", 3, value)
					}
				},
				ExecCall: func() ([]redis.Result, error) {
					return []redis.Result{
						&mocks.MockResultOutput{},
						&mocks.MockResultOutput{}}, nil
				},
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	storage := NewImpressionsCountStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	err := storage.RecordImpressionsCount(dtos.ImpressionsCountDTO{
		PerFeature: []dtos.ImpressionsInTimeFrameDTO{
			{
				FeatureName: "feature-name-test",
				TimeFrame:   22222,
				RawCount:    3,
			},
			{
				FeatureName: "other-feature-name",
				TimeFrame:   3333,
				RawCount:    5,
			},
		},
	})

	if err != nil {
		t.Error("It should not return error")
	}
}
