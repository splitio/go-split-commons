package redis

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestRecordImpressionsCount(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions.count"
	hincrbyCall := 0
	expireCall := 0
	mockedRedisClient := mocks.MockClient{
		ExpireCall: func(key string, value time.Duration) redis.Result {
			return &mocks.MockResultOutput{
				BoolCall: func() bool {
					expireCall++
					return true
				},
			}
		},
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
					if field == "feature-name::3333" && value != 8 {
						t.Errorf("Unexpected value. Expected: %d Actual: %d", 8, value)
					}

					hincrbyCall++
				},
				HLenCall: func(key string) {},
				ExecCall: func() ([]redis.Result, error) {
					if hincrbyCall == 3 {
						return []redis.Result{
							&mocks.MockResultOutput{
								IntCall: func() int64 {
									return 3
								},
							},
							&mocks.MockResultOutput{
								IntCall: func() int64 {
									return 5
								},
							},
							&mocks.MockResultOutput{
								IntCall: func() int64 {
									return 8
								},
							},
							&mocks.MockResultOutput{
								IntCall: func() int64 {
									return 3
								},
							}}, nil
					}

					return []redis.Result{
						&mocks.MockResultOutput{
							IntCall: func() int64 {
								return 6
							},
						},
						&mocks.MockResultOutput{
							IntCall: func() int64 {
								return 10
							},
						},
						&mocks.MockResultOutput{
							IntCall: func() int64 {
								return 16
							},
						},
						&mocks.MockResultOutput{
							IntCall: func() int64 {
								return 3
							},
						}}, nil
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
			{
				FeatureName: "feature-name",
				TimeFrame:   3333,
				RawCount:    8,
			},
		},
	})

	if err != nil {
		t.Error("It should not return error")
	}

	if hincrbyCall != 3 {
		t.Error("Call should be 3")
	}

	if expireCall != 1 {
		t.Error("expireCall should be 1")
	}

	err = storage.RecordImpressionsCount(dtos.ImpressionsCountDTO{
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
			{
				FeatureName: "feature-name",
				TimeFrame:   3333,
				RawCount:    8,
			},
		},
	})

	if err != nil {
		t.Error("It should not return error")
	}

	if hincrbyCall != 6 {
		t.Error("Call should be 3")
	}

	if expireCall != 1 {
		t.Error("expireCall should be 1")
	}
}
