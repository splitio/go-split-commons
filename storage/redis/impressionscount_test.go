package redis

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestGetImpressionsCount(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.impressions.count"
	mockedRedisClient := mocks.MockClient{
		DelCall: func(keys ...string) redis.Result {
			if len(keys) != 1 && keys[0] != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, keys[0])
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) {
					return int64(len(keys)), nil
				},
			}
		},
		HGetAllCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				MapStringStringCall: func() (map[string]string, error) {
					mapa := map[string]string{
						"Feature-1::2222222": "5",
						"Feature-2::5555555": "6",
						"Feature-5::6666666": "7",
					}

					return mapa, nil
				},
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	storage := NewImpressionsCountStorage(mockPrefixedClient, logging.NewLogger(&logging.LoggerOptions{}))

	result, _ := storage.GetImpressionsCount()

	if len(result.PerFeature) != 3 {
		t.Error("Len should be 3")
	}

	for _, impcount := range result.PerFeature {
		if impcount.FeatureName == "Feature-1" && (impcount.RawCount != 5 || impcount.TimeFrame != 2222222) {
			t.Errorf("Unexpected value for Feature-1. Expected : 5 - 2222222, Actual: %d - %d", impcount.RawCount, impcount.TimeFrame)
		}
		if impcount.FeatureName == "Feature-2" && (impcount.RawCount != 6 || impcount.TimeFrame != 5555555) {
			t.Errorf("Unexpected value for Feature-2. Expected : 6 - 5555555, Actual: %d - %d", impcount.RawCount, impcount.TimeFrame)
		}
		if impcount.FeatureName == "Feature-5" && (impcount.RawCount != 7 || impcount.TimeFrame != 6666666) {
			t.Errorf("Unexpected value for Feature-5. Expected : 7 - 6666666, Actual: %d - %d", impcount.RawCount, impcount.TimeFrame)
		}
	}
}

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
