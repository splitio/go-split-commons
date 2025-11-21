package impression

import (
	"errors"
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func getImpressions(amount int) []dtos.Impression {
	impressions := []dtos.Impression{}

	i := 1
	for i <= amount {
		impressions = append(impressions, dtos.Impression{
			BucketingKey: fmt.Sprintf("someBucketingKey%d", i),
			ChangeNumber: 123456789,
			FeatureName:  fmt.Sprintf("someFeature%d", i),
			KeyName:      fmt.Sprintf("someKey%d", i),
			Label:        "someLabel",
			Time:         123456789,
			Treatment:    fmt.Sprintf("someTreatment%d", i),
		})

		i++
	}

	return impressions
}

func TestRedisImpressionRecorderError(t *testing.T) {
	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}

			return make([]dtos.Impression, 0), errors.New("Some")
		},
	}
	impressionRedisMockStorage := mocks.MockImpressionStorage{}

	recorder := NewRecorderRedis(impressionMockStorage, impressionRedisMockStorage, logging.NewLogger(&logging.LoggerOptions{}))
	err := recorder.SynchronizeImpressions(50)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestRedisImpressionRecorderWithoutImpressions(t *testing.T) {
	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.Impression, 0), nil
		},
	}
	impressionRedisMockStorage := mocks.MockImpressionStorage{}

	recorder := NewRecorderRedis(impressionMockStorage, impressionRedisMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	err := recorder.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestSynchronizeImpressions(t *testing.T) {
	popNCount := 0
	logCount := 0
	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}

			popNCount++
			return getImpressions(10), nil
		},
	}

	impressionRedisMockStorage := mocks.MockImpressionStorage{
		LogImpressionsCall: func(impressions []dtos.Impression) error {
			if len(impressions) != 10 {
				t.Errorf("Impression len should be 10. Actual %d", len(impressions))
			}

			logCount++
			return nil
		},
	}

	recorder := NewRecorderRedis(impressionMockStorage, impressionRedisMockStorage, logging.NewLogger(&logging.LoggerOptions{}))

	err := recorder.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}

	if popNCount != 1 {
		t.Errorf("PopN count should be 1. Actual %d", popNCount)
	}

	if logCount != 1 {
		t.Errorf("LogImpressions count should be 1. Actual %d", logCount)
	}
}
