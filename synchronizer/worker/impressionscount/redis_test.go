package impressionscount

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/provisional/strategy"
	"github.com/splitio/go-split-commons/v8/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSynchronizeImpressionsCount(t *testing.T) {
	timeFrame1 := time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano()
	timeFrame2 := time.Date(2020, 9, 2, 11, 0, 0, 0, time.UTC).UnixNano()
	featureName1 := "feature-name-1"
	featureName2 := "feature-name-2"

	impressionsCountStorage := mocks.MockImpressionsCountStorage{
		RecordImpressionsCountCall: func(impressions dtos.ImpressionsCountDTO) error {
			if len(impressions.PerFeature) != 3 {
				t.Errorf("Len should be 3. Actual: %d", len(impressions.PerFeature))
			}

			for _, imp := range impressions.PerFeature {
				if imp.FeatureName == featureName1 && imp.TimeFrame == timeFrame1 && imp.RawCount != 11 {
					t.Errorf("Unexpected rawcount for %s::%d. Actual: %d", featureName1, timeFrame1, imp.RawCount)
				}
				if imp.FeatureName == featureName1 && imp.TimeFrame == timeFrame2 && imp.RawCount != 8 {
					t.Errorf("Unexpected rawcount for %s::%d. Actual: %d", featureName1, timeFrame2, imp.RawCount)
				}
				if imp.FeatureName == featureName2 && imp.TimeFrame == timeFrame1 && imp.RawCount != 3 {
					t.Errorf("Unexpected rawcount for %s::%d. Actual: %d", featureName2, timeFrame1, imp.RawCount)
				}
			}

			return nil
		},
	}

	impressionsCounter := strategy.NewImpressionsCounter()
	impressionsCounter.Inc(featureName1, timeFrame1, 2)
	impressionsCounter.Inc(featureName1, timeFrame1, 4)
	impressionsCounter.Inc(featureName1, timeFrame1, 5)
	impressionsCounter.Inc(featureName1, timeFrame2, 8)
	impressionsCounter.Inc(featureName2, timeFrame1, 3)

	recorder := NewRecorderRedis(impressionsCounter, impressionsCountStorage, logging.NewLogger(&logging.LoggerOptions{}))

	err := recorder.SynchronizeImpressionsCount()

	if err != nil {
		t.Error("Should not be an error")
	}
}
