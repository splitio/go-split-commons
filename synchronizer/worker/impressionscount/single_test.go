package impressionscount

import (
	"errors"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/provisional"
	"github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

func TestImpressionsCountRecorderError(t *testing.T) {
	impressionMockRecorder := mocks.MockImpressionRecorder{
		RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
			return errors.New("some")
		},
	}

	impressionsCountSync := NewRecorderSingle(
		provisional.NewImpressionsCounter(),
		impressionMockRecorder,
		dtos.Metadata{},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := impressionsCountSync.SynchronizeImpressionsCount()
	if err == nil {
		t.Error("It should return err")
	}
}

func TestImpressionsCountRecorder(t *testing.T) {
	now := time.Now().UnixNano()
	nextHour := time.Now().Add(1 * time.Hour).UnixNano()
	impressionMockRecorder := mocks.MockImpressionRecorder{
		RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
			if len(pf.PerFeature) != 3 {
				t.Error("It should be 3")
			}
			if pf.PerFeature[0].FeatureName != "some" {
				t.Error("It should be some")
			}
			if pf.PerFeature[0].RawCount != 2 {
				t.Error("It should be 2")
			}
			if pf.PerFeature[0].TimeFrame != util.TruncateTimeFrame(now/int64(time.Millisecond)) {
				t.Error("Wrong truncated timeFrame")
			}
			if pf.PerFeature[1].FeatureName != "another" {
				t.Error("It should be some")
			}
			if pf.PerFeature[1].RawCount != 1 {
				t.Error("It should be 1")
			}
			if pf.PerFeature[1].TimeFrame != util.TruncateTimeFrame(now/int64(time.Millisecond)) {
				t.Error("Wrong truncated timeFrame")
			}
			if pf.PerFeature[2].FeatureName != "some" {
				t.Error("It should be some")
			}
			if pf.PerFeature[2].RawCount != 4 {
				t.Error("It should be 4")
			}
			if pf.PerFeature[2].TimeFrame != util.TruncateTimeFrame(nextHour/int64(time.Millisecond)) {
				t.Error("Wrong truncated timeFrame")
			}

			return nil
		},
	}

	impCounter := provisional.NewImpressionsCounter()
	impressionsCountSync := NewRecorderSingle(
		impCounter,
		impressionMockRecorder,
		dtos.Metadata{},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	impCounter.Inc("some", now, 1)
	impCounter.Inc("another", now+1, 1)
	impCounter.Inc("some", now+2, 1)
	impCounter.Inc("some", nextHour, 4)

	err := impressionsCountSync.SynchronizeImpressionsCount()
	if err != nil {
		t.Error("It should not return err")
	}
}
