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
			for _, x := range pf.PerFeature {
				switch x.TimeFrame {
				case util.TruncateTimeFrame(now / int64(time.Millisecond)):
					switch x.FeatureName {
					case "some":
						if x.RawCount != 2 {
							t.Error("It should be 2")
						}
					case "another":
						if x.RawCount != 1 {
							t.Error("It should be 1")
						}
					default:
						t.Error("Unexpected incomming feature")
					}
				case util.TruncateTimeFrame(nextHour / int64(time.Millisecond)):
					if x.FeatureName != "some" {
						t.Error("It should be some")
					}
					if x.RawCount != 4 {
						t.Error("It should be 4")
					}
				default:
					t.Error("Unexpected incomming feature")
				}
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
