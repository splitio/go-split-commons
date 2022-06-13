package impressionscount

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/provisional/strategy"
	"github.com/splitio/go-split-commons/v4/service/mocks"
	st "github.com/splitio/go-split-commons/v4/storage/mocks"
	"github.com/splitio/go-split-commons/v4/telemetry"
	"github.com/splitio/go-split-commons/v4/util"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestImpressionsCountRecorderError(t *testing.T) {
	impressionMockRecorder := mocks.MockImpressionRecorder{
		RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
			return &dtos.HTTPError{Code: 500, Message: "some"}
		},
	}
	telemetryMockStorage := st.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.ImpressionCountSync {
				t.Error("It should be impressions")
			}
			if status != 500 {
				t.Error("Status should be 500")
			}
		},
	}

	impressionsCountSync := NewRecorderSingle(strategy.NewImpressionsCounter(), impressionMockRecorder, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage)

	err := impressionsCountSync.SynchronizeImpressionsCount()
	if err == nil {
		t.Error("It should return err")
	}
}

func TestImpressionsCountRecorder(t *testing.T) {
	before := time.Now().UTC()
	now := time.Now().UnixNano()
	nextHour := time.Now().Add(1 * time.Hour).UnixNano()
	impressionMockRecorder := mocks.MockImpressionRecorder{
		RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
			if len(pf.PerFeature) != 3 {
				t.Error("It should be 3")
			}
			for _, x := range pf.PerFeature {
				switch x.TimeFrame {
				case util.TruncateTimeFrame(now):
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
				case util.TruncateTimeFrame(nextHour):
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
	telemetryMockStorage := st.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.ImpressionCountSync {
				t.Error("Resource should be impressionsCount")
			}
			if tm.Before(before) {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.ImpressionCountSync {
				t.Error("Resource should be impresisonsCount")
			}
		},
	}

	impCounter := strategy.NewImpressionsCounter()
	impressionsCountSync := NewRecorderSingle(impCounter, impressionMockRecorder, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage)

	impCounter.Inc("some", now, 1)
	impCounter.Inc("another", now+1, 1)
	impCounter.Inc("some", now+2, 1)
	impCounter.Inc("some", nextHour, 4)

	err := impressionsCountSync.SynchronizeImpressionsCount()
	if err != nil {
		t.Error("It should not return err")
	}
}
