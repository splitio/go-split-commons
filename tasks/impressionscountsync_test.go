package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/provisional"
	"github.com/splitio/go-split-commons/v3/service/mocks"
	st "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/impressionscount"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestImpressionCountSyncTask(t *testing.T) {
	call := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})
	impressionMockRecorder := mocks.MockImpressionRecorder{
		RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
			call++
			return nil
		},
	}
	telemetryMockStorage := st.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.ImpressionCountSync {
				t.Error("Resource should be impressionsCount")
			}
		},
	}

	impManager := provisional.NewImpressionsCounter()
	impressionsCountTask := NewRecordImpressionsCountTask(impressionscount.NewRecorderSingle(impManager, impressionMockRecorder, dtos.Metadata{}, logger, telemetryMockStorage), logger)

	impressionsCountTask.Start()
	time.Sleep(1 * time.Second)
	if !impressionsCountTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}
	impressionsCountTask.Stop(true)
	if impressionsCountTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if call != 1 {
		t.Error("It should call twice for flushing impressions")
	}
}
