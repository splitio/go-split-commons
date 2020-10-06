package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-split-commons/v2/provisional"
	"github.com/splitio/go-split-commons/v2/service/mocks"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/impressionscount"
	"github.com/splitio/go-toolkit/v3/logging"
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

	impManager := provisional.NewImpressionsCounter()
	impressionsCountTask := NewRecordImpressionsCountTask(impressionscount.NewRecorderSingle(
		impManager, impressionMockRecorder, dtos.Metadata{}, logger,
	), logger)

	impressionsCountTask.Start()
	if !impressionsCountTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}
	impressionsCountTask.Stop(true)
	time.Sleep(time.Millisecond * 300)
	if impressionsCountTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if call != 2 {
		t.Error("It should call twice for flushing impressions")
	}
}
