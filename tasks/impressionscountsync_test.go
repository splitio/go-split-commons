package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/provisional"
	"github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker/impressionscount"
	"github.com/splitio/go-toolkit/logging"
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
