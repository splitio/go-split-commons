package push

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

func TestSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQUeue := make(chan dtos.SegmentChangeNotification, 5000)

	segmentHandler := func(segmentName string, till *int64) error {
		if segmentName != "some" {
			t.Error("Unexpected segment name")
		}
		if *till != 123456789 {
			t.Error("Unexpected till")
		}
		return nil
	}

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQUeue, segmentHandler, logger)
	segmentWorker.Start()
	segmentQUeue <- dtos.NewSegmentChangeNotification("some", 123456789, "some")

	if !segmentWorker.IsRunning() {
		t.Error("It should be running")
	}
	time.Sleep(1 * time.Second)
	segmentWorker.Stop()

	if segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
}
