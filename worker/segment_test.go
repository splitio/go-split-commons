package worker

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/synchronizer/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelDebug})
	segmentQUeue := make(chan dtos.SegmentChangeNotification, 5000)
	mockedSync := mocks.MockSynchronizer{
		SynchronizeSegmentCall: func(segmentName string, till *int64) error {
			if segmentName != "some" {
				t.Error("Unexpected segment name")
			}
			if *till != 123456789 {
				t.Error("Unexpected till")
			}
			return nil
		},
	}

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQUeue, mockedSync, logger)
	segmentWorker.Start()
	segmentQUeue <- dtos.NewSegmentChangeNotification("some", 123456789, "some")

	go func() {
		if !segmentWorker.IsRunning() {
			t.Error("It should be running")
		}
		time.Sleep(1 * time.Second)
		segmentWorker.Stop()
	}()

	time.Sleep(2 * time.Second)
	if segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
}
