package push

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/push/mocks"
	"github.com/splitio/go-toolkit/v3/logging"
)

func TestSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQUeue := make(chan SegmentChangeUpdate, 5000)

	mockSync := &mocks.LocalSyncMock{
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

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQUeue, mockSync, logger)
	segmentWorker.Start()
	segmentQUeue <- SegmentChangeUpdate{
		segmentName: "some",
		BaseUpdate:  BaseUpdate{BaseMessage: BaseMessage{channel: "some"}, changeNumber: 123456789},
	}

	if !segmentWorker.IsRunning() {
		t.Error("It should be running")
	}
	time.Sleep(1 * time.Second)
	segmentWorker.Stop()

	if segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
}
