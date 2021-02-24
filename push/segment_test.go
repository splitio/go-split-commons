package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/push/mocks"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQUeue := make(chan SegmentChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSegmentCall: func(segmentName string, till *int64, cache bool) error {
			atomic.AddInt32(&count, 1)
			if segmentName != "some" {
				t.Error("Unexpected segment name")
			}
			if *till != 123456789 && *till != 223456789 {
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

	segmentWorker.Stop()
	segmentWorker.Stop()
	segmentWorker.Stop()
	segmentWorker.Stop()
	segmentWorker.Stop()
	segmentWorker.Start()
	segmentWorker.Start()
	segmentWorker.Start()
	segmentWorker.Start()
	segmentWorker.Start()
	segmentWorker.Start()
	segmentQUeue <- SegmentChangeUpdate{
		segmentName: "some",
		BaseUpdate:  BaseUpdate{BaseMessage: BaseMessage{channel: "some"}, changeNumber: 223456789},
	}

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}

}
