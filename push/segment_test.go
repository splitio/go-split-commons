package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/push/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQueue := make(chan dtos.SegmentChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSegmentCall: func(segmentName string, till *int64) error {
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

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, mockSync, logger)
	segmentWorker.Start()
	segmentQueue <- *dtos.NewSegmentChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 123456789),
		"some",
	)

	time.Sleep(1 * time.Second)
	if !segmentWorker.IsRunning() {
		t.Error("It should be running")
	}
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
	segmentQueue <- *dtos.NewSegmentChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 223456789),
		"some",
	)

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}

}
