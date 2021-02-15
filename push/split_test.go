package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/push/mocks"
	"github.com/splitio/go-toolkit/v3/logging"
)

func TestSplitUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)
			if *till != 123456789 && *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger)
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 123456789}}

	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}
	time.Sleep(1 * time.Second)
	splitWorker.Stop()

	if splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}

	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Start()
	splitWorker.Start()
	splitWorker.Start()
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}}

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}

}
