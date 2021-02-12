package push

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/push/mocks"
	"github.com/splitio/go-toolkit/v3/logging"
)

func TestSplitUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			if *till != 123456789 {
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
}
