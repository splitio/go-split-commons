package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v10/dtos"
	"github.com/splitio/go-split-commons/v10/push/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestConfigUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	configQueue := make(chan dtos.ConfigChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeConfigCall: func(update *dtos.ConfigChangeUpdate) error {
			atomic.AddInt32(&count, 1)
			switch update.ChangeNumber() {
			case 100:
			case 200:
			default:
				t.Error("Unexpected change number. ", update.ChangeNumber())
			}
			return nil
		},
	}

	configWorker, err := NewConfigUpdateWorker(configQueue, mockSync, logger)
	if err != nil {
		t.Error("It should not return err")
	}
	configWorker.Start()
	configQueue <- *dtos.NewConfigChangeUpdate(dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "config_channel"), 100), nil, nil, nil)

	time.Sleep(1 * time.Second)
	if !configWorker.IsRunning() {
		t.Error("It should be running")
	}
	configWorker.Stop()

	if configWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if c := atomic.LoadInt32(&count); c != 1 {
		t.Error("should have been called once. got: ", c)
	}

	configWorker.Stop()
	configWorker.Stop()
	configWorker.Start()
	configWorker.Start()
	configWorker.Start()

	configQueue <- *dtos.NewConfigChangeUpdate(dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "config_channel"), 200), nil, nil, nil)

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}
	configWorker.Stop()
}

func TestConfigUpdateWorkerQueueTooSmall(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	configQueue := make(chan dtos.ConfigChangeUpdate, 10)
	mockSync := &mocks.LocalSyncMock{}

	_, err := NewConfigUpdateWorker(configQueue, mockSync, logger)
	if err == nil {
		t.Error("It should return an error for an undersized queue")
	}
}
