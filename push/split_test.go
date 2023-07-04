package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/push/mocks"
	storageMocks "github.com/splitio/go-split-commons/v5/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSplitUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeFeatureFlagsCall: func(ffChange *dtos.SplitChangeUpdate) error {
			atomic.AddInt32(&count, 1)
			switch atomic.LoadInt32(&count) {
			case 1:
				if ffChange.ChangeNumber() != 123456789 {
					t.Error("Unexpected passed changeNumber")
				}
			case 2:
				if ffChange.ChangeNumber() != 223456789 {
					t.Error("Unexpected passed changeNumber")
				}
			default:
				t.Error("Unexpected passed changeNumber")
			}

			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 123456789), nil, nil,
	)

	time.Sleep(1 * time.Second)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}
	splitWorker.Stop()

	if splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}

	splitWorker.Start()
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 223456789), nil, nil,
	)

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}
}
