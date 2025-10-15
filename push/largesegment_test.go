package push

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/push/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLargeSegmentUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	lsQueue := make(chan dtos.LargeSegmentChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeLargeSegmentUpdateCall: func(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error {
			atomic.AddInt32(&count, 1)
			switch lsRFDResponseDTO.Name {
			case "ls1":
				if lsRFDResponseDTO.ChangeNumber != 100 {
					t.Error("Unexpected CN for ls1. ", lsRFDResponseDTO.ChangeNumber)
				}
			case "ls2":
				if lsRFDResponseDTO.ChangeNumber != 200 {
					t.Error("Unexpected CN for ls2. ", lsRFDResponseDTO.ChangeNumber)
				}
			default:
				t.Error("Unexpected name")
			}

			return nil
		},
	}

	ls := []dtos.LargeSegmentRFDResponseDTO{
		{
			Name:             "ls1",
			NotificationType: dtos.UpdateTypeLargeSegmentChange,
			SpecVersion:      "1.0",
			RFD:              &dtos.RFD{},
		},
	}

	segmentWorker, _ := NewLargeSegmentUpdateWorker(lsQueue, mockSync, logger)
	segmentWorker.Start()
	lsQueue <- *dtos.NewLargeSegmentChangeUpdate(dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "ls_channel"), 100), ls)

	time.Sleep(1 * time.Second)
	if !segmentWorker.IsRunning() {
		t.Error("It should be running")
	}
	segmentWorker.Stop()

	if segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if c := atomic.LoadInt32(&count); c != 1 {
		t.Error("should have been called once. got: ", c)
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
	ls = []dtos.LargeSegmentRFDResponseDTO{
		{
			Name:             "ls2",
			NotificationType: dtos.UpdateTypeLargeSegmentChange,
			SpecVersion:      "1.0",
			RFD:              &dtos.RFD{},
			ChangeNumber:     201,
		},
	}
	lsQueue <- *dtos.NewLargeSegmentChangeUpdate(dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "ls_channel"), 200), ls)

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}
}
