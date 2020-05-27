package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/logging"
)

func TestEventSyncTask(t *testing.T) {
	var call int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}

	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			atomic.AddInt64(&call, 1)
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.EventDTO{mockedEvent1, mockedEvent2, mockedEvent3}, nil
		},
		EmptyCall: func() bool {
			if call == 1 {
				return false
			}
			return true
		},
	}

	eventMockRecorder := recorderMock.MockEventRecorder{
		RecordCall: func(events []dtos.EventDTO) error {
			if len(events) != 3 {
				t.Error("Wrong length of events passed")
			}
			if events[0].Key != "someKey1" {
				t.Error("Wrong event received")
			}
			if events[1].Key != "someKey2" {
				t.Error("Wrong event received")
			}
			if events[2].Key != "someKey3" {
				t.Error("Wrong event received")
			}
			return nil
		},
	}

	eventTask := NewRecordEventsTask(
		synchronizer.NewEventSynchronizer(
			eventMockStorage,
			eventMockRecorder,
			logger,
		),
		50,
		3,
		logger,
	)

	eventTask.Start()
	if !eventTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}
	eventTask.Stop(true)
	time.Sleep(time.Second * 1)
	if eventTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if call != 2 {
		t.Error("It should call twice for flushing events")
	}
}
