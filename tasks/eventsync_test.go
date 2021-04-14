package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/telemetry"
	"github.com/splitio/go-toolkit/logging"
)

func TestEventSyncTask(t *testing.T) {
	call := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}

	eventMockStorage := mocks.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			call++
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.EventDTO{mockedEvent1, mockedEvent2, mockedEvent3}, nil
		},
		EmptyCall: func() bool { return call != 1 },
	}

	eventMockRecorder := recorderMock.MockEventRecorder{
		RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
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

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.EventSync {
				t.Error("Resource should be events")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.EventSync {
				t.Error("Resource should be events")
			}
		},
	}

	eventTask := NewRecordEventsTask(
		event.NewEventRecorderSingle(eventMockStorage, eventMockRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		50,
		1,
		logger,
	)

	eventTask.Start()
	time.Sleep(2 * time.Second)
	if !eventTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}
	eventTask.Stop(true)
	time.Sleep(time.Millisecond * 300)
	if eventTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if call < 2 {
		t.Error("It should call twice for flushing events")
	}
}

func TestEventSyncTaskMultiple(t *testing.T) {
	var call int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Timestamp: 123456789, TrafficTypeName: "someTraffic"}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Timestamp: 123456789, TrafficTypeName: "someTraffic"}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Timestamp: 123456789, TrafficTypeName: "someTraffic"}

	eventMockStorage := mocks.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			atomic.AddInt64(&call, 1)
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.EventDTO{mockedEvent1, mockedEvent2, mockedEvent3}, nil
		},
		EmptyCall: func() bool {
			return false
		},
	}

	eventMockRecorder := recorderMock.MockEventRecorder{
		RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
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

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.EventSync {
				t.Error("Resource should be events")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.EventSync {
				t.Error("Resource should be events")
			}
		},
	}

	eventTask := NewRecordEventsTasks(
		event.NewEventRecorderSingle(eventMockStorage, eventMockRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		50,
		2,
		logger,
		3,
	)

	eventTask.Start()
	time.Sleep(3 * time.Second)
	if !eventTask.IsRunning() {
		t.Error("Task recorder task should be running")
	}
	eventTask.Stop(true)
	if eventTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	time.Sleep(1 * time.Second)
	if x := atomic.LoadInt64(&call); x != 3 {
		t.Error("It should call three times for flushing events. Was:", x)
	}
}
