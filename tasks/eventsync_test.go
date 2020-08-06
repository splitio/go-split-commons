package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-toolkit/logging"
)

func TestEventSyncTask(t *testing.T) {
	call := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}

	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			call++
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

	eventTask := NewRecordEventsTask(
		event.NewEventRecorderSingle(
			eventMockStorage,
			eventMockRecorder,
			storage.NewMetricWrapper(storageMock.MockMetricStorage{
				IncCounterCall: func(key string) {
					if key != "events.status.200" && key != "backend::request.ok" {
						t.Error("Unexpected counter key to increase")
					}
				},
				IncLatencyCall: func(metricName string, index int) {
					if metricName != "events.time" && metricName != "backend::/api/events/bulk" {
						t.Error("Unexpected latency key to track")
					}
				},
			}, nil, nil),
			logger,
			dtos.Metadata{},
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
	time.Sleep(time.Millisecond * 300)
	if eventTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if call != 2 {
		t.Error("It should call twice for flushing events")
	}
}

func TestEventSyncTaskMultiple(t *testing.T) {
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

	eventTask := NewRecordEventsTasks(
		event.NewEventRecorderSingle(
			eventMockStorage,
			eventMockRecorder,
			storage.NewMetricWrapper(storageMock.MockMetricStorage{
				IncCounterCall: func(key string) {
					if key != "events.status.200" && key != "backend::request.ok" {
						t.Error("Unexpected counter key to increase")
					}
				},
				IncLatencyCall: func(metricName string, index int) {
					if metricName != "events.time" && metricName != "backend::/api/events/bulk" {
						t.Error("Unexpected latency key to track")
					}
				},
			}, nil, nil),
			logger,
			dtos.Metadata{},
		),
		50,
		50,
		logger,
		3,
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

	if atomic.LoadInt64(&call) != 3 {
		t.Error("It should call twice for flushing events")
	}
}
