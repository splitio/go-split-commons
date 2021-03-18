package event

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"errors"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service/api"
	recorderMock "github.com/splitio/go-split-commons/v3/service/mocks"
	"github.com/splitio/go-split-commons/v3/storage/inmemory/mutexqueue"
	storageMock "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSynhronizeEventError(t *testing.T) {
	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.EventDTO, 0), errors.New("Some")
		},
	}

	eventMockRecorder := recorderMock.MockEventRecorder{}

	eventSync := NewEventRecorderSingle(
		eventMockStorage,
		eventMockRecorder,
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := eventSync.SynchronizeEvents(50)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestSynhronizeEventWithNoEvents(t *testing.T) {
	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.EventDTO, 0), nil
		},
	}

	eventMockRecorder := recorderMock.MockEventRecorder{
		RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
			t.Error("It should not be called")
			return nil
		},
	}

	eventSync := NewEventRecorderSingle(
		eventMockStorage,
		eventMockRecorder,
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := eventSync.SynchronizeEvents(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestSynhronizeEvent(t *testing.T) {
	event1 := dtos.EventDTO{
		EventTypeID:     "someId",
		Key:             "someKey",
		Properties:      make(map[string]interface{}),
		Timestamp:       123456789,
		TrafficTypeName: "someTraffic",
		Value:           nil,
	}
	event2 := dtos.EventDTO{
		EventTypeID:     "someId2",
		Key:             "someKey2",
		Properties:      make(map[string]interface{}),
		Timestamp:       123456789,
		TrafficTypeName: "someTraffic",
		Value:           nil,
	}

	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.EventDTO{event1, event2}, nil
		},
	}

	eventMockRecorder := recorderMock.MockEventRecorder{
		RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
			if len(events) != 2 {
				t.Error("Wrong length of events passed")
			}
			if events[0].Key != "someKey" {
				t.Error("Wrong event received")
			}
			if events[1].Key != "someKey2" {
				t.Error("Wrong event received")
			}
			return nil
		},
	}

	eventSync := NewEventRecorderSingle(
		eventMockStorage,
		eventMockRecorder,
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := eventSync.SynchronizeEvents(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestSynhronizeEventSync(t *testing.T) {
	var requestReceived int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/bulk" || r.Method != "POST" {
			t.Error("Invalid request. Should be POST to /events/bulk")
		}
		atomic.AddInt64(&requestReceived, 1)

		body, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Error("Error reading body")
			return
		}

		var events []dtos.EventDTO
		err = json.Unmarshal(body, &events)
		if err != nil {
			t.Errorf("Error parsing json: %s", err)
			return
		}

		if len(events) != 3 {
			t.Error("Incorrect number of events")
			return
		}

		if events[0].Key != "someKey1" {
			t.Error("Wrong event sent")
		}
		if events[1].Key != "someKey2" {
			t.Error("Wrong event sent")
		}
		if events[2].Key != "someKey3" {
			t.Error("Wrong event sent")
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	eventRecorder := api.NewHTTPEventsRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}

	eventStorage := mutexqueue.NewMQEventsStorage(100, nil, logger)
	eventStorage.Push(mockedEvent1, 100)
	eventStorage.Push(mockedEvent2, 100)
	eventStorage.Push(mockedEvent3, 100)

	eventSync := NewEventRecorderSingle(
		eventStorage,
		eventRecorder,
		logger,
		dtos.Metadata{},
	)

	eventSync.SynchronizeEvents(5)

	if requestReceived != 1 {
		t.Error("It should call once")
	}
}
