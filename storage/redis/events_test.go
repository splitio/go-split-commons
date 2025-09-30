package redis

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
	"github.com/splitio/go-toolkit/v5/redis/mocks"
)

func TestPushEventsError(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.events"
	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 0, errors.New("Some Error") },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	eventStorage := NewEventsStorage(mockPrefixedClient, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}))

	event := dtos.EventDTO{
		EventTypeID:     "someId",
		Key:             "someKey",
		Properties:      make(map[string]interface{}),
		Timestamp:       123456789,
		TrafficTypeName: "someTraffic",
		Value:           0,
	}

	err := eventStorage.Push(event, 0)
	if err == nil {
		t.Error("It should return error")
	}
}

func TestPushEvents(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.events"
	mockedRedisClient := mocks.MockClient{
		RPushCall: func(key string, values ...interface{}) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if len(values) != 1 {
				t.Error("It should sent one event", len(values))
			}
			return &mocks.MockResultOutput{
				ResultCall: func() (int64, error) { return 1, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	eventStorage := NewEventsStorage(mockPrefixedClient, dtos.Metadata{}, logging.NewLogger(&logging.LoggerOptions{}))

	event := dtos.EventDTO{
		EventTypeID:     "someId",
		Key:             "someKey",
		Properties:      make(map[string]interface{}),
		Timestamp:       123456789,
		TrafficTypeName: "someTraffic",
		Value:           0,
	}

	err := eventStorage.Push(event, 0)
	if err != nil {
		t.Error("It should not return error")
	}
}

func marshalEvent(event dtos.QueueStoredEventDTO) string {
	json, _ := json.Marshal(event)
	return string(json)
}

func TestPopNEventsWithMetadata(t *testing.T) {
	expectedKey := "someprefix.SPLITIO.events"

	metadata := dtos.Metadata{
		SDKVersion:  "go-test",
		MachineIP:   "1.2.3.4",
		MachineName: "test",
	}
	event := dtos.EventDTO{
		EventTypeID:     "someId",
		Key:             "someKey",
		Properties:      make(map[string]interface{}),
		Timestamp:       123456789,
		TrafficTypeName: "someTraffic",
		Value:           0,
	}
	queueEvent := dtos.QueueStoredEventDTO{
		Metadata: metadata,
		Event:    event,
	}
	queueEvent2 := dtos.QueueStoredEventDTO{
		Metadata: dtos.Metadata{
			SDKVersion:  "go-test-2",
			MachineIP:   "1.2.3.4",
			MachineName: "test",
		},
		Event: event,
	}

	mockedRedisClient := mocks.MockClient{
		LRangeCall: func(key string, start int64, stop int64) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if start != 0 {
				t.Errorf("Unexpected start. Expected: %d Actual: %d", 0, start)
			}
			if stop != 9998 {
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", 9998, stop)
			}
			return &mocks.MockResultOutput{
				MultiCall: func() ([]string, error) {
					return []string{marshalEvent(queueEvent), marshalEvent(queueEvent), marshalEvent(queueEvent2)}, nil
				},
			}
		},
		LTrimCall: func(key string, start, stop int64) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			if start != 3 {
				t.Errorf("Unexpected start. Expected: %d Actual: %d", 3, start)
			}
			if stop != -1 {
				t.Errorf("Unexpected stop. Expected: %d Actual: %d", -1, stop)
			}
			return &mocks.MockResultOutput{
				ErrCall: func() error { return nil },
			}
		},
		LLenCall: func(key string) redis.Result {
			if key != expectedKey {
				t.Errorf("Unexpected key. Expected: %s Actual: %s", expectedKey, key)
			}
			return &mocks.MockResultOutput{
				ErrCall:    func() error { return nil },
				ResultCall: func() (int64, error) { return 3, nil },
			}
		},
	}

	mockPrefixedClient, _ := redis.NewPrefixedRedisClient(&mockedRedisClient, "someprefix")
	eventStorage := NewEventsStorage(mockPrefixedClient, metadata, logging.NewLogger(&logging.LoggerOptions{}))

	storedEvents, err := eventStorage.PopNWithMetadata(3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(storedEvents) != 3 {
		t.Error("Unexpected returned events")
	}
}
