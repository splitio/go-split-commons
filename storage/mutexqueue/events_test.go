package mutexqueue

import (
	"strconv"
	"testing"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestMSEventsStorage(t *testing.T) {
	logger := logging.NewLogger(nil)

	e0 := dtos.EventDTO{EventTypeID: "ET0", Key: "K0", Timestamp: 0, TrafficTypeName: "TTN0", Value: 0.0}
	e1 := dtos.EventDTO{EventTypeID: "ET1", Key: "K1", Timestamp: 1, TrafficTypeName: "TTN1", Value: 0.1}
	e2 := dtos.EventDTO{EventTypeID: "ET2", Key: "K2", Timestamp: 2, TrafficTypeName: "TTN2", Value: 0.2}
	e3 := dtos.EventDTO{EventTypeID: "ET3", Key: "K3", Timestamp: 3, TrafficTypeName: "TTN3", Value: 0.3}
	e4 := dtos.EventDTO{EventTypeID: "ET4", Key: "K4", Timestamp: 4, TrafficTypeName: "TTN4", Value: 0.4}
	e5 := dtos.EventDTO{EventTypeID: "ET5", Key: "K5", Timestamp: 5, TrafficTypeName: "TTN5", Value: 0.5}
	e6 := dtos.EventDTO{EventTypeID: "ET6", Key: "K6", Timestamp: 6, TrafficTypeName: "TTN6", Value: 0.6}
	e7 := dtos.EventDTO{EventTypeID: "ET7", Key: "K7", Timestamp: 7, TrafficTypeName: "TTN7", Value: 0.7}
	e8 := dtos.EventDTO{EventTypeID: "ET8", Key: "K8", Timestamp: 8, TrafficTypeName: "TTN8", Value: 0.8}
	e9 := dtos.EventDTO{EventTypeID: "ET9", Key: "K9", Timestamp: 9, TrafficTypeName: "TTN9", Value: 0.9}

	isFull := make(chan string, 1)
	queueSize := 20
	queue := NewMQEventsStorage(queueSize, isFull, logger)

	if queue.Count() != 0 {
		t.Error("Queue count error")
	}
	if !queue.Empty() {
		t.Error("Queue empty error")
	}

	// Push from back to front
	queue.Push(e0, 1000)
	queue.Push(e1, 1000)
	queue.Push(e2, 1000)
	queue.Push(e3, 1000)
	queue.Push(e4, 1000)

	if queue.Count() != 5 {
		t.Error("Queue count error")
	}
	if queue.Empty() {
		t.Error("Queue empty error")
	}

	queue.Push(e5, 1000)
	queue.Push(e6, 1000)
	queue.Push(e7, 1000)
	queue.Push(e8, 1000)
	queue.Push(e9, 1000)

	events, _ := queue.PopN(25)

	for i := 0; i < len(events); i++ {
		if events[i].EventTypeID != "ET"+strconv.Itoa(i) {
			t.Error("EventTypeID error")
		}

		if events[i].Key != "K"+strconv.Itoa(i) {
			t.Error("Key error")
		}

		if events[i].TrafficTypeName != "TTN"+strconv.Itoa(i) {
			t.Error("TrafficTypeName error")
		}
		if events[i].Timestamp != int64(i) {
			t.Error("Timestamp error")
		}
		if events[i].Value != float64(i)/float64(10) {
			t.Error("Value error")
		}
	}

}

func TestMSEventsStorageMaxSize(t *testing.T) {
	logger := logging.NewLogger(nil)

	e := dtos.EventDTO{EventTypeID: "ET0", Key: "K0", Timestamp: 0, TrafficTypeName: "TTN0", Value: 0.0}

	isFull := make(chan string, 1)
	maxSize := 10
	queue := NewMQEventsStorage(maxSize, isFull, logger)

	select {
	case <-isFull:
		t.Error("Signal sent when it shouldn't have!")
	default:
	}

	for i := 0; i < maxSize; i++ {
		err := queue.Push(e, 1000)
		if err != nil {
			t.Error("Error pushing element into queue")
		}
	}

	err := queue.Push(e, 1000)
	if err != ErrorMaxSizeReached {
		t.Error("Error reporting max size reached")
	}

	select {
	case <-isFull:
	default:
		t.Error("Signal sent when it shouldn't have!")
	}
}

func TestMSEventsStorageMaxSizeInBytes(t *testing.T) {
	logger := logging.NewLogger(nil)

	e := dtos.EventDTO{
		EventTypeID:     "ET0",
		Key:             "K0",
		Timestamp:       0,
		TrafficTypeName: "TTN0",
		Value:           0.0,
	}
	isFull := make(chan string, 1)
	maxSize := 9999999 // Huge number so that it explodes only because of size in bytes
	queue := NewMQEventsStorage(maxSize, isFull, logger)

	select {
	case <-isFull:
		t.Error("Signal sent when it shouldn't have!")
	default:
	}

	for i := 0; i < 159; i++ {
		queue.Push(e, 32768)
	}

	queue.Push(e, 32768)
	queue.Push(e, 32768)

	select {
	case <-isFull:
	default:
		t.Error("Signal sent when it shouldn't have!")
	}
}
