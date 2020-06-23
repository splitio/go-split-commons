package processor

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func wrapEvent(channel string, data string) map[string]interface{} {
	event := make(map[string]interface{})
	event["id"] = "ZlalwoKlXW:0:0"
	event["clientId"] = "pri:MzIxMDYyOTg5MA=="
	event["timestamp"] = 1591996755043
	event["encoding"] = "json"
	event["channel"] = channel
	event["data"] = data

	return event
}

func TestHandleIncomingMessage(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQueue := make(chan dtos.SegmentChangeNotification, 5000)
	splitQueue := make(chan dtos.SplitChangeNotification, 5000)
	splitStorage := mocks.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string) {
			if splitName != "test" {
				t.Error("Wrong splitName passed")
			}
			if defaultTreatment != "some" {
				t.Error("Wrong defaultTreatment passed")
			}
		},
	}
	p, _ := NewProcessor(segmentQueue, splitQueue, splitStorage, make(chan struct{}, 1), logger)

	e0 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "")
	p.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e0 = wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}")
	p.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e1 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}")
	p.HandleIncomingMessage(e1)
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	e2 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}")
	p.HandleIncomingMessage(e2)
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	e3 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}")
	p.HandleIncomingMessage(e3)
	if len(segmentQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}
}

func TestKeepAlive(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelAll})
	keepalive := make(chan struct{}, 1)

	processor, _ := NewProcessor(make(chan dtos.SegmentChangeNotification, 5000), make(chan dtos.SplitChangeNotification, 5000), mocks.MockSplitStorage{},
		keepalive, logger)

	e := make(map[string]interface{})
	e["event"] = "keepalive"
	processor.HandleIncomingMessage(e)

	received := <-keepalive
	if received != struct{}{} {
		t.Error("It should receive keepalive event")
	}
}
