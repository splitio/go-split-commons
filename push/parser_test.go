package push

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestGetInt64(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	parser := NewNotificationParser(nil, logger)
	v0 := parser.getInt64("some")
	if v0 != nil {
		t.Error("It should be nil")
	}

	var number interface{}
	number = int64(123456789)
	v1 := parser.getInt64(number)
	if v1 == nil {
		t.Error("It should not be nil")
	}
}

func TestGetString(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	parser := NewNotificationParser(nil, logger)
	v0 := parser.getString(34)
	if v0 != nil {
		t.Error("It should be nil")
	}

	v1 := parser.getString("some")
	if v1 == nil {
		t.Error("It should not be nil")
	}
}

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
	mockSplitStorage := mocks.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string) {
			if splitName != "test" {
				t.Error("Wrong splitName passed")
			}
			if defaultTreatment != "some" {
				t.Error("Wrong defaultTreatment passed")
			}
		},
	}

	processor, err := NewProcessor(segmentQueue, splitQueue, mockSplitStorage, logger)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(processor, logger)

	e0 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "")
	parser.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e1 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}")
	parser.HandleIncomingMessage(e1)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e2 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}")
	parser.HandleIncomingMessage(e2)
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	e3 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}")
	parser.HandleIncomingMessage(e3)
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	e4 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}")
	parser.HandleIncomingMessage(e4)
	if len(segmentQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	e5 := make(map[string]interface{})
	e5["message"] = "Token expired"
	e5["code"] = 40142
	e5["statusCode"] = 401
	e5["href"] = "https://help.io/error/40142"
	parser.HandleIncomingMessage(e5)
}
