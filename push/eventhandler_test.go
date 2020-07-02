package push

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

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
	parser := NewNotificationParser(logger)

	eventHandler := NewEventHandler(parser, processor, logger)
	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", ""))
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}"))
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}"))
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}"))
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}"))
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
	eventHandler.HandleIncomingMessage(e5)
	if len(segmentQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}
}
