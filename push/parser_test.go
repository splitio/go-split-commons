package push

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestGetInt64(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	parser := NewNotificationParser(nil, nil, logger)
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

	parser := NewNotificationParser(nil, nil, logger)
	v0 := parser.getString(34)
	if v0 != nil {
		t.Error("It should be nil")
	}

	v1 := parser.getString("some")
	if v1 == nil {
		t.Error("It should not be nil")
	}
}

func wrapEvent(channel string, data string, name *string) map[string]interface{} {
	event := make(map[string]interface{})
	event["id"] = "ZlalwoKlXW:0:0"
	event["clientId"] = "pri:MzIxMDYyOTg5MA=="
	event["timestamp"] = 1591996755043
	event["encoding"] = "json"
	event["channel"] = channel
	event["data"] = data
	if name != nil {
		event["name"] = *name
	}

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
	parser := NewNotificationParser(processor, nil, logger)

	e0 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "", nil)
	parser.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e1 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}", nil)
	parser.HandleIncomingMessage(e1)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e2 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}", nil)
	parser.HandleIncomingMessage(e2)
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	e3 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}", nil)
	parser.HandleIncomingMessage(e3)
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	e4 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}", nil)
	parser.HandleIncomingMessage(e4)
	if len(segmentQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}
}

func TestHandleIncomingMessageError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelDebug})
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
	parser := NewNotificationParser(processor, nil, logger)

	e0 := make(map[string]interface{})
	e0["message"] = "Token expired"
	e0["code"] = 40142
	e0["statusCode"] = 401
	e0["href"] = "https://help.io/error/40142"
	parser.HandleIncomingMessage(e0)

	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}
}

func TestOccupancy(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	processor, err := NewProcessor(make(chan dtos.SegmentChangeNotification, 5000), make(chan dtos.SplitChangeNotification, 5000), mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return err")
	}
	keeper := NewKeeper(make(chan int, 1))
	parser := NewNotificationParser(processor, keeper, logger)

	name := "[meta]occupancy"
	e0 := wrapEvent("[?occupancy=metrics.publishers]control_sec", "{\"metrics\":{\"publishers\":1}}", &name)
	parser.HandleIncomingMessage(e0)

	e1 := wrapEvent("[?occupancy=metrics.publishers]control_pri", "{\"metrics\":{\"publishers\":2}}", &name)
	parser.HandleIncomingMessage(e1)

	if keeper.activeRegion != "us-east-1" {
		t.Error("Unexpected activeRegion")
	}
	if len(keeper.managers) != 2 {
		t.Error("Wrong amount of managers")
	}
	if keeper.managers["control_sec"] != 1 {
		t.Error("Unexpected amount of publishers")
	}
	if keeper.managers["control_pri"] != 2 {
		t.Error("Unexpected amount of publishers")
	}
}
