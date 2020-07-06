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
	publishers := make(chan int, 1)
	keeper := NewKeeper(publishers)

	eventHandler := NewEventHandler(keeper, parser, processor, logger)
	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "", nil))
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}", nil))
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}", nil))
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}", nil))
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	eventHandler.HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}", nil))
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

	name := "[meta]occupancy"
	e6 := wrapEvent("[?occupancy=metrics.publishers]control_sec", "{\"metrics\":{\"publishers\":1}}", &name)
	eventHandler.HandleIncomingMessage(e6)

	e7 := wrapEvent("[?occupancy=metrics.publishers]control_pri", "{\"metrics\":{\"publishers\":2}}", &name)
	eventHandler.HandleIncomingMessage(e7)

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
