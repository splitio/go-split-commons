package push

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestHandleIncomingMessageError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelDebug})
	segmentQueue := make(chan dtos.SegmentChangeNotification, 5000)
	splitQueue := make(chan dtos.SplitChangeNotification, 5000)
	keepalive := make(chan struct{}, 1)
	publishers := make(chan int, 1)
	keeper := NewKeeper(publishers)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, keepalive, *keeper, logger)
	if err != nil {
		t.Error("It should not return err")
	}

	e := make(map[string]interface{})
	e["message"] = "Token expired"
	e["code"] = 40142
	e["statusCode"] = 401
	e["href"] = "https://help.io/error/40142"
	processor.HandleIncomingMessage(e)

	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(keeper.managers) != 0 {
		t.Error("It should not update keepers")
	}
	if keeper.last != nil {
		t.Error("It should be nil")
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
	p, _ := NewProcessor(segmentQueue, splitQueue, splitStorage, make(chan struct{}, 1), *NewKeeper(make(chan int, 1)), logger)

	e0 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "", nil)
	p.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e0 = wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"WRONG\"}", nil)
	p.HandleIncomingMessage(e0)
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	e1 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}", nil)
	p.HandleIncomingMessage(e1)
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}

	e2 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}", nil)
	p.HandleIncomingMessage(e2)
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	e3 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}", nil)
	p.HandleIncomingMessage(e3)
	if len(segmentQueue) != 1 {
		t.Error("It should be 1")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}
}

func TestKeepAlive(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	keepalive := make(chan struct{}, 1)

	processor, _ := NewProcessor(make(chan dtos.SegmentChangeNotification, 5000), make(chan dtos.SplitChangeNotification, 5000), mocks.MockSplitStorage{},
		keepalive, *NewKeeper(make(chan int, 1)), logger)

	e := make(map[string]interface{})
	e["event"] = "keepalive"
	processor.HandleIncomingMessage(e)

	received := <-keepalive
	if received != struct{}{} {
		t.Error("It should receive keepalive event")
	}
}

func TestOccupancy(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	keepalive := make(chan struct{}, 1)
	keeper := NewKeeper(make(chan int, 1))

	processor, _ := NewProcessor(make(chan dtos.SegmentChangeNotification, 5000), make(chan dtos.SplitChangeNotification, 5000), mocks.MockSplitStorage{},
		keepalive, *keeper, logger)

	name := "[meta]occupancy"
	e0 := wrapEvent("[?occupancy=metrics.publishers]control_sec", "{\"metrics\":{\"publishers\":1}}", &name)
	processor.HandleIncomingMessage(e0)

	e1 := wrapEvent("[?occupancy=metrics.publishers]control_pri", "{\"metrics\":{\"publishers\":2}}", &name)
	processor.HandleIncomingMessage(e1)

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
