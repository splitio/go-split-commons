package push

import (
	"testing"

	"github.com/splitio/go-toolkit/logging"
)

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

func TestParse(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	parser := NewNotificationParser(logger)

	e0 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"some\",\"splitName\":\"test\"}", nil)
	i0 := parser.Parse(e0)
	if i0.event != "update" {
		t.Error("Unexpected type")
	}

	e1 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}", nil)
	i1 := parser.Parse(e1)
	if i1.event != "update" {
		t.Error("Unexpected type")
	}

	e2 := wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments", "{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"some\"}", nil)
	i2 := parser.Parse(e2)
	if i2.event != "update" {
		t.Error("Unexpected type")
	}

	e3 := make(map[string]interface{})
	e3["message"] = "Token expired"
	e3["code"] = float64(40142)
	e3["statusCode"] = float64(401)
	e3["href"] = "https://help.io/error/40142"
	i3 := parser.Parse(e3)
	if i3.event != "error" {
		t.Error("Unexpected type")
	}

	name := "[meta]occupancy"
	e4 := wrapEvent("[?occupancy=metrics.publishers]control_sec", "{\"metrics\":{\"publishers\":1}}", &name)
	i4 := parser.Parse(e4)
	if i4.event != occupancy {
		t.Error("Unexpected type")
	}
}
