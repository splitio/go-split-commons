package sse

import (
	"testing"
)

/*
{
   "id":"St40RHV9u9:0:0",
   "clientId":"pri:NTIxMjUxMjI0",
   "timestamp":1591988399435,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
   "data":"{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"PUSH_SEGMENT_CSV_MULTIPLE\"}"
}
{
   "id":"gFp3nSE582:0:0",
   "clientId":"pri:MzIxMDYyOTg5MA==",
   "timestamp":1591996685999,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
   "data":"{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}"
}
{
   "id":"ZlalwoKlXW:0:0",
   "clientId":"pri:MzIxMDYyOTg5MA==",
   "timestamp":1591996755043,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
   "data":"{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"INITIALIZATION_STEP\",\"splitName\":\"PUSH_TEST_2\"}"
}
*/

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
	// HandleIncomingMessage(wrapEvent("NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits", "{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"INITIALIZATION_STEP\",\"splitName\":\"PUSH_TEST_2\"}"))
	t.Error("ASD")
}
