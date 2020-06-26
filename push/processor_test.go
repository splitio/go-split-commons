package push

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestProcessor(t *testing.T) {
	controlType := "some"
	changeNumber := int64(1591996754396)
	defaultTreatment := "defaultTreatment"
	splitName := "split"
	segment := "segment"

	logger := logging.NewLogger(&logging.LoggerOptions{})
	segmentQueue := make(chan dtos.SegmentChangeNotification, 5000)
	splitQueue := make(chan dtos.SplitChangeNotification, 5000)
	splitStorage := mocks.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string) {
			if splitName != "split" {
				t.Error("Wrong splitName passed")
			}
			if defaultTreatment != "defaultTreatment" {
				t.Error("Wrong defaultTreatment passed")
			}
		},
	}
	processor, err := NewProcessor(segmentQueue, splitQueue, splitStorage, logger)
	if err != nil {
		t.Error("It should not return err")
	}

	w0 := dtos.IncomingNotification{
		Channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
		Type:    "wrong",
	}
	err = processor.Process(w0)
	if err == nil {
		t.Error("It should return error")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 0 {
		t.Error("It should be 0")
	}

	c0 := dtos.IncomingNotification{
		Channel: "control_pri",
		Type:    dtos.Control,
	}
	err = processor.Process(c0)
	if err == nil {
		t.Error("It should return error")
	}

	c1 := dtos.IncomingNotification{
		Channel:     "control_pri",
		Type:        dtos.Control,
		ControlType: &controlType,
	}
	err = processor.Process(c1)
	if err != nil {
		t.Error("It should not return error")
	}

	sk0 := dtos.IncomingNotification{
		Channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:    "SPLIT_KILL",
	}
	err = processor.Process(sk0)
	if err == nil {
		t.Error("It should return error")
	}

	sk1 := dtos.IncomingNotification{
		Channel:      "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:         "SPLIT_KILL",
		ChangeNumber: &changeNumber,
	}
	err = processor.Process(sk1)
	if err == nil {
		t.Error("It should return error")
	}

	sk2 := dtos.IncomingNotification{
		Channel:          "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:             "SPLIT_KILL",
		ChangeNumber:     &changeNumber,
		DefaultTreatment: &defaultTreatment,
	}
	err = processor.Process(sk2)
	if err == nil {
		t.Error("It should return error")
	}

	sk3 := dtos.IncomingNotification{
		Channel:          "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:             "SPLIT_KILL",
		ChangeNumber:     &changeNumber,
		DefaultTreatment: &defaultTreatment,
		SplitName:        &splitName,
	}
	err = processor.Process(sk3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 1 {
		t.Error("It should be 1")
	}

	s0 := dtos.IncomingNotification{
		Channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:    "SPLIT_UPDATE",
	}
	err = processor.Process(s0)
	if err == nil {
		t.Error("It should return error")
	}

	s1 := dtos.IncomingNotification{
		Channel:      "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
		Type:         "SPLIT_UPDATE",
		ChangeNumber: &changeNumber,
	}
	err = processor.Process(s1)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}

	se0 := dtos.IncomingNotification{
		Channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
		Type:    "SEGMENT_UPDATE",
	}
	err = processor.Process(se0)
	if err == nil {
		t.Error("It should return error")
	}

	se1 := dtos.IncomingNotification{
		Channel:      "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
		Type:         "SEGMENT_UPDATE",
		ChangeNumber: &changeNumber,
	}
	err = processor.Process(se1)
	if err == nil {
		t.Error("It should return error")
	}

	se2 := dtos.IncomingNotification{
		Channel:      "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
		Type:         "SEGMENT_UPDATE",
		ChangeNumber: &changeNumber,
		SegmentName:  &segment,
	}
	err = processor.Process(se2)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(segmentQueue) != 1 {
		t.Error("It should be 0")
	}
	if len(splitQueue) != 2 {
		t.Error("It should be 2")
	}
}
