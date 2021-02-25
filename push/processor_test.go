package push

import (
	"testing"

	"github.com/splitio/go-split-commons/v3/push/mocks"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestProcessor(t *testing.T) {
	changeNumber := int64(1591996754396)
	defaultTreatment := "defaultTreatment"
	splitName := "split"
	segment := "segment"

	logger := logging.NewLogger(&logging.LoggerOptions{})
	syncMock := &mocks.LocalSyncMock{
		LocalKillCall: func(splitName, defaultTreatment string, changeNumber int64) {
			if splitName != "split" {
				t.Error("Wrong splitName passed")
			}
			if defaultTreatment != "defaultTreatment" {
				t.Error("Wrong defaultTreatment passed")
			}
			if changeNumber != 1591996754396 {
				t.Error("Wrong changeNumber passed")
			}
		},
	}
	processor, err := NewProcessor(5000, 5000, syncMock, logger)
	if err != nil {
		t.Error("It should not return err")
	}

	sk3 := &SplitKillUpdate{
		BaseUpdate: BaseUpdate{
			BaseMessage:  BaseMessage{channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits"},
			changeNumber: changeNumber,
		},
		defaultTreatment: defaultTreatment,
		splitName:        splitName,
	}
	err = processor.ProcessSplitKillUpdate(sk3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(processor.segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(processor.splitQueue) != 1 {
		t.Error("It should be 1")
	}

	s1 := &SplitChangeUpdate{
		BaseUpdate: BaseUpdate{
			BaseMessage:  BaseMessage{channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits"},
			changeNumber: changeNumber,
		},
	}
	err = processor.ProcessSplitChangeUpdate(s1)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(processor.segmentQueue) != 0 {
		t.Error("It should be 0")
	}
	if len(processor.splitQueue) != 2 {
		t.Error("It should be 2")
	}

	se2 := &SegmentChangeUpdate{
		BaseUpdate: BaseUpdate{
			changeNumber: changeNumber,
			BaseMessage:  BaseMessage{channel: "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments"},
		},
		segmentName: segment,
	}

	err = processor.ProcessSegmentChangeUpdate(se2)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(processor.segmentQueue) != 1 {
		t.Error("It should be 0")
	}
	if len(processor.splitQueue) != 2 {
		t.Error("It should be 2")
	}
}
