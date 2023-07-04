package push

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/push/mocks"
	storageMocks "github.com/splitio/go-split-commons/v5/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
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
	ffStorageMock := storageMocks.MockSplitStorage{}
	processor, err := NewProcessor(5000, 5000, syncMock, logger, ffStorageMock)
	if err != nil {
		t.Error("It should not return err")
	}

	sk3 := dtos.NewSplitKillUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits"), changeNumber),
		splitName,
		defaultTreatment,
	)
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

	s1 := dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits"), changeNumber),
		nil, nil)
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

	se2 := dtos.NewSegmentChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments"), changeNumber),
		segment,
	)

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
