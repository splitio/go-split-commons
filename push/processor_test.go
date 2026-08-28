package push

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v10/conf"
	"github.com/splitio/go-split-commons/v10/dtos"
	"github.com/splitio/go-split-commons/v10/push/mocks"
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
	processor, err := NewProcessor(5000, 5000, syncMock, logger, &conf.LargeSegmentConfig{
		Enable:          true,
		UpdateQueueSize: 5000,
	}, 0)
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

	se3 := dtos.NewLargeSegmentChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_largesegments"), changeNumber),
		[]dtos.LargeSegmentRFDResponseDTO{{Name: "largesegment", NotificationType: "large"}},
	)

	err = processor.ProcessLargeSegmentChangeUpdate(se3)
	if err != nil {
		t.Error("It should not return error")
	}
	if len(processor.segmentQueue) != 1 {
		t.Error("It should be 0")
	}
	if len(processor.splitQueue) != 2 {
		t.Error("It should be 2")
	}
	if len(processor.largeSegment.queue) != 1 {
		t.Error("lsQueue should be 1")
	}
}

func TestProcessorConfigDisabled(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	syncMock := &mocks.LocalSyncMock{}
	processor, err := NewProcessor(5000, 5000, syncMock, logger, nil, 0)
	if err != nil {
		t.Error("It should not return err")
	}
	if processor.config != nil {
		t.Error("config should be nil when configQueueSize is 0")
	}

	err = processor.ProcessConfigChangeUpdate(dtos.NewConfigChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "config_channel"), 100), nil, nil, nil))
	if err != nil {
		t.Error("It should not return error when config streaming is disabled")
	}
}

func TestProcessorConfigQueueTooSmall(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	syncMock := &mocks.LocalSyncMock{}
	_, err := NewProcessor(5000, 5000, syncMock, logger, nil, 10)
	if err == nil {
		t.Error("It should return an error for an undersized configQueue")
	}
}

func TestProcessorConfigEnabled(t *testing.T) {
	changeNumber := int64(100)
	logger := logging.NewLogger(&logging.LoggerOptions{})
	done := make(chan struct{}, 1)
	syncMock := &mocks.LocalSyncMock{
		SynchronizeConfigCall: func(update *dtos.ConfigChangeUpdate) error {
			if update.ChangeNumber() != changeNumber {
				t.Error("Wrong changeNumber passed")
			}
			done <- struct{}{}
			return nil
		},
	}
	processor, err := NewProcessor(5000, 5000, syncMock, logger, nil, 5000)
	if err != nil {
		t.Error("It should not return err")
	}
	if processor.config == nil {
		t.Error("config should not be nil when configQueueSize > 0")
	}

	err = processor.ProcessConfigChangeUpdate(dtos.NewConfigChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "config_channel"), changeNumber), nil, nil, nil))
	if err != nil {
		t.Error("It should not return error")
	}
	if len(processor.config.queue) != 1 {
		t.Error("configQueue should be 1")
	}

	processor.StartWorkers()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("SynchronizeConfig should have been called")
	}
	processor.StopWorkers()
}
