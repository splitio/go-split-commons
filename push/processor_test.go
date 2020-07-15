package push

import (
	"sync/atomic"
	"testing"
	"time"

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

	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelDebug})
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
	controlStatus := make(chan int, 1)
	processor, err := NewProcessor(segmentQueue, splitQueue, splitStorage, logger, controlStatus)
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

	c1 := dtos.IncomingNotification{
		Channel:     "control_pri",
		Type:        dtos.Control,
		ControlType: &controlType,
	}
	err = processor.Process(c1)
	if err != nil {
		t.Error("It should not return error")
	}

	var streamingDisabledCall int64
	var streamingResumedCall int64
	var streamingPausedCall int64
	go func() {
		for {
			select {
			case msg := <-controlStatus:
				switch msg {
				case streamingPaused:
					atomic.AddInt64(&streamingPausedCall, 1)
				case streamingResumed:
					atomic.AddInt64(&streamingResumedCall, 1)
				case streamingDisabled:
					atomic.AddInt64(&streamingDisabledCall, 1)
				default:
					t.Error("Unexpected event received")
				}
			}
		}
	}()

	disabled := streamingDisabledType
	c2 := dtos.IncomingNotification{
		Channel:     "control_pri",
		Type:        dtos.Control,
		ControlType: &disabled,
	}
	err = processor.Process(c2)
	if err != nil {
		t.Error("It should not return error")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&streamingDisabledCall) != 1 {
		t.Error("It should send a message for disabling streaming")
	}

	paused := streamingPausedType
	c3 := dtos.IncomingNotification{
		Channel:     "control_pri",
		Type:        dtos.Control,
		ControlType: &paused,
	}
	err = processor.Process(c3)
	if err != nil {
		t.Error("It should not return error")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&streamingPausedCall) != 1 {
		t.Error("It should send a message for pausing streaming")
	}

	resumed := streamingResumedType
	c4 := dtos.IncomingNotification{
		Channel:     "control_pri",
		Type:        dtos.Control,
		ControlType: &resumed,
	}
	err = processor.Process(c4)
	if err != nil {
		t.Error("It should not return error")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&streamingResumedCall) != 1 {
		t.Error("It should send a message for resuming streaming")
	}
}
