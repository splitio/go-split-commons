package push

import (
	"testing"

	"github.com/splitio/go-toolkit/logging"
)

func TestStatusTrackerAblyerror(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)

	if *tracker.HandleAblyError(&AblyError{code: 40141}) != StatusRetryableError {
		t.Error("should be a retryable error")
	}

	tracker.Reset()
	if *tracker.HandleAblyError(&AblyError{code: 40000}) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}

	tracker.Reset()
	if *tracker.HandleAblyError(&AblyError{code: 50000}) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}
}

func TestStatusTrackerControlMessages(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)

	if *tracker.HandleControl(&ControlUpdate{controlType: ControlTypeStreamingPaused}) != StatusDown {
		t.Error("should be a push down")
	}

	if *tracker.HandleControl(&ControlUpdate{controlType: ControlTypeStreamingEnabled}) != StatusUp {
		t.Error("should be a push up")
	}

	if *tracker.HandleControl(&ControlUpdate{controlType: ControlTypeStreamingDisabled}) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}
}

func TestStatusTrackerOccupancyMessages(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)

	if tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_pri"}, publishers: 0}) != nil {
		t.Error("should have returned no message")
	}

	if *tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_sec"}, publishers: 0}) != StatusDown {
		t.Error("should have returned streaming down")
	}

	if *tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_pri"}, publishers: 1}) != StatusUp {
		t.Error("should have returned streaming down")
	}

}

func TestHandleDisconnection(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)

	if *tracker.HandleDisconnection() != StatusRetryableError {
		t.Error("should have returned retryable error")
	}

	tracker.Reset()
	tracker.NotifySSEShutdownExpected()
	if tracker.HandleDisconnection() != nil {
		t.Error("should have returned nil")
	}
}

func TestHandlersWhenDisconnectionNotified(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)
	tracker.NotifySSEShutdownExpected()

	if tracker.HandleAblyError(&AblyError{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}

	if tracker.HandleControl(&ControlUpdate{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}

	if tracker.HandleOccupancy(&OccupancyMessage{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}
}

func TestStatusTrackerCombinations(t *testing.T) {
	logger := logging.NewLogger(nil)
	tracker := NewStatusTracker(logger)

	if tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_pri"}, publishers: 0}) != nil {
		t.Error("should have returned no message")
	}

	if *tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_sec"}, publishers: 0}) != StatusDown {
		t.Error("should have returned streaming down")
	}

	if tracker.HandleControl(&ControlUpdate{controlType: ControlTypeStreamingPaused}) != nil {
		t.Error("no error should be propagated if we're already down")
	}

	if tracker.HandleOccupancy(&OccupancyMessage{BaseMessage: BaseMessage{channel: "control_pri"}, publishers: 1}) != nil {
		t.Error("should not return status up since streaming is down")
	}

	if *tracker.HandleControl(&ControlUpdate{controlType: ControlTypeStreamingEnabled}) != StatusUp {
		t.Error("should be a push up")
	}
}
