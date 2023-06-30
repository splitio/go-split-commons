package push

import (
	"testing"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/storage/mocks"
	"github.com/splitio/go-split-commons/v5/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestStatusTrackerAblyerror(t *testing.T) {
	logger := logging.NewLogger(nil)
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			if streamingEvent.Type != telemetry.EventTypeAblyError || streamingEvent.Data < 40000 || streamingEvent.Data > 50000 {
				t.Error("Should track ably error")
			}
		},
	}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)

	if *tracker.HandleAblyError(dtos.NewAblyError(40141, 0, "", "", 0)) != StatusRetryableError {
		t.Error("should be a retryable error")
	}

	tracker.Reset()
	if *tracker.HandleAblyError(dtos.NewAblyError(40000, 0, "", "", 0)) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}

	tracker.Reset()
	if *tracker.HandleAblyError(dtos.NewAblyError(50000, 0, "", "", 0)) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}
}

func TestStatusTrackerControlMessages(t *testing.T) {
	logger := logging.NewLogger(nil)
	mockedTelemetryStorage := mocks.MockTelemetryStorage{}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)

	if *tracker.HandleControl(dtos.NewControlUpdate(dtos.NewBaseMessage(0, ""), dtos.ControlTypeStreamingPaused)) != StatusDown {
		t.Error("should be a push down")
	}

	if *tracker.HandleControl(dtos.NewControlUpdate(dtos.NewBaseMessage(0, ""), dtos.ControlTypeStreamingEnabled)) != StatusUp {
		t.Error("should be a push up")
	}

	if *tracker.HandleControl(dtos.NewControlUpdate(dtos.NewBaseMessage(0, ""), dtos.ControlTypeStreamingDisabled)) != StatusNonRetryableError {
		t.Error("should be a non retryable error")
	}
}

func TestStatusTrackerOccupancyMessages(t *testing.T) {
	occupancy := 0
	logger := logging.NewLogger(nil)
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch occupancy {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeOccupancyPri || streamingEvent.Data != 0 {
					t.Error("Should be PRI and 0")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeOccupancySec || streamingEvent.Data != 0 {
					t.Error("Should be SEC and 0")
				}
			case 2:
				if streamingEvent.Type != telemetry.EventTypeOccupancyPri || streamingEvent.Data != 1 {
					t.Error("Should be PRI and 1")
				}
			}
			occupancy++
		},
	}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)

	if tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_pri"), 0)) != nil {
		t.Error("should have returned no message")
	}

	if *tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_sec"), 0)) != StatusDown {
		t.Error("should have returned streaming down")
	}

	if *tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_pri"), 1)) != StatusUp {
		t.Error("should have returned streaming down")
	}

}

func TestHandleDisconnection(t *testing.T) {
	called := 0
	logger := logging.NewLogger(nil)
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeConnectionError || streamingEvent.Data != telemetry.NonRequested {
					t.Error("It should be non requested")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeConnectionError || streamingEvent.Data != telemetry.Requested {
					t.Error("It should be non requested")
				}
			}
			called++
		},
	}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)

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
	mockedTelemetryStorage := mocks.MockTelemetryStorage{}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)
	tracker.NotifySSEShutdownExpected()

	if tracker.HandleAblyError(&dtos.AblyError{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}

	if tracker.HandleControl(&dtos.ControlUpdate{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}

	if tracker.HandleOccupancy(&dtos.OccupancyMessage{}) != nil {
		t.Error("should be nil when a disconnection is expected")
	}
}

func TestStatusTrackerCombinations(t *testing.T) {
	logger := logging.NewLogger(nil)
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {},
	}
	tracker := NewStatusTracker(logger, mockedTelemetryStorage)

	if tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_pri"), 0)) != nil {
		t.Error("should have returned no message")
	}

	if *tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_sec"), 0)) != StatusDown {
		t.Error("should have returned streaming down")
	}

	if tracker.HandleControl(dtos.NewControlUpdate(dtos.NewBaseMessage(0, ""), dtos.ControlTypeStreamingPaused)) != nil {
		t.Error("no error should be propagated if we're already down")
	}

	if tracker.HandleOccupancy(dtos.NewOccupancyMessage(dtos.NewBaseMessage(0, "control_pri"), 1)) != nil {
		t.Error("should not return status up since streaming is down")
	}

	if *tracker.HandleControl(dtos.NewControlUpdate(dtos.NewBaseMessage(0, ""), dtos.ControlTypeStreamingEnabled)) != StatusUp {
		t.Error("should be a push up")
	}
}
