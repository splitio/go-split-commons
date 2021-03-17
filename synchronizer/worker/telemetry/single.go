package telemetry

import (
	"github.com/splitio/go-split-commons/v3/constants"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/logging"
)

// RecorderSingle struct for telemetry sync
type RecorderSingle struct {
	telemetryConsumer telemetry.FacadeConsumer
	telemetryRecorder service.TelemetryRecorder
	logger            logging.LoggerInterface
	metadata          dtos.Metadata
}

// NewEventRecorderSingle creates new event synchronizer for posting events
func NewTelemetryRecorder(
	telemetryConsumer telemetry.FacadeConsumer,
	telemetryRecorder service.TelemetryRecorder,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) TelemetryRecorder {
	return &RecorderSingle{
		telemetryConsumer: telemetryConsumer,
		telemetryRecorder: telemetryRecorder,
		logger:            logger,
		metadata:          metadata,
	}
}

func (e *RecorderSingle) buildStats() dtos.Stats {
	methodLatencies := e.telemetryConsumer.PopLatencies()
	methodExceptions := e.telemetryConsumer.PopExceptions()
	lastSynchronization := e.telemetryConsumer.GetLastSynchronization()
	httpErrors := e.telemetryConsumer.PopHTTPErrors()
	httpLatencies := e.telemetryConsumer.PopHTTPLatencies()
	return dtos.Stats{
		MethodLatencies:      &methodLatencies,
		MethodExceptions:     &methodExceptions,
		ImpressionsDropped:   e.telemetryConsumer.GetImpressionsStats(constants.ImpressionsDropped),
		ImpressionsDeduped:   e.telemetryConsumer.GetImpressionsStats(constants.ImpressionsDeduped),
		ImpressionsQueued:    e.telemetryConsumer.GetImpressionsStats(constants.ImpressionsQueued),
		EventsQueued:         e.telemetryConsumer.GetEventsStats(constants.EventsQueued),
		EventsDropped:        e.telemetryConsumer.GetEventsStats(constants.EventsDropped),
		LastSynchronizations: &lastSynchronization,
		HTTPErrors:           &httpErrors,
		HTTPLatencies:        &httpLatencies,
		SplitCount:           e.telemetryConsumer.GetSplitsCount(),
		SegmentCount:         e.telemetryConsumer.GetSegmentsCount(),
		SegmentKeyCount:      e.telemetryConsumer.GetSegmentKeysCount(),
		TokenRefreshes:       e.telemetryConsumer.PopTokenRefreshes(),
		AuthRejections:       e.telemetryConsumer.PopAuthRejections(),
		StreamingEvents:      e.telemetryConsumer.PopStreamingEvents(),
		SessionLengthMs:      e.telemetryConsumer.GetSessionLength(),
		Tags:                 e.telemetryConsumer.PopTags(),
	}
}

// SynchronizeTelemetry syncs telemetry
func (e *RecorderSingle) SynchronizeTelemetry() error {
	stats := e.buildStats()
	return e.telemetryRecorder.RecordStats(stats, e.metadata)
}
