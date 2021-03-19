package telemetry

import (
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/logging"
)

// RecorderSingle struct for telemetry sync
type RecorderSingle struct {
	telemetryStorage  storage.TelemetryStorageConsumer
	telemetryRecorder service.TelemetryRecorder
	splitStorage      storage.SplitStorageConsumer
	segmentStorage    storage.SegmentStorageConsumer
	logger            logging.LoggerInterface
	metadata          dtos.Metadata
}

// NewEventRecorderSingle creates new event synchronizer for posting events
func NewTelemetryRecorder(
	telemetryStorage storage.TelemetryStorageConsumer,
	telemetryRecorder service.TelemetryRecorder,
	splitStorage storage.SplitStorageConsumer,
	segmentStorage storage.SegmentStorageConsumer,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) TelemetryRecorder {
	return &RecorderSingle{
		telemetryStorage:  telemetryStorage,
		telemetryRecorder: telemetryRecorder,
		splitStorage:      splitStorage,
		segmentStorage:    segmentStorage,
		logger:            logger,
		metadata:          metadata,
	}
}

func (e *RecorderSingle) getSegmentKeysCount() int64 {
	toReturn := 0
	segmentNames := e.splitStorage.SegmentNames().List()
	for _, segmentName := range segmentNames {
		toReturn += e.segmentStorage.Keys(segmentName.(string)).Size()
	}
	return int64(toReturn)
}

func (e *RecorderSingle) buildStats() dtos.Stats {
	methodLatencies := e.telemetryStorage.PopLatencies()
	methodExceptions := e.telemetryStorage.PopExceptions()
	lastSynchronization := e.telemetryStorage.GetLastSynchronization()
	httpErrors := e.telemetryStorage.PopHTTPErrors()
	httpLatencies := e.telemetryStorage.PopHTTPLatencies()
	return dtos.Stats{
		MethodLatencies:      &methodLatencies,
		MethodExceptions:     &methodExceptions,
		ImpressionsDropped:   e.telemetryStorage.GetImpressionsStats(telemetry.ImpressionsDropped),
		ImpressionsDeduped:   e.telemetryStorage.GetImpressionsStats(telemetry.ImpressionsDeduped),
		ImpressionsQueued:    e.telemetryStorage.GetImpressionsStats(telemetry.ImpressionsQueued),
		EventsQueued:         e.telemetryStorage.GetEventsStats(telemetry.EventsQueued),
		EventsDropped:        e.telemetryStorage.GetEventsStats(telemetry.EventsDropped),
		LastSynchronizations: &lastSynchronization,
		HTTPErrors:           &httpErrors,
		HTTPLatencies:        &httpLatencies,
		SplitCount:           int64(len(e.splitStorage.SplitNames())),
		SegmentCount:         int64(e.splitStorage.SegmentNames().Size()),
		SegmentKeyCount:      e.getSegmentKeysCount(),
		TokenRefreshes:       e.telemetryStorage.PopTokenRefreshes(),
		AuthRejections:       e.telemetryStorage.PopAuthRejections(),
		StreamingEvents:      e.telemetryStorage.PopStreamingEvents(),
		SessionLengthMs:      e.telemetryStorage.GetSessionLength(),
		Tags:                 e.telemetryStorage.PopTags(),
	}
}

// SynchronizeTelemetry syncs telemetry
func (e *RecorderSingle) SynchronizeTelemetry() error {
	stats := e.buildStats()
	return e.telemetryRecorder.RecordStats(stats, e.metadata)
}
