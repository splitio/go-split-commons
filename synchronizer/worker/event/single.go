package event

import (
	"errors"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-toolkit/v4/logging"
)

// RecorderSingle struct for event sync
type RecorderSingle struct {
	eventStorage  storage.EventStorageConsumer
	eventRecorder service.EventsRecorder
	logger        logging.LoggerInterface
	metadata      dtos.Metadata
}

// NewEventRecorderSingle creates new event synchronizer for posting events
func NewEventRecorderSingle(
	eventStorage storage.EventStorageConsumer,
	eventRecorder service.EventsRecorder,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) EventRecorder {
	return &RecorderSingle{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		logger:        logger,
		metadata:      metadata,
	}
}

// SynchronizeEvents syncs events
func (e *RecorderSingle) SynchronizeEvents(bulkSize int64) error {
	queuedEvents, err := e.eventStorage.PopN(bulkSize)
	if err != nil {
		e.logger.Error("Error reading events queue", err)
		return errors.New("Error reading events queue")
	}

	if len(queuedEvents) == 0 {
		e.logger.Debug("No events fetched from queue. Nothing to send")
		return nil
	}

	err = e.eventRecorder.Record(queuedEvents, e.metadata)
	if err != nil {
		return err
	}
	return nil
}

// FlushEvents flushes events
func (e *RecorderSingle) FlushEvents(bulkSize int64) error {
	for !e.eventStorage.Empty() {
		err := e.SynchronizeEvents(bulkSize)
		if err != nil {
			return err
		}
	}
	return nil
}
