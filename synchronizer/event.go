package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// EventSynchronizer struct for event sync
type EventSynchronizer struct {
	eventStorage  storage.EventsStorage
	eventRecorder service.EventsRecorder
	logger        logging.LoggerInterface
}

// NewEventSynchronizer creates new event synchronizer for posting events
func NewEventSynchronizer(
	eventStorage storage.EventsStorage,
	eventRecorder service.EventsRecorder,
	logger logging.LoggerInterface,
) *EventSynchronizer {
	return &EventSynchronizer{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		logger:        logger,
	}
}

// SynchronizeEvents syncs events
func (e *EventSynchronizer) SynchronizeEvents(bulkSize int64) error {
	queuedEvents, err := e.eventStorage.PopN(bulkSize)
	if err != nil {
		e.logger.Error("Error reading events queue", err)
		return errors.New("Error reading events queue")
	}

	if len(queuedEvents) == 0 {
		e.logger.Debug("No events fetched from queue. Nothing to send")
		return nil
	}

	return e.eventRecorder.Record(queuedEvents)
}

// FlushEvents flushes events
func (e *EventSynchronizer) FlushEvents(bulkSize int64) error {
	for !e.eventStorage.Empty() {
		err := e.SynchronizeEvents(bulkSize)
		if err != nil {
			return err
		}
	}
	return nil
}
