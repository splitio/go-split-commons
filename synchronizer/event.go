package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/service/api"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// EventSynchronizer struct for event sync
type EventSynchronizer struct {
	eventStorage  storage.EventsStorage
	eventRecorder *api.HTTPEventsRecorder
	logger        logging.LoggerInterface
}

// NewEventSynchronizer creates new event synchronizer for posting events
func NewEventSynchronizer(
	eventStorage storage.EventsStorage,
	eventRecorder *api.HTTPEventsRecorder,
	logger logging.LoggerInterface,
) *EventSynchronizer {
	return &EventSynchronizer{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		logger:        logger,
	}
}

// SynchronizeEvents syncs events
func (s *EventSynchronizer) SynchronizeEvents(bulkSize int64) error {
	queuedEvents, err := s.eventStorage.PopN(bulkSize)
	if err != nil {
		s.logger.Error("Error reading events queue", err)
		return errors.New("Error reading events queue")
	}

	if len(queuedEvents) == 0 {
		s.logger.Debug("No events fetched from queue. Nothing to send")
		return nil
	}

	return s.eventRecorder.Record(queuedEvents)
}
