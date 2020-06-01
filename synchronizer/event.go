package synchronizer

import (
	"errors"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	postEventsLatencies        = "events.time"
	postEventsLatenciesBackend = "backend::/api/events/bulk"
	postEventsCounters         = "events.status.{status}"
	postEventsLocalCounters    = "backend::request.{status}"
)

// EventSynchronizer struct for event sync
type EventSynchronizer struct {
	eventStorage  storage.EventsStorage
	eventRecorder service.EventsRecorder
	metricStorage storage.MetricsStorage
	logger        logging.LoggerInterface
}

// NewEventSynchronizer creates new event synchronizer for posting events
func NewEventSynchronizer(
	eventStorage storage.EventsStorage,
	eventRecorder service.EventsRecorder,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
) *EventSynchronizer {
	return &EventSynchronizer{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		metricStorage: metricStorage,
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

	before := time.Now()
	err = e.eventRecorder.Record(queuedEvents)
	if err != nil {
		if _, ok := err.(*dtos.HTTPError); ok {
			e.metricStorage.IncCounter(strings.Replace(postEventsLocalCounters, "{status}", "error", 1))
			e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
		}
		return err
	}
	elapsed := int(time.Now().Sub(before).Nanoseconds())
	e.metricStorage.IncLatency(postEventsLatencies, elapsed)
	e.metricStorage.IncLatency(postEventsLatenciesBackend, elapsed)
	e.metricStorage.IncCounter(strings.Replace(postEventsLocalCounters, "{status}", "ok", 1))
	e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", "200", 1))
	return nil
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
