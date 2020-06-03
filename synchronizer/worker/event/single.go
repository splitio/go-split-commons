package event

import (
	"errors"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

// RecorderSingle struct for event sync
type RecorderSingle struct {
	eventStorage  storage.EventStorageConsumer
	eventRecorder service.EventsRecorder
	metricStorage storage.MetricsStorageProducer
	logger        logging.LoggerInterface
	metadata      dtos.Metadata
}

// NewEventRecorderSingle creates new event synchronizer for posting events
func NewEventRecorderSingle(
	eventStorage storage.EventStorageConsumer,
	eventRecorder service.EventsRecorder,
	metricStorage storage.MetricsStorageProducer,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) EventRecorder {
	return &RecorderSingle{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		metricStorage: metricStorage,
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

	before := time.Now()
	err = e.eventRecorder.Record(queuedEvents, e.metadata)
	if err != nil {
		if _, ok := err.(*dtos.HTTPError); ok {
			e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
		}
		return err
	}
	bucket := util.Bucket(time.Now().Sub(before).Nanoseconds())
	e.metricStorage.IncLatency(postEventsLatencies, bucket)
	e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", "200", 1))
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
