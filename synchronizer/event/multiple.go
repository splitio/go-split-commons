package event

import (
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/common"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/split-synchronizer/log"
)

// EventSynchronizerMultiple struct for event sync
type EventSynchronizerMultiple struct {
	eventStorage  storage.EventsStorage
	eventRecorder service.EventsRecorder
	metricStorage storage.MetricsStorage
	logger        logging.LoggerInterface
}

// NewEventSynchronizerMultiple creates new event synchronizer for posting events
func NewEventSynchronizerMultiple(
	eventStorage storage.EventsStorage,
	eventRecorder service.EventsRecorder,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
) EventSynchronizer {
	return &EventSynchronizerMultiple{
		eventStorage:  eventStorage,
		eventRecorder: eventRecorder,
		metricStorage: metricStorage,
		logger:        logger,
	}
}

// SynchronizeEvents syncs events
func (e *EventSynchronizerMultiple) SynchronizeEvents(bulkSize int64) error {
	// [SDKVersion][MachineIP][MachineName]
	toSend := make(map[string]map[string]map[string][]dtos.EventDTO)

	storedEvents, err := e.eventStorage.PopNWithMetadata(bulkSize) //PopN has a mutex, so this function can be async without issues
	if err != nil {
		log.Error.Println("(Task) Post Events fails fetching events from storage", err.Error())
		return err
	}

	for _, stored := range storedEvents {
		if stored.Metadata.SDKVersion == "" {
			continue
		}

		sdk := stored.Metadata.SDKVersion
		ip := stored.Metadata.MachineIP
		mname := stored.Metadata.MachineName

		if ip == "" {
			ip = "unknown"
		}
		if mname == "" {
			mname = "unknown"
		}

		if toSend[sdk] == nil {
			toSend[sdk] = make(map[string]map[string][]dtos.EventDTO)
		}

		if toSend[sdk][ip] == nil {
			toSend[sdk][ip] = make(map[string][]dtos.EventDTO)
		}

		if toSend[sdk][ip][mname] == nil {
			toSend[sdk][ip][mname] = make([]dtos.EventDTO, 0)
		}

		toSend[sdk][ip][mname] = append(toSend[sdk][ip][mname], stored.Event)
	}

	for s, byIP := range toSend {
		for i, byName := range byIP {
			for n, bulk := range byName {
				err := common.WithAttempts(3, func() error {
					before := time.Now()
					/*
						if appcontext.ExecutionMode() == appcontext.ProducerMode && attemps == 0 {
							beforePostServer := time.Now().UnixNano()
							StoreDataFlushed(beforePostServer, len(bulk), storageAdapter.Size(), "events")
						}
					*/
					err = e.eventRecorder.Record(bulk, dtos.Metadata{
						MachineIP:   i,
						SDKVersion:  s,
						MachineName: n,
					})
					if err != nil {
						e.logger.Error("Error attempting to post events")
					}
					elapsed := int(time.Now().Sub(before).Nanoseconds())
					e.metricStorage.IncLatency(postEventsLatencies, elapsed)
					e.metricStorage.IncLatency(postEventsLatenciesBackend, elapsed)
					e.metricStorage.IncCounter(strings.Replace(postEventsLocalCounters, "{status}", "ok", 1))
					e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", "200", 1))
					return nil
				})
				if err != nil {
					if _, ok := err.(*dtos.HTTPError); ok {
						e.metricStorage.IncCounter(strings.Replace(postEventsLocalCounters, "{status}", "error", 1))
						e.metricStorage.IncCounter(strings.Replace(postEventsCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
					}
					return err
				}
			}
		}
	}
	return nil
}

// FlushEvents flushes events
func (e *EventSynchronizerMultiple) FlushEvents(bulkSize int64) error {
	return nil
}
