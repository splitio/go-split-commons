package mutexqueue

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
)

// MaxAccumulatedBytes is the maximum size to accumulate in events before flush (in bytes)
const MaxAccumulatedBytes = 5 * 1024 * 1024

// NewMQEventsStorage returns an instance of MQEventsStorage
func NewMQEventsStorage(queueSize int, isFull chan string, logger logging.LoggerInterface, runtimeTelemetry storage.TelemetryRuntimeProducer) *MQEventsStorage {
	return &MQEventsStorage{
		queue:            list.New(),
		size:             queueSize,
		mutexQueue:       &sync.Mutex{},
		fullChan:         isFull,
		logger:           logger,
		runtimeTelemetry: runtimeTelemetry,
	}
}

type eventWrapper struct {
	event dtos.EventDTO
	size  int
}

// MQEventsStorage in memory events storage
type MQEventsStorage struct {
	queue            *list.List
	size             int
	accumulatedBytes int
	mutexQueue       *sync.Mutex
	fullChan         chan string //only write channel
	logger           logging.LoggerInterface
	runtimeTelemetry storage.TelemetryRuntimeProducer
}

func (s *MQEventsStorage) sendSignalIsFull() {
	// Nom blocking select
	select {
	case s.fullChan <- "EVENTS_FULL":
		// Send "queue is full" signal
		break
	default:
		s.logger.Debug("Some error occurred on sending signal for events")
	}
}

// Push an event into slice
func (s *MQEventsStorage) Push(event dtos.EventDTO, size int) error {
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()

	if s.queue.Len()+1 > s.size {
		s.runtimeTelemetry.RecordEventsStats(telemetry.EventsDropped, 1)
		s.sendSignalIsFull()
		return ErrorMaxSizeReached
	}

	// Add element
	s.queue.PushBack(eventWrapper{event: event, size: size})
	s.runtimeTelemetry.RecordEventsStats(telemetry.EventsQueued, 1)
	s.accumulatedBytes += size
	if s.queue.Len() == s.size || s.accumulatedBytes >= MaxAccumulatedBytes {
		s.sendSignalIsFull()
	}

	return nil
}

// PopN pop N elements from queue
func (s *MQEventsStorage) PopN(n int64) ([]dtos.EventDTO, error) {
	var toReturn []dtos.EventDTO
	var totalItems int

	// Mutexing queue
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()

	if int64(s.queue.Len()) >= n {
		totalItems = int(n)
	} else {
		totalItems = s.queue.Len()
	}

	toReturn = make([]dtos.EventDTO, 0)
	accumulated := 0
	errorCount := 0
	for i := 0; i < totalItems; i++ {
		bundled, ok := s.queue.Remove(s.queue.Front()).(eventWrapper)
		if !ok {
			errorCount++
			continue
		}
		toReturn = append(toReturn, bundled.event)
		accumulated += bundled.size
		if accumulated >= MaxAccumulatedBytes {
			// If we reached the maximum allowed size, break the loop so that we don't sent huge POST bodies to the BE
			break
		}
	}

	s.accumulatedBytes -= accumulated
	if errorCount > 0 {
		return toReturn, fmt.Errorf("%d elements could not be decoded", errorCount)
	}

	return toReturn, nil
}

// Empty returns if slice len if zero
func (s *MQEventsStorage) Empty() bool {
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()
	return s.queue.Len() == 0
}

// Count returns the number of events into slice
func (s *MQEventsStorage) Count() int64 {
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()
	return int64(s.queue.Len())
}
