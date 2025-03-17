package mutexmap

import (
	"sync"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

type MMUniqueKeysStorage struct {
	data             map[string]*set.ThreadUnsafeSet
	maxSize          int64
	size             int64
	mutex            *sync.RWMutex
	fullChan         chan string //only write channel
	logger           logging.LoggerInterface
	runtimeTelemetry storage.TelemetryRuntimeProducer
}

func NewMMUniqueKeysStorage(maxSize int64, isFull chan string, logger logging.LoggerInterface, runtimeTelemetry storage.TelemetryRuntimeProducer) *MMUniqueKeysStorage {
	return &MMUniqueKeysStorage{
		data:             make(map[string]*set.ThreadUnsafeSet),
		maxSize:          maxSize,
		size:             0,
		mutex:            &sync.RWMutex{},
		fullChan:         isFull,
		logger:           logger,
		runtimeTelemetry: runtimeTelemetry,
	}
}

func (s *MMUniqueKeysStorage) Add(featureName string, key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.size++
	_, ok := s.data[featureName]
	if !ok {
		s.data[featureName] = set.NewSet()
	}

	s.data[featureName].Add(key)

	if s.size >= s.maxSize {
		s.sendSignalIsFull()
	}
}

func (s *MMUniqueKeysStorage) PopAll() dtos.Uniques {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	toReturn := s.data
	s.data = make(map[string]*set.ThreadUnsafeSet)
	s.size = 0

	result := getUniqueKeysDto(toReturn)

	return result
}

func (s *MMUniqueKeysStorage) sendSignalIsFull() {
	// Nom blocking select
	select {
	case s.fullChan <- "UNIQUE_KEYS_FULL":
		// Send "queue is full" signal
		break
	default:
		s.logger.Debug("Some error occurred on sending signal for unique keys")
	}
}

func getUniqueKeysDto(uniques map[string]*set.ThreadUnsafeSet) dtos.Uniques {
	uniqueKeys := dtos.Uniques{
		Keys: make([]dtos.Key, 0, len(uniques)),
	}

	for name, keys := range uniques {
		list := keys.List()
		keysDto := make([]string, 0, len(list))

		for _, value := range list {
			keysDto = append(keysDto, value.(string))
		}
		keyDto := dtos.Key{
			Feature: name,
			Keys:    keysDto,
		}

		uniqueKeys.Keys = append(uniqueKeys.Keys, keyDto)
	}

	return uniqueKeys
}

var _ storage.UniqueKeysStorageConsumer = (*MMUniqueKeysStorage)(nil)
var _ storage.UniqueKeysStorageProducer = (*MMUniqueKeysStorage)(nil)
