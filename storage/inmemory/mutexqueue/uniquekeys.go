package mutexqueue

import (
	"container/list"
	"sync"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

type UniqueKeyWrapper struct {
	featureName string
	key         string
}

type MQUniqueKeysStorage struct {
	queue      *list.List
	maxSize    int64
	mutexQueue *sync.Mutex
	fullChan   chan string //only write channel
	logger     logging.LoggerInterface
}

func NewMQUniqueKeysStorage(maxSize int64, isFull chan string, logger logging.LoggerInterface) *MQUniqueKeysStorage {
	return &MQUniqueKeysStorage{
		queue:      list.New(),
		maxSize:    maxSize,
		mutexQueue: &sync.Mutex{},
		fullChan:   isFull,
		logger:     logger,
	}
}

func (s *MQUniqueKeysStorage) Push(featureName string, key string) {
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()

	s.queue.PushBack(UniqueKeyWrapper{featureName: featureName, key: key})
	if s.queue.Len() == int(s.maxSize) {
		s.sendSignalIsFull()
	}
}

func (s *MQUniqueKeysStorage) PopN(n int64) dtos.Uniques {
	s.mutexQueue.Lock()
	defer s.mutexQueue.Unlock()

	var totalItems int
	if int64(s.queue.Len()) >= n {
		totalItems = int(n)
	} else {
		totalItems = s.queue.Len()
	}

	uniques := make(map[string]*set.ThreadUnsafeSet)
	for i := 0; i < totalItems; i++ {
		item, ok := s.queue.Remove(s.queue.Front()).(UniqueKeyWrapper)
		if !ok {
			continue
		}

		_, exists := uniques[item.featureName]
		if !exists {
			uniques[item.featureName] = set.NewSet()
		}

		uniques[item.featureName].Add(item.key)
	}

	return getUniqueKeysDto(uniques)
}

func (s *MQUniqueKeysStorage) sendSignalIsFull() {
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
	keysToReturn := make([]dtos.Key, 0)

	for name, keys := range uniques {
		keyDto := dtos.Key{
			Feature: name,
			Keys:    keys.List(),
		}

		keysToReturn = append(keysToReturn, keyDto)
	}

	return dtos.Uniques{
		Keys: keysToReturn,
	}
}

var _ storage.UniqueKeysStorageConsumer = (*MQUniqueKeysStorage)(nil)
var _ storage.UniqueKeysStorageProducer = (*MQUniqueKeysStorage)(nil)
