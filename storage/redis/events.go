package redis

import (
	"encoding/json"
	"math"
	"sync"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/queuecache"
	"github.com/splitio/go-toolkit/redis"
)

// EventsStorage redis implementation of EventsStorage interface
type EventsStorage struct {
	cache    queuecache.InMemoryQueueCacheOverlay
	client   *redis.PrefixedRedisClient
	logger   logging.LoggerInterface
	metadata dtos.Metadata
	redisKey string
}

var elMutex = &sync.Mutex{}

// MaxAccumulatedSize is the maximum number of bytes to be fetched from cache before posting to the backend
const MaxAccumulatedSize = 5 * 1024 * 1024

// MaxEventSize is the maximum allowed event size
const MaxEventSize = 32 * 1024

// NewEventsStorage returns an instance of RedisEventsStorage
func NewEventsStorage(redisClient *redis.PrefixedRedisClient, metadata dtos.Metadata, logger logging.LoggerInterface) *EventsStorage {
	refillFunc := func(count int) ([]interface{}, error) {
		elMutex.Lock()
		defer elMutex.Unlock()
		lrange, err := redisClient.LRange(redisEvents, 0, int64(count-1))
		if err != nil {
			logger.Error("Fetching events", err)
			return nil, err
		}
		totalFetchedEvents := len(lrange)

		idxFrom := count
		if totalFetchedEvents < count {
			idxFrom = totalFetchedEvents
		}

		err = redisClient.LTrim(redisEvents, int64(idxFrom), -1)
		if err != nil {
			logger.Error("Trim events", err)
			return nil, err
		}

		toReturn := make([]interface{}, len(lrange))
		for index, item := range lrange {
			toReturn[index] = item
		}
		return toReturn, nil
	}

	return &EventsStorage{
		cache:    *queuecache.New(10000, refillFunc),
		client:   redisClient,
		logger:   logger,
		metadata: metadata,
		redisKey: redisEvents,
	}
}

// Push events into Redis LIST data type with RPUSH command
func (r *EventsStorage) Push(event dtos.EventDTO, _ int) error {
	var queueMessage = dtos.QueueStoredEventDTO{Metadata: r.metadata, Event: event}

	eventJSON, err := json.Marshal(queueMessage)
	if err != nil {
		r.logger.Error("Something were wrong marshaling provided event to JSON", err.Error())
		return err
	}

	r.logger.Debug("Pushing events to:", r.redisKey, string(eventJSON))

	_, errPush := r.client.RPush(r.redisKey, eventJSON)
	if errPush != nil {
		r.logger.Error("Something were wrong pushing event to redis", errPush)
		return errPush
	}

	return nil
}

// PopN return N elements from 0 to N
func (r *EventsStorage) PopN(n int64) ([]dtos.EventDTO, error) {
	toReturn := make([]dtos.EventDTO, 0)
	/*
		r.mutex.Lock()
		lrange, err := r.client.LRange(r.redisKey, 0, n-1)
		if err != nil {
			r.logger.Error("Fetching events", err.Error())
			r.mutex.Unlock()
			return nil, err
		}
		totalFetchedEvents := int64(len(lrange))

		idxFrom := n
		if totalFetchedEvents < n {
			idxFrom = totalFetchedEvents
		}

		err = r.client.LTrim(r.redisKey, idxFrom, -1)
		if err != nil {
			r.logger.Error("Trim events", err.Error())
			r.mutex.Unlock()
			return nil, err
		}
		r.mutex.Unlock()

		//JSON unmarshal
		for _, se := range lrange {
			storedEventDTO := dtos.QueueStoredEventDTO{}
			err := json.Unmarshal([]byte(se), &storedEventDTO)
			if err != nil {
				r.logger.Error("Error decoding event JSON", err.Error())
				continue
			}
			if storedEventDTO.Metadata.MachineIP == r.metadataMessage.MachineIP &&
				storedEventDTO.Metadata.MachineName == r.metadataMessage.MachineName &&
				storedEventDTO.Metadata.SDKVersion == r.metadataMessage.SDKVersion {
				toReturn = append(toReturn, storedEventDTO.Event)
			}
		}
	*/
	return toReturn, nil
}

// PopNWithMetadata pop N elements from queue
func (r *EventsStorage) PopNWithMetadata(n int64) ([]dtos.QueueStoredEventDTO, error) {
	toReturn := make([]dtos.QueueStoredEventDTO, n)
	var err error
	fetchedCount := 0
	accumulatedSize := 0
	writeIndex := 0
	for r.Count() > 0 && int64(fetchedCount) < n && accumulatedSize+MaxEventSize < MaxAccumulatedSize && err == nil {
		numberOfItemsToFetch := int(math.Min(
			float64((MaxAccumulatedSize-accumulatedSize)/MaxEventSize),
			float64(n-int64(fetchedCount)),
		))
		elems, err := r.cache.Fetch(numberOfItemsToFetch)
		if err != nil {
			r.logger.Error("Error fetching events", err.Error())
			break
		}

		for _, elem := range elems {
			asStr, ok := elem.(string)
			if !ok {
				r.logger.Error("Error type-asserting event as string", err.Error())
				continue
			}

			storedEventDTO := dtos.QueueStoredEventDTO{}
			err = json.Unmarshal([]byte(asStr), &storedEventDTO)
			if err != nil {
				r.logger.Error("Error decoding event JSON", err.Error())
				continue
			}
			accumulatedSize += storedEventDTO.Event.Size()
			toReturn[writeIndex] = storedEventDTO
			writeIndex++
		}
		fetchedCount += len(elems)
	}
	return toReturn[0:writeIndex], nil
}

// Count returns the number of items in the redis list
func (r *EventsStorage) Count() int64 {
	val, err := r.client.LLen(r.redisKey)
	if err != nil {
		return 0
	}
	return val
}

// Empty returns true if redis list is zero length
func (r *EventsStorage) Empty() bool {
	return r.Count() == 0
}

// Drop drops events from queue
func (r *EventsStorage) Drop(size *int64) error {
	elMutex.Lock()
	defer elMutex.Unlock()
	if size == nil {
		_, err := r.client.Del(r.redisKey)
		return err
	}
	return r.client.LTrim(r.redisKey, *size, -1)
}
