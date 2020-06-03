package redis

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
)

const impressionsTTLRefresh = time.Duration(3600) * time.Second

// ImpressionStorage is a redis-based implementation of split storage
type ImpressionStorage struct {
	client         *redis.PrefixedRedisClient
	mutex          *sync.Mutex
	logger         logging.LoggerInterface
	redisKey       string
	impressionsTTL time.Duration
	metadata       dtos.Metadata
}

// NewImpressionStorage creates a new RedisSplitStorage and returns a reference to it
func NewImpressionStorage(client *redis.PrefixedRedisClient, metadata dtos.Metadata, logger logging.LoggerInterface) *ImpressionStorage {
	return &ImpressionStorage{
		client:         client,
		mutex:          &sync.Mutex{},
		logger:         logger,
		redisKey:       redisImpressionsQueue,
		impressionsTTL: redisImpressionsTTL,
		metadata:       metadata,
	}
}

// push stores impressions in redis
func (r *ImpressionStorage) push(impressions []dtos.ImpressionQueueObject) error {
	var impressionsJSON []interface{}
	for _, impression := range impressions {
		iJSON, err := json.Marshal(impression)
		if err != nil {
			r.logger.Error("Error encoding impression in json")
			r.logger.Error(err)
		} else {
			impressionsJSON = append(impressionsJSON, iJSON)
		}
	}

	r.logger.Debug("Pushing impressions to: ", r.redisKey, len(impressionsJSON))

	inserted, errPush := r.client.RPush(r.redisKey, impressionsJSON...)
	if errPush != nil {
		r.logger.Error("Something were wrong pushing impressions to redis", errPush)
		return errPush
	}

	// Checks if expiration needs to be set
	if inserted == int64(len(impressionsJSON)) {
		r.logger.Debug("Proceeding to set expiration for: ", r.redisKey)
		result := r.client.Expire(r.redisKey, time.Duration(r.impressionsTTL)*time.Minute)
		if result == false {
			r.logger.Error("Something were wrong setting expiration", errPush)
		}
	}
	return nil
}

// LogImpressions stores impressions in redis as Queue
func (r *ImpressionStorage) LogImpressions(impressions []dtos.Impression) error {
	var impressionsToStore []dtos.ImpressionQueueObject
	for _, i := range impressions {
		var impression = dtos.ImpressionQueueObject{Metadata: r.metadata, Impression: i}
		impressionsToStore = append(impressionsToStore, impression)
	}

	if len(impressionsToStore) > 0 {
		return r.push(impressionsToStore)
	}
	return nil
}

// PopN return N elements from 0 to N
func (r *ImpressionStorage) PopN(n int64) ([]dtos.Impression, error) {
	toReturn := make([]dtos.Impression, 0, 0)
	return toReturn, nil
}

// size returns the size of the impressions queue
func (r *ImpressionStorage) size() int64 {
	val, err := r.client.LLen(r.redisKey)
	if err != nil {
		return 0
	}
	return val
}

// Empty returns true if redis list is zero length
func (r *ImpressionStorage) Empty() bool {
	return r.size() == 0
}
