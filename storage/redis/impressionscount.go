package redis

import (
	"fmt"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
)

type ImpressionsCountStorage struct {
	client   *redis.PrefixedRedisClient
	mutex    *sync.Mutex
	logger   logging.LoggerInterface
	redisKey string
}

func NewImpressionsCountStorage(client *redis.PrefixedRedisClient, logger logging.LoggerInterface) storage.ImpressionsCountProducer {
	return &ImpressionsCountStorage{
		client:   client,
		mutex:    &sync.Mutex{},
		logger:   logger,
		redisKey: KeyImpressionsCount,
	}
}

func (r *ImpressionsCountStorage) RecordImpressionsCount(impressions dtos.ImpressionsCountDTO) error {
	if len(impressions.PerFeature) < 1 {
		return nil
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	pipe := r.client.Pipeline()
	for _, value := range impressions.PerFeature {
		pipe.HIncrBy(r.redisKey, fmt.Sprintf("%s::%d", value.FeatureName, value.TimeFrame), value.RawCount)
	}

	pipe.HLen(r.redisKey)
	res, err := pipe.Exec()
	if err != nil {
		r.logger.Error("Error incrementing impressions count, %w", err)
		return err
	}
	if len(res) < len(impressions.PerFeature) {
		return fmt.Errorf("Error incrementing impressions count")
	}

	// Checks if expiration needs to be set
	if shouldSetExpirationKey(&impressions, res) {
		r.logger.Debug("Proceeding to set expiration for: ", r.redisKey)
		result := r.client.Expire(r.redisKey, time.Duration(TTLImpressions)*time.Second)
		if result == false {
			r.logger.Error("Something were wrong setting expiration for %s", r.redisKey)
		}
	}

	return nil
}

func shouldSetExpirationKey(impressions *dtos.ImpressionsCountDTO, res []redis.Result) bool {
	hlenRes := res[len(res)-1]

	var totalCounts int64
	var resCounts int64
	for i := 0; i < len(impressions.PerFeature); i++ {
		totalCounts += impressions.PerFeature[i].RawCount
		resCounts += res[i].Int()
	}

	return totalCounts+int64(len(impressions.PerFeature)) == hlenRes.Int()+resCounts
}
