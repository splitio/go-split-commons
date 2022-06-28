package redis

import (
	"fmt"
	"sync"

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
	r.mutex.Lock()
	defer r.mutex.Unlock()

	pipe := r.client.Pipeline()
	for _, value := range impressions.PerFeature {
		pipe.HIncrBy(r.redisKey, fmt.Sprintf("%s::%d", value.FeatureName, value.TimeFrame), value.RawCount)
	}

	res, err := pipe.Exec()
	if len(res) < len(impressions.PerFeature) || err != nil {
		r.logger.Error("Error incrementing impressions count")
		return err
	}

	return nil
}
