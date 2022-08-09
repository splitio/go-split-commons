package impression

import (
	"errors"

	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/logging"
)

type RecorderRedis struct {
	impressionStorage storage.ImpressionStorageConsumer
	redisStorage      storage.ImpressionStorageProducer
	logger            logging.LoggerInterface
}

// NewRecorderRedis creates new impressionsCount synchronizer for log impressionsCount in redis
func NewRecorderRedis(
	impressionStorage storage.ImpressionStorageConsumer,
	redisStorage storage.ImpressionStorageProducer,
	logger logging.LoggerInterface,
) ImpressionRecorder {
	return &RecorderRedis{
		impressionStorage: impressionStorage,
		redisStorage:      redisStorage,
		logger:            logger,
	}
}

// SynchronizeImpressions syncs impressions
func (i *RecorderRedis) SynchronizeImpressions(bulkSize int64) error {
	queuedImpressions, err := i.impressionStorage.PopN(bulkSize)
	if err != nil {
		i.logger.Error("Error reading impressions queue", err)
		return errors.New("Error reading impressions queue")
	}

	if len(queuedImpressions) == 0 {
		i.logger.Debug("No impressions fetched from queue. Nothing to send")
		return nil
	}

	err = i.redisStorage.LogImpressions(queuedImpressions)
	if err != nil {
		i.logger.Error("Error saving impressions in redis", err)
		return errors.New("Error saving impressions in redis")
	}

	return nil
}

// FlushImpressions flushes impressions
func (i *RecorderRedis) FlushImpressions(bulkSize int64) error {
	for !i.impressionStorage.Empty() {
		err := i.SynchronizeImpressions(bulkSize)
		if err != nil {
			return err
		}
	}

	return nil
}
