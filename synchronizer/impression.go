package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// ImpressionSynchronizer struct for impression sync
type ImpressionSynchronizer struct {
	impressionStorage  storage.ImpressionStorage
	impressionRecorder service.ImpressionsRecorder
	logger             logging.LoggerInterface
}

// NewImpressionSynchronizer creates new impression synchronizer for posting impressions
func NewImpressionSynchronizer(
	impressionStorage storage.ImpressionStorage,
	impressionRecorder service.ImpressionsRecorder,
	logger logging.LoggerInterface,
) *ImpressionSynchronizer {
	return &ImpressionSynchronizer{
		impressionStorage:  impressionStorage,
		impressionRecorder: impressionRecorder,
		logger:             logger,
	}
}

// SynchronizeImpressions syncs impressions
func (i *ImpressionSynchronizer) SynchronizeImpressions(bulkSize int64) error {
	queuedImpressions, err := i.impressionStorage.PopN(bulkSize)
	if err != nil {
		i.logger.Error("Error reading impressions queue", err)
		return errors.New("Error reading impressions queue")
	}

	if len(queuedImpressions) == 0 {
		i.logger.Debug("No impressions fetched from queue. Nothing to send")
		return nil
	}

	return i.impressionRecorder.Record(queuedImpressions)
}

// FlushImpressions flushes impressions
func (i *ImpressionSynchronizer) FlushImpressions(bulkSize int64) error {
	for !i.impressionStorage.Empty() {
		err := i.SynchronizeImpressions(bulkSize)
		if err != nil {
			return err
		}
	}
	return nil
}
