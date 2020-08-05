package impression

import (
	"errors"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

// RecorderSingle struct for impression sync
type RecorderSingle struct {
	impressionStorage  storage.ImpressionStorageConsumer
	impressionRecorder service.ImpressionsRecorder
	metricsWrapper     *storage.MetricWrapper
	logger             logging.LoggerInterface
	metadata           dtos.Metadata
}

// NewRecorderSingle creates new impression synchronizer for posting impressions
func NewRecorderSingle(
	impressionStorage storage.ImpressionStorageConsumer,
	impressionRecorder service.ImpressionsRecorder,
	metricsWrapper *storage.MetricWrapper,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) ImpressionRecorder {
	return &RecorderSingle{
		impressionStorage:  impressionStorage,
		impressionRecorder: impressionRecorder,
		metricsWrapper:     metricsWrapper,
		logger:             logger,
		metadata:           metadata,
	}
}

// SynchronizeImpressions syncs impressions
func (i *RecorderSingle) SynchronizeImpressions(bulkSize int64) error {
	queuedImpressions, err := i.impressionStorage.PopN(bulkSize)
	if err != nil {
		i.logger.Error("Error reading impressions queue", err)
		return errors.New("Error reading impressions queue")
	}

	if len(queuedImpressions) == 0 {
		i.logger.Debug("No impressions fetched from queue. Nothing to send")
		return nil
	}
	before := time.Now()
	err = i.impressionRecorder.Record(queuedImpressions, i.metadata)
	if err != nil {
		if _, ok := err.(*dtos.HTTPError); ok {
			i.metricsWrapper.StoreCounters(storage.TestImpressionsCounter, string(err.(*dtos.HTTPError).Code), false)
		}
		return err
	}
	bucket := util.Bucket(time.Now().Sub(before).Nanoseconds())
	i.metricsWrapper.StoreLatencies(storage.TestImpressionsLatency, bucket, false)
	i.metricsWrapper.StoreCounters(storage.TestImpressionsCounter, "ok", false)
	return nil
}

// FlushImpressions flushes impressions
func (i *RecorderSingle) FlushImpressions(bulkSize int64) error {
	for !i.impressionStorage.Empty() {
		err := i.SynchronizeImpressions(bulkSize)
		if err != nil {
			return err
		}
	}
	return nil
}
