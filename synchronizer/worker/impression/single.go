package impression

import (
	"errors"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/util"
	"github.com/splitio/go-toolkit/logging"
)

// RecorderSingle struct for impression sync
type RecorderSingle struct {
	impressionStorage  storage.ImpressionStorage
	impressionRecorder service.ImpressionsRecorder
	metricStorage      storage.MetricsStorage
	logger             logging.LoggerInterface
	metadata           dtos.Metadata
}

// NewRecorderSingle creates new impression synchronizer for posting impressions
func NewRecorderSingle(
	impressionStorage storage.ImpressionStorage,
	impressionRecorder service.ImpressionsRecorder,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) ImpressionRecorder {
	return &RecorderSingle{
		impressionStorage:  impressionStorage,
		impressionRecorder: impressionRecorder,
		metricStorage:      metricStorage,
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
			i.metricStorage.IncCounter(strings.Replace(testImpressionsCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
		}
		return err
	}
	bucket := util.Bucket(time.Now().Sub(before).Nanoseconds())
	i.metricStorage.IncLatency(testImpressionsLatencies, bucket)
	i.metricStorage.IncCounter(strings.Replace(testImpressionsCounters, "{status}", "200", 1))
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
