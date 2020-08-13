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

	impressionsToPost := make(map[string][]dtos.ImpressionDTO)
	for _, impression := range queuedImpressions {
		keyImpression := dtos.ImpressionDTO{
			KeyName:      impression.KeyName,
			Treatment:    impression.Treatment,
			Time:         impression.Time,
			ChangeNumber: impression.ChangeNumber,
			Label:        impression.Label,
			BucketingKey: impression.BucketingKey,
		}
		v, ok := impressionsToPost[impression.FeatureName]
		if ok {
			v = append(v, keyImpression)
		} else {
			v = []dtos.ImpressionDTO{keyImpression}
		}
		impressionsToPost[impression.FeatureName] = v
	}

	bulkImpressions := make([]dtos.ImpressionsDTO, 0)
	for testName, testImpressions := range impressionsToPost {
		bulkImpressions = append(bulkImpressions, dtos.ImpressionsDTO{
			TestName:       testName,
			KeyImpressions: testImpressions,
		})
	}

	before := time.Now()
	err = i.impressionRecorder.Record(bulkImpressions, i.metadata)
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
