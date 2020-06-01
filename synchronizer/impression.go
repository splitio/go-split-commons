package synchronizer

import (
	"errors"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	testImpressionsLatencies        = "testImpressions.time"
	testImpressionsLatenciesBackend = "backend::/api/testImpressions/bulk"
	testImpressionsCounters         = "testImpressions.status.{status}"
	testImpressionsLocalCounters    = "backend::request.{status}"
)

// ImpressionSynchronizer struct for impression sync
type ImpressionSynchronizer struct {
	impressionStorage  storage.ImpressionStorage
	impressionRecorder service.ImpressionsRecorder
	metricStorage      storage.MetricsStorage
	logger             logging.LoggerInterface
}

// NewImpressionSynchronizer creates new impression synchronizer for posting impressions
func NewImpressionSynchronizer(
	impressionStorage storage.ImpressionStorage,
	impressionRecorder service.ImpressionsRecorder,
	metricStorage storage.MetricsStorage,
	logger logging.LoggerInterface,
) *ImpressionSynchronizer {
	return &ImpressionSynchronizer{
		impressionStorage:  impressionStorage,
		impressionRecorder: impressionRecorder,
		metricStorage:      metricStorage,
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
	before := time.Now()
	err = i.impressionRecorder.Record(queuedImpressions)
	if err != nil {
		if _, ok := err.(*dtos.HTTPError); ok {
			i.metricStorage.IncCounter(strings.Replace(testImpressionsLocalCounters, "{status}", "error", 1))
			i.metricStorage.IncCounter(strings.Replace(testImpressionsCounters, "{status}", string(err.(*dtos.HTTPError).Code), 1))
		}
		return err
	}
	elapsed := int(time.Now().Sub(before).Nanoseconds())
	i.metricStorage.IncLatency(testImpressionsLatencies, elapsed)
	i.metricStorage.IncLatency(testImpressionsLatenciesBackend, elapsed)
	i.metricStorage.IncCounter(strings.Replace(testImpressionsLocalCounters, "{status}", "ok", 1))
	i.metricStorage.IncCounter(strings.Replace(testImpressionsCounters, "{status}", "200", 1))
	return nil
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
