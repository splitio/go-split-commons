package synchronizer

import (
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// SplitSynchronizers struct for synchronizers
type SplitSynchronizers struct {
	SplitSynchronizer      *SplitSynchronizer
	SegmentSynchronizer    *SegmentSynchronizer
	MetricSynchronizer     *MetricSynchronizer
	ImpressionSynchronizer *ImpressionSynchronizer
	EventSynchronizer      *EventSynchronizer
}

// NewSplitSynchronizers creates new SplitSynchronizers
func NewSplitSynchronizers(
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	segmentStorage storage.SegmentStorage,
	metricStorage storage.MetricsStorage,
	impressionStorage storage.ImpressionStorage,
	eventStorage storage.EventsStorage,
	logger logging.LoggerInterface,
) *SplitSynchronizers {
	return &SplitSynchronizers{
		SplitSynchronizer:      NewSplitSynchronizer(splitStorage, splitAPI.SplitFetcher),
		SegmentSynchronizer:    NewSegmentSynchronizer(splitStorage, segmentStorage, splitAPI.SegmentFetcher, logger),
		MetricSynchronizer:     NewMetricSynchronizer(metricStorage, splitAPI.MetricRecorder),
		ImpressionSynchronizer: NewImpressionSynchronizer(impressionStorage, splitAPI.ImpressionRecorder, logger),
		EventSynchronizer:      NewEventSynchronizer(eventStorage, splitAPI.EventRecorder, logger),
	}
}
