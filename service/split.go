package service

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api"
	"github.com/splitio/go-toolkit/logging"
)

// SplitAPI struct for fetchers and recorders
type SplitAPI struct {
	SplitFetcher       SplitFetcher
	SegmentFetcher     SegmentFetcher
	ImpressionRecorder ImpressionsRecorder
	EventRecorder      EventsRecorder
	MetricRecorder     MetricsRecorder
}

// NewSplitAPI creates new splitAPI
func NewSplitAPI(
	apikey string,
	version string,
	conf *conf.AdvancedConfig,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) *SplitAPI {
	return &SplitAPI{
		SplitFetcher:       api.NewHTTPSplitFetcher(apikey, conf, version, logger),
		SegmentFetcher:     api.NewHTTPSegmentFetcher(apikey, version, conf, logger),
		ImpressionRecorder: api.NewHTTPImpressionRecorder(apikey, conf, metadata, version, logger),
		EventRecorder:      api.NewHTTPEventsRecorder(apikey, conf, metadata, version, logger),
		MetricRecorder:     api.NewHTTPMetricsRecorder(apikey, version, conf, metadata, logger),
	}
}
