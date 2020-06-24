package service

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/service/api"
	"github.com/splitio/go-toolkit/logging"
)

// SplitAPI struct for fetchers and recorders
type SplitAPI struct {
	AuthClient         AuthClient
	SplitFetcher       SplitFetcher
	SegmentFetcher     SegmentFetcher
	ImpressionRecorder ImpressionsRecorder
	EventRecorder      EventsRecorder
	MetricRecorder     MetricsRecorder
}

// NewSplitAPI creates new splitAPI
func NewSplitAPI(
	apikey string,
	conf *conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *SplitAPI {
	return &SplitAPI{
		AuthClient:         api.NewAuthAPIClient(apikey, conf, logger),
		SplitFetcher:       api.NewHTTPSplitFetcher(apikey, conf, logger),
		SegmentFetcher:     api.NewHTTPSegmentFetcher(apikey, conf, logger),
		ImpressionRecorder: api.NewHTTPImpressionRecorder(apikey, conf, logger),
		EventRecorder:      api.NewHTTPEventsRecorder(apikey, conf, logger),
		MetricRecorder:     api.NewHTTPMetricsRecorder(apikey, conf, logger),
	}
}
