package api

import (
	"fmt"

	"github.com/splitio/go-split-commons/v9/conf"
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

// HTTPConfigsFetcher struct is responsible for fetching configs from the backend via HTTP protocol
type HTTPConfigsFetcher struct {
	httpFetcherBase
}

// NewHTTPConfigsFetcher instantiates and returns an HTTPConfigsFetcher
func NewHTTPConfigsFetcher(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) service.SplitFetcher {
	return &HTTPConfigsFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
	}
}

// Fetch makes an HTTP call to the /configs endpoint and returns the FFResponse
func (f *HTTPConfigsFetcher) Fetch(fetchOptions *service.FlagRequestParams) (dtos.FFResponse, error) {
	// Fetch raw data from /configs endpoint using the provided fetchOptions
	data, err := f.fetchRaw("/configs", fetchOptions)
	if err != nil {
		f.logger.Error("Error fetching configs: ", err)
		return nil, err
	}

	// Parse and wrap the response in FFResponseConfigs
	ffResponse, err := dtos.NewFFResponseConfigs(data)
	if err != nil {
		f.logger.Error("Error parsing configs JSON: ", err)
		return nil, err
	}

	f.logger.Debug(fmt.Sprintf("Fetched %d configs from /configs endpoint", len(ffResponse.FeatureFlags())))

	return ffResponse, nil
}

// IsProxy returns false as HTTPConfigsFetcher is not a proxy
func (f *HTTPConfigsFetcher) IsProxy() bool {
	return false
}

// Verify that HTTPConfigsFetcher implements SplitFetcher interface
var _ service.SplitFetcher = (*HTTPConfigsFetcher)(nil)
