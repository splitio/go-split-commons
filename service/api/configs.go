package api

import (
	"encoding/json"
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
func NewHTTPConfigsFetcher(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) *HTTPConfigsFetcher {
	return &HTTPConfigsFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
	}
}

// Fetch makes an HTTP call to the /configs endpoint and returns the converted RuleChangesDTO
func (f *HTTPConfigsFetcher) Fetch(since int64) (*dtos.RuleChangesDTO, error) {
	// Build query parameters
	params := &service.FlagRequestParams{}
	params.WithChangeNumber(since)

	// Fetch raw data from /configs endpoint
	data, err := f.fetchRaw("/configs", params)
	if err != nil {
		f.logger.Error("Error fetching configs: ", err)
		return nil, err
	}

	// Unmarshal into ConfigsResponseDTO
	var configsResponse dtos.ConfigsResponseDTO
	err = json.Unmarshal(data, &configsResponse)
	if err != nil {
		f.logger.Error("Error parsing configs JSON: ", err)
		return nil, err
	}

	// Convert ConfigDTOs to SplitDTOs
	splits := make([]dtos.SplitDTO, 0, len(configsResponse.Configs.Configs))
	for _, config := range configsResponse.Configs.Configs {
		splitDTO := dtos.ConvertConfigToSplit(config)
		splits = append(splits, splitDTO)
	}

	f.logger.Debug(fmt.Sprintf("Fetched %d configs, converted to %d splits", len(configsResponse.Configs.Configs), len(splits)))

	// Build and return RuleChangesDTO
	ruleChanges := &dtos.RuleChangesDTO{
		FeatureFlags: dtos.FeatureFlagsDTO{
			Since:  configsResponse.Configs.Since,
			Till:   configsResponse.Configs.Till,
			Splits: splits,
		},
		RuleBasedSegments: dtos.RuleBasedSegmentsDTO{
			Since:             configsResponse.Configs.Since,
			Till:              configsResponse.Configs.Till,
			RuleBasedSegments: configsResponse.RBS,
		},
	}

	return ruleChanges, nil
}
