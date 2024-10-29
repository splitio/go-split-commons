package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/api/specs"
	"github.com/splitio/go-toolkit/v5/logging"
)

type httpFetcherBase struct {
	client Client
	logger logging.LoggerInterface
}

func (h *httpFetcherBase) fetchRaw(endpoint string, fetchOptions service.RequestParams) ([]byte, error) {
	data, err := h.client.Get(endpoint, fetchOptions)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// HTTPSplitFetcher struct is responsible for fetching splits from the backend via HTTP protocol
type HTTPSplitFetcher struct {
	httpFetcherBase
	flagSetsFilter string
	specVersion    *string
}

// NewHTTPSplitFetcher instantiates and return an HTTPSplitFetcher
func NewHTTPSplitFetcher(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) service.SplitFetcher {
	return &HTTPSplitFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
		flagSetsFilter: strings.Join(cfg.FlagSetsFilter, ","),
		specVersion:    specs.Match(cfg.FlagsSpecVersion),
	}
}

// Fetch makes an http call to the split backend and returns the list of updated splits
func (f *HTTPSplitFetcher) Fetch(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
	fetchOptions.WithFlagSetsFilter(f.flagSetsFilter).WithSpecVersion(f.specVersion)
	data, err := f.fetchRaw("/splitChanges", fetchOptions)
	if err != nil {
		f.logger.Error("Error fetching split changes ", err)
		return nil, err
	}

	var splitChangesDto dtos.SplitChangesDTO
	err = json.Unmarshal(data, &splitChangesDto)
	if err != nil {
		f.logger.Error("Error parsing split changes JSON ", err)
		return nil, err
	}

	return &splitChangesDto, nil
}

// HTTPSegmentFetcher struct is responsible for fetching segment by name from the API via HTTP method
type HTTPSegmentFetcher struct {
	httpFetcherBase
}

// NewHTTPSegmentFetcher instantiates and returns a new HTTPSegmentFetcher.
func NewHTTPSegmentFetcher(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) service.SegmentFetcher {
	return &HTTPSegmentFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
	}
}

// Fetch issues a GET request to the split backend and returns the contents of a particular segment
func (f *HTTPSegmentFetcher) Fetch(segmentName string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
	var bufferQuery bytes.Buffer
	bufferQuery.WriteString("/segmentChanges/")
	bufferQuery.WriteString(segmentName)

	data, err := f.fetchRaw(bufferQuery.String(), fetchOptions)
	if err != nil {
		f.logger.Error(err.Error())
		return nil, err
	}
	var segmentChangesDto dtos.SegmentChangesDTO
	err = json.Unmarshal(data, &segmentChangesDto)
	if err != nil {
		f.logger.Error("Error parsing segment changes JSON for segment ", segmentName, err)
		return nil, err
	}

	return &segmentChangesDto, nil
}

type LargeSegmentFetcher interface {
	Fetch(name string, fetchOptions *service.SegmentRequestParams) *dtos.LargeSegmentResponse
}

type HTTPLargeSegmentFetcher struct {
	httpFetcherBase
	membershipVersion *string
	httpClient        *http.Client
}

// NewHTTPLargeSegmentsFetcher
func NewHTTPLargeSegmentFetcher(apikey string, memVersion string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) service.LargeSegmentFetcher {
	return &HTTPLargeSegmentFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
		membershipVersion: &cfg.MembershipVersion,
		httpClient:        &http.Client{},
	}
}

func (f *HTTPLargeSegmentFetcher) Fetch(name string, fetchOptions *service.SegmentRequestParams) *dtos.LargeSegmentResponse {
	var bufferQuery bytes.Buffer
	bufferQuery.WriteString("/largeSegmentDefinition/")
	bufferQuery.WriteString(name)

	data, err := f.client.Get(bufferQuery.String(), fetchOptions)
	if err != nil {
		return &dtos.LargeSegmentResponse{
			Error: err,
			Retry: true,
		}
	}

	var rfeDTO dtos.RfeDTO
	err = json.Unmarshal(data, &rfeDTO)
	if err != nil {
		return &dtos.LargeSegmentResponse{
			Error: fmt.Errorf("error getting Request for Export: %s. %w", name, err),
			Retry: true,
		}
	}

	if time.Now().UnixMilli() > rfeDTO.ExpiresAt {
		return &dtos.LargeSegmentResponse{
			Error: fmt.Errorf("URL expired"),
			Retry: true,
		}
	}

	var toReturn dtos.LargeSegmentDTO
	retry, err := f.downloadAndParse(rfeDTO, &toReturn)
	if err != nil {
		return &dtos.LargeSegmentResponse{
			Error: err,
			Retry: retry,
		}
	}

	return &dtos.LargeSegmentResponse{
		Data:  &toReturn,
		Error: nil,
	}
}

func (f *HTTPLargeSegmentFetcher) downloadAndParse(rfe dtos.RfeDTO, tr *dtos.LargeSegmentDTO) (bool, error) {
	method := rfe.Params.Method
	if len(method) == 0 {
		method = http.MethodGet
	}

	req, _ := http.NewRequest(method, rfe.Params.URL, bytes.NewBuffer(rfe.Params.Body))
	req.Header = rfe.Params.Headers
	response, err := f.httpClient.Do(req)
	if err != nil {
		return true, err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return true,
			dtos.HTTPError{
				Code:    response.StatusCode,
				Message: response.Status,
			}
	}
	defer response.Body.Close()

	switch rfe.Format {
	case Csv:
		return csvReader(response, rfe, tr)
	default:
		return false, fmt.Errorf("unsupported file format")
	}
}
