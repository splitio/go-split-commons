package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/splitio/go-split-commons/v7/conf"
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/api/specs"
	"github.com/splitio/go-toolkit/v5/common"
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

func (f *HTTPSplitFetcher) IsProxy() bool {
	_, err := f.fetchRaw("/version", nil)
	if err == nil {
		return false
	}
	httpErr, ok := err.(*dtos.HTTPError)
	return ok && httpErr.Code == http.StatusNotFound
}

// Fetch makes an http call to the split backend and returns the list of updated splits
func (f *HTTPSplitFetcher) Fetch(fetchOptions *service.FlagRequestParams) (dtos.FFResponse, error) {
	fetchOptions.WithFlagSetsFilter(f.flagSetsFilter)
	data, err := f.fetchRaw("/splitChanges", fetchOptions)
	if err != nil {
		f.logger.Error("Error fetching split changes ", err)
		return nil, err
	}

	var splitChangesDto dtos.FFResponse

	if common.StringFromRef(f.specVersion) == specs.FLAG_V1_3 {
		splitChangesDto, err = dtos.NewFFResponseV13(data)
	} else {
		splitChangesDto, err = dtos.NewFFResponseLegacy(data)
	}
	if err != nil {
		f.logger.Error("Error parsing split changes JSON ", err)
		return nil, err
	}

	return splitChangesDto, nil
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
	Fetch(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegment, error)
}

type HTTPLargeSegmentFetcher struct {
	httpFetcherBase
	httpClient *http.Client
}

// NewHTTPLargeSegmentsFetcher
func NewHTTPLargeSegmentFetcher(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) service.LargeSegmentFetcher {
	return &HTTPLargeSegmentFetcher{
		httpFetcherBase: httpFetcherBase{
			client: NewHTTPClient(apikey, cfg, cfg.SdkURL, logger, metadata),
			logger: logger,
		},
		httpClient: &http.Client{},
	}
}

func (f *HTTPLargeSegmentFetcher) Fetch(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
	var bufferQuery bytes.Buffer
	bufferQuery.WriteString("/largeSegmentDefinition/")
	bufferQuery.WriteString(name)

	data, err := f.client.Get(bufferQuery.String(), fetchOptions)
	if err != nil {
		return nil, err
	}

	var rfdResponseDTO *dtos.LargeSegmentRFDResponseDTO
	err = json.Unmarshal(data, &rfdResponseDTO)
	if err != nil {
		return nil, err
	}

	return rfdResponseDTO, nil
}

func (f *HTTPLargeSegmentFetcher) DownloadFile(name string, rfdResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
	rfd := rfdResponseDTO.RFD
	method := rfd.Params.Method
	if len(method) == 0 {
		method = http.MethodGet
	}

	req, _ := http.NewRequest(method, rfd.Params.URL, bytes.NewBuffer(rfd.Params.Body))
	req.Header = rfd.Params.Headers
	response, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, dtos.HTTPError{
			Code:    response.StatusCode,
			Message: response.Status,
		}
	}
	defer response.Body.Close()

	switch rfd.Data.Format {
	case Csv:
		return csvReader(response, name, rfdResponseDTO.SpecVersion, rfdResponseDTO.ChangeNumber, rfd)
	default:
		return nil, fmt.Errorf("unsupported file format")
	}
}
