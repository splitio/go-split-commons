// Package api contains all functions and dtos Split APIs
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/conf"
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/api/specs"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

var splitsMock, _ = ioutil.ReadFile("../../testdata/splits_mock.json")
var oldSplitMock, _ = ioutil.ReadFile("../../testdata/old_splits_mock.json")
var splitMock, _ = ioutil.ReadFile("../../testdata/split_mock.json")
var segmentMock, _ = ioutil.ReadFile("../../testdata/segment_mock.json")

func TestSpitChangesFetch11(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, CacheControlNoCache, r.Header.Get(CacheControlHeader), "wrong cache control header")
		assert.Equal(t, "123456", r.URL.Query().Get("since"), "wrong since")
		assert.Equal(t, "", r.URL.Query().Get("till"), "wrong till")
		assert.Equal(t, "", r.URL.Query().Get("sets"), "wrong sets")
		assert.Equal(t, specs.FLAG_V1_1, r.URL.Query().Get("s"), "wrong spec")
		assert.Equal(t, "s=1.1&since=123456&rbSince=123456", r.URL.RawQuery, "wrong query params")
		fmt.Fprintln(w, fmt.Sprintf(string(oldSplitMock), splitMock))
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL:        ts.URL,
			SdkURL:           ts.URL,
			FlagsSpecVersion: specs.FLAG_V1_1,
		},
		logger,
		dtos.Metadata{},
	)

	splitChangesDTO, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithChangeNumberRB(123456))
	assert.Equal(t, nil, err, err)
	assert.False(t, splitChangesDTO.FFTill() != 1491244291288 ||
		splitChangesDTO.FeatureFlags()[0].Name != "DEMO_MURMUR2", "DTO mal formed")
	assert.NotEqual(t, nil, splitChangesDTO.FeatureFlags()[0].Configurations, "DTO mal formed")
	assert.Equal(t, "", splitChangesDTO.FeatureFlags()[0].Configurations["of"], "DTO mal formed")
	assert.Equal(t, "{\"color\": \"blue\",\"size\": 13}", splitChangesDTO.FeatureFlags()[0].Configurations["on"], "DTO mal formed")
}

func TestSpitChangesFetch(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, CacheControlNoCache, r.Header.Get(CacheControlHeader), "wrong cache control header")
		assert.Equal(t, "123456", r.URL.Query().Get("since"), "wrong since")
		assert.Equal(t, "", r.URL.Query().Get("till"), "wrong till")
		assert.Equal(t, "", r.URL.Query().Get("sets"), "wrong sets")
		assert.Equal(t, specs.FLAG_V1_3, r.URL.Query().Get("s"), "wrong spec")
		assert.Equal(t, "s=1.3&since=123456&rbSince=123456", r.URL.RawQuery, "wrong query params")
		fmt.Fprintln(w, fmt.Sprintf(string(splitsMock), splitMock))
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL:        ts.URL,
			SdkURL:           ts.URL,
			FlagsSpecVersion: specs.FLAG_V1_3,
		},
		logger,
		dtos.Metadata{},
	)

	splitChangesDTO, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithChangeNumberRB(123456))
	assert.Equal(t, nil, err, err)
	assert.False(t, splitChangesDTO.FFTill() != 1491244291288 ||
		splitChangesDTO.FeatureFlags()[0].Name != "DEMO_MURMUR2", "DTO mal formed")
	assert.NotEqual(t, nil, splitChangesDTO.FeatureFlags()[0].Configurations, "DTO mal formed")
	assert.Equal(t, "", splitChangesDTO.FeatureFlags()[0].Configurations["of"], "DTO mal formed")
	assert.Equal(t, "{\"color\": \"blue\",\"size\": 13}", splitChangesDTO.FeatureFlags()[0].Configurations["on"], "DTO mal formed")
}

func TestSpitChangesFetchWithFlagOptions(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var cacheControl string
	var queryParams url.Values

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheControl = r.Header.Get(CacheControlHeader)
		queryParams = r.URL.Query()
		fmt.Fprintln(w, fmt.Sprintf(string(splitsMock), splitMock))
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
		dtos.Metadata{},
	)

	_, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456))
	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.False(t, queryParams.Has("till"), "Expected to not have till")

	expectedTill := int64(10000)
	_, err = splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithTill(expectedTill))
	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.Equal(t, "10000", queryParams.Get("till"), "Expected to have till")
}

func TestSpitChangesFetchWithFlagSetsFilter(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var cacheControl string
	var queryParams url.Values

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheControl = r.Header.Get(CacheControlHeader)
		queryParams = r.URL.Query()
		fmt.Fprintln(w, fmt.Sprintf(string(splitsMock), splitMock))
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL:      ts.URL,
			SdkURL:         ts.URL,
			FlagSetsFilter: []string{"one", "two"},
		},
		logger,
		dtos.Metadata{},
	)

	_, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456))
	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.False(t, queryParams.Has("till"), "Expected to not have till")
	assert.True(t, queryParams.Has("sets"), "Expected to  have sets")

	asString := queryParams.Get("sets")
	asArray := strings.Split(asString, ",")
	setsToTest := make(map[string]struct{})
	for _, featureFlag := range asArray {
		setsToTest[featureFlag] = struct{}{}
	}

	_, ok := setsToTest["one"]
	assert.True(t, ok, "one key not found in setsToTest")
	_, ok1 := setsToTest["two"]
	assert.True(t, ok1, "two key not found in setsToTest")
}

func TestSpitChangesFetchWithAll(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var cacheControl string
	var queryParams url.Values

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheControl = r.Header.Get(CacheControlHeader)
		queryParams = r.URL.Query()
		fmt.Fprintln(w, fmt.Sprintf(string(splitsMock), splitMock))

		if r.URL.RawQuery != "s=1.1&since=123456&rbSince=123456&sets=one%2Ctwo&till=10000" {
			t.Error("wrong query params")
		}
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL:        ts.URL,
			SdkURL:           ts.URL,
			FlagSetsFilter:   []string{"one", "two"},
			FlagsSpecVersion: specs.FLAG_V1_1,
		},
		logger,
		dtos.Metadata{},
	)

	_, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithTill(10000).WithChangeNumberRB(123456))

	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.True(t, queryParams.Has("till"), "Expected to have till")
	assert.True(t, queryParams.Has("sets"), "Expected to have sets")
	assert.Equal(t, specs.FLAG_V1_1, queryParams.Get("s"), "Expected to have spec")

	asString := queryParams.Get("sets")
	asArray := strings.Split(asString, ",")
	setsToTest := make(map[string]struct{})
	for _, featureFlag := range asArray {
		setsToTest[featureFlag] = struct{}{}
	}
	_, ok := setsToTest["one"]
	assert.True(t, ok, "one key not found in setsToTest")
	_, ok1 := setsToTest["two"]
	assert.True(t, ok1, "two key not found in setsToTest")
}

func TestSpitChangesFetchHTTPError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}))
	defer ts.Close()

	splitFetcher := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
		dtos.Metadata{},
	)

	_, err := splitFetcher.Fetch(service.MakeFlagRequestParams())
	assert.NotEqual(t, nil, err, "Error expected but not found", err)
}

func TestSegmentChangesFetch(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(segmentMock))
	}))
	defer ts.Close()

	segmentFetcher := NewHTTPSegmentFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
		dtos.Metadata{},
	)

	segmentFetched, err := segmentFetcher.Fetch("employees", service.MakeSegmentRequestParams())
	assert.Equal(t, nil, err, "Error fetching segment", err)
	assert.Equal(t, "employees", segmentFetched.Name, "Fetched segment mal-formed")
}

func TestSegmentChangesFetchHTTPError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}))
	defer ts.Close()

	segmentFetcher := NewHTTPSegmentFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
		dtos.Metadata{},
	)

	_, err := segmentFetcher.Fetch("employees", service.MakeSegmentRequestParams())
	assert.NotEqual(t, nil, err, "Error expected but not found", err)
}

func TestSegmentChangesFetchWithFlagOptions(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var cacheControl string
	var queryParams url.Values

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheControl = r.Header.Get(CacheControlHeader)
		queryParams = r.URL.Query()
		fmt.Fprintln(w, string(segmentMock))
	}))
	defer ts.Close()

	segmentFetcher := NewHTTPSegmentFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
		dtos.Metadata{},
	)

	_, err := segmentFetcher.Fetch("employees", service.MakeSegmentRequestParams().WithChangeNumber(123456))
	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.False(t, queryParams.Has("till"), "Expected to. not have till")

	expectedTill := int64(10000)
	_, err = segmentFetcher.Fetch("employees", service.MakeSegmentRequestParams().WithTill(expectedTill).WithChangeNumber(123456))
	assert.Equal(t, nil, err, err)
	assert.Equal(t, CacheControlNoCache, cacheControl, "Wrong header sent")
	assert.Equal(t, "123456", queryParams.Get("since"), "Expected to have since")
	assert.Equal(t, "10000", queryParams.Get("till"), "Expected to have till")
}

// Large Segments tests
func TestFetchCsvFormatHappyPath(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	test_csv, _ := os.ReadFile("testdata/large_segment_test.csv")
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(test_csv)
	}))
	defer fileServer.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(buildLargeSegmentRFDResponseDTO(fileServer.URL))
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	if err != nil {
		t.Error("Error should be nil")
	}

	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, nil, err, err)
	assert.Equal(t, "large_segment_test", result.Name, "LS name should be large_segment_test. Actual: ", result.Name)
	assert.Equal(t, 1500, len(result.Keys), "Keys lenght should be 1500. Actual: ", len(result.Keys))
}

func TestFetchCsvMultipleColumns(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	test_csv, _ := os.ReadFile("testdata/ls_wrong.csv")
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(test_csv)
	}))
	defer fileServer.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(buildLargeSegmentRFDResponseDTO(fileServer.URL))
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	assert.Equal(t, nil, err, err)
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, "unssuported file content. The file has multiple columns", err.Error(), "Error should not be nil")
	assert.Equal(t, (*dtos.LargeSegment)(nil), result, "Response.Data should be nil")
}

func TestFetchCsvFormatWithOtherVersion(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	test_csv, _ := os.ReadFile("testdata/large_segment_test.csv")
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(test_csv)
	}))
	defer fileServer.Close()

	response := buildLargeSegmentRFDResponseDTO(fileServer.URL)
	response.SpecVersion = "1111.0"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(response)
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	assert.Equal(t, nil, err, err)
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, "unsupported csv version 1111.0", err.Error(), "Error should not be nil")
	assert.Equal(t, (*dtos.LargeSegment)(nil), result, "Response.Data should be nil")
}

func TestFetchUnknownFormat(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	test_csv, _ := os.ReadFile("testdata/large_segment_test.csv")
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(test_csv)
	}))
	defer fileServer.Close()

	response := buildLargeSegmentRFDResponseDTO(fileServer.URL)
	response.RFD.Data.Format = Unknown
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(response)
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	assert.Equal(t, nil, err, err)
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, "unsupported file format", err.Error(), "Error should not be nil")
	assert.Equal(t, (*dtos.LargeSegment)(nil), result, "Response.Data should be nil")
}

func TestFetchAPIError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	rfe, err := fetcher.Fetch("large_segment_test", &service.SegmentRequestParams{})
	assert.Equal(t, "500 Internal Server Error", err.Error(), "Error should be 500")
	if rfe != nil {
		t.Error("RequestForExport should be nil")
	}
}

func TestFetchDownloadServerError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}))
	defer fileServer.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(buildLargeSegmentRFDResponseDTO(fileServer.URL))
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	assert.Equal(t, nil, err, "Error should be nil")
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, "500 Internal Server Error", err.Error(), "Error should not be nil")
	assert.Equal(t, (*dtos.LargeSegment)(nil), result, "Response.Data should be nil")
}

func TestFetchWithPost(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	test_csv, _ := os.ReadFile("testdata/large_segment_test.csv")
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(test_csv)
	}))
	defer fileServer.Close()

	response := buildLargeSegmentRFDResponseDTO(fileServer.URL)
	response.RFD.Params.Method = http.MethodPost
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := json.Marshal(response)
		w.Write(data)
	}))
	defer ts.Close()

	fetcher := NewHTTPLargeSegmentFetcher(
		"api-key",
		conf.AdvancedConfig{
			SdkURL: ts.URL,
			LargeSegment: &conf.LargeSegmentConfig{
				Version: specs.LARGESEGMENT_V10,
				Enable:  true,
			},
		},
		logger,
		dtos.Metadata{},
	)

	lsName := "large_segment_test"
	lsRfdResponseDTO, err := fetcher.Fetch(lsName, &service.SegmentRequestParams{})
	assert.Equal(t, nil, err, "Error should be nil")
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	assert.Equal(t, nil, err, "Error should be nil")
	assert.Equal(t, "large_segment_test", result.Name, "LS name should be large_segment_test. Actual: ", result.Name)
	assert.Equal(t, 1500, len(result.Keys), "Keys lenght should be 1500. Actual: ", len(result.Keys))
}

func TestIsProxy(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	// Test case 1: Not a proxy (version endpoint returns 200)
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/version", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	splitFetcher1 := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts1.URL,
			SdkURL:    ts1.URL,
		},
		logger,
		dtos.Metadata{},
	).(*HTTPSplitFetcher) // Type assertion to access unexported method

	isProxy := splitFetcher1.isProxy(service.MakeFlagRequestParams())
	assert.False(t, isProxy, "Should not be identified as proxy when version endpoint returns 200")

	// Test case 2: Is a proxy (version endpoint returns 404)
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/version", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts2.Close()

	splitFetcher2 := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts2.URL,
			SdkURL:    ts2.URL,
		},
		logger,
		dtos.Metadata{},
	).(*HTTPSplitFetcher) // Type assertion to access unexported method

	isProxy = splitFetcher2.isProxy(service.MakeFlagRequestParams())
	assert.True(t, isProxy, "Should be identified as proxy when version endpoint returns 404")

	// Test case 3: Not a proxy (version endpoint returns other error code)
	ts3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/version", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts3.Close()

	splitFetcher3 := NewHTTPSplitFetcher(
		"",
		conf.AdvancedConfig{
			EventsURL: ts3.URL,
			SdkURL:    ts3.URL,
		},
		logger,
		dtos.Metadata{},
	).(*HTTPSplitFetcher) // Type assertion to access unexported method

	isProxy = splitFetcher3.isProxy(service.MakeFlagRequestParams())
	assert.False(t, isProxy, "Should not be identified as proxy when version endpoint returns non-404 error")
}

func buildLargeSegmentRFDResponseDTO(url string) dtos.LargeSegmentRFDResponseDTO {
	return dtos.LargeSegmentRFDResponseDTO{
		NotificationType: "LS_NEW_DEFINITION",
		SpecVersion:      specs.LARGESEGMENT_V10,
		ChangeNumber:     100,
		RFD: &dtos.RFD{
			Params: dtos.Params{
				Method: http.MethodGet,
				URL:    url,
			},
			Data: dtos.Data{
				Format:    Csv,
				TotalKeys: 1500,
				FileSize:  100,
				ExpiresAt: time.Now().UnixMilli() + 10000,
			},
		},
	}
}
