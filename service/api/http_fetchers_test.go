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
)

var splitsMock, _ = ioutil.ReadFile("../../testdata/splits_mock.json")
var splitMock, _ = ioutil.ReadFile("../../testdata/split_mock.json")
var segmentMock, _ = ioutil.ReadFile("../../testdata/segment_mock.json")

func TestSpitChangesFetch(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(CacheControlHeader) != CacheControlNoCache {
			t.Error("wrong cache control header")
		}
		if r.URL.Query().Get("since") != "123456" {
			t.Error("wrong since")
		}
		if r.URL.Query().Get("till") != "" {
			t.Error("wrong till")
		}
		if r.URL.Query().Get("sets") != "" {
			t.Error("wrong sets")
		}
		if r.URL.Query().Get("s") != specs.FLAG_V1_1 {
			t.Error("wrong spec")
		}
		if r.URL.RawQuery != "s=1.1&since=123456&rbSince=123456" {
			t.Error("wrong query params")
		}
		fmt.Fprintln(w, fmt.Sprintf(string(splitsMock), splitMock))
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
	if err != nil {
		t.Error(err)
	}

	if splitChangesDTO.FeatureFlags.Till != 1491244291288 ||
		splitChangesDTO.FeatureFlags.Splits[0].Name != "DEMO_MURMUR2" {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.FeatureFlags.Splits[0].Configurations == nil {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.FeatureFlags.Splits[0].Configurations["of"] != "" {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.FeatureFlags.Splits[0].Configurations["on"] != "{\"color\": \"blue\",\"size\": 13}" {
		t.Error("DTO mal formed")
	}
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
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	expectedTill := int64(10000)
	_, err = splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithTill(expectedTill))
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if queryParams.Get("till") != "10000" {
		t.Error("Expected to have till")
	}
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
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	if !queryParams.Has("sets") {
		t.Error("Expected to have sets")
	}
	asString := queryParams.Get("sets")
	asArray := strings.Split(asString, ",")
	setsToTest := make(map[string]struct{})
	for _, featureFlag := range asArray {
		setsToTest[featureFlag] = struct{}{}
	}
	if _, ok := setsToTest["one"]; !ok {
		t.Error("Expected one to be present")
	}
	if _, ok := setsToTest["two"]; !ok {
		t.Error("Expected two to be present")
	}
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
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if !queryParams.Has("till") {
		t.Error("Expected to have till")
	}
	if !queryParams.Has("sets") {
		t.Error("Expected to have sets")
	}
	if queryParams.Get("s") != specs.FLAG_V1_1 {
		t.Error("Expected to have spec")
	}
	asString := queryParams.Get("sets")
	asArray := strings.Split(asString, ",")
	setsToTest := make(map[string]struct{})
	for _, featureFlag := range asArray {
		setsToTest[featureFlag] = struct{}{}
	}
	if _, ok := setsToTest["one"]; !ok {
		t.Error("Expected one to be present")
	}
	if _, ok := setsToTest["two"]; !ok {
		t.Error("Expected two to be present")
	}
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
	if err == nil {
		t.Error("Error expected but not found")
	}
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
	if err != nil {
		t.Error("Error fetching segment", err)
		return
	}
	if segmentFetched.Name != "employees" {
		t.Error("Fetched segment mal-formed")
	}
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
	if err == nil {
		t.Error("Error expected but not found")
	}
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
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	expectedTill := int64(10000)
	_, err = segmentFetcher.Fetch("employees", service.MakeSegmentRequestParams().WithTill(expectedTill).WithChangeNumber(123456))
	if err != nil {
		t.Error(err)
	}
	if cacheControl != CacheControlNoCache {
		t.Error("Wrong header sent")
	}
	if queryParams.Get("since") != "123456" {
		t.Error("Expected to have since")
	}
	if queryParams.Get("till") != "10000" {
		t.Error("Expected to have till")
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}

	if result.Name != "large_segment_test" {
		t.Error("LS name should be large_segment_test. Actual: ", result.Name)
	}

	if len(result.Keys) != 1500 {
		t.Error("Keys lenght should be 1500. Actual: ", len(result.Keys))
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	if err.Error() != "unssuported file content. The file has multiple columns" {
		t.Error("Error should not be nil")
	}

	if result != (*dtos.LargeSegment)(nil) {
		t.Error("Response.Data should be nil")
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	if err.Error() != "unsupported csv version 1111.0" {
		t.Error("Error should not be nil")
	}

	if result != (*dtos.LargeSegment)(nil) {
		t.Error("Response.Data should be nil")
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	if err.Error() != "unsupported file format" {
		t.Error("Error should not be nil")
	}

	if result != (*dtos.LargeSegment)(nil) {
		t.Error("Response.Data should be nil")
	}
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
	if err.Error() != "500 Internal Server Error" {
		t.Error("Error should be 500")
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	if err.Error() != "500 Internal Server Error" {
		t.Error("Error should not be nil")
	}

	if result != (*dtos.LargeSegment)(nil) {
		t.Error("Response.Data should be nil")
	}
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
	if err != nil {
		t.Error("Error should be nil")
	}
	result, err := fetcher.DownloadFile(lsName, lsRfdResponseDTO)
	if err != nil {
		t.Error("Error shuld be nil")
	}

	if result.Name != "large_segment_test" {
		t.Error("LS name should be large_segment_test. Actual: ", result.Name)
	}

	if len(result.Keys) != 1500 {
		t.Error("Keys lenght should be 1500. Actual: ", len(result.Keys))
	}
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
