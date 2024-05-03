// Package api contains all functions and dtos Split APIs
package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/splitio/go-split-commons/v5/conf"
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
	"github.com/splitio/go-split-commons/v5/service/api/specs"
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
		if r.URL.RawQuery != "s=1.1&since=123456" {
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

	splitChangesDTO, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456))
	if err != nil {
		t.Error(err)
	}

	if splitChangesDTO.Till != 1491244291288 ||
		splitChangesDTO.Splits[0].Name != "DEMO_MURMUR2" {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.Splits[0].Configurations == nil {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.Splits[0].Configurations["of"] != "" {
		t.Error("DTO mal formed")
	}

	if splitChangesDTO.Splits[0].Configurations["on"] != "{\"color\": \"blue\",\"size\": 13}" {
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

		if r.URL.RawQuery != "s=1.1&since=123456&sets=one%2Ctwo&till=10000" {
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

	_, err := splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(123456).WithTill(10000))
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
