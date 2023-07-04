// Package api contains all functions and dtos Split APIs
package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/splitio/go-split-commons/v5/conf"
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

var splitsMock, _ = ioutil.ReadFile("../../testdata/splits_mock.json")
var splitMock, _ = ioutil.ReadFile("../../testdata/split_mock.json")
var segmentMock, _ = ioutil.ReadFile("../../testdata/segment_mock.json")

func TestSpitChangesFetch(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cache-Control") != "no-cache" {
			t.Error("wrong cache control header")
		}
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

	splitChangesDTO, err := splitFetcher.Fetch(-1, &service.FetchOptions{CacheControlHeaders: true})
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
		cacheControl = r.Header.Get("Cache-Control")
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

	_, err := splitFetcher.Fetch(-1, &service.FetchOptions{CacheControlHeaders: true})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "no-cache" {
		t.Error("Wrong header sent")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	_, err = splitFetcher.Fetch(-1, &service.FetchOptions{CacheControlHeaders: false})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "" {
		t.Error("Cache control should not be present")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	expectedTill := int64(10000)
	_, err = splitFetcher.Fetch(-1, &service.FetchOptions{CacheControlHeaders: true, ChangeNumber: &expectedTill})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "no-cache" {
		t.Error("Wrong header sent")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Get("till") != "10000" {
		t.Error("Expected to have till")
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

	_, err := splitFetcher.Fetch(-1, &service.FetchOptions{CacheControlHeaders: false})
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

	segmentFetched, err := segmentFetcher.Fetch("employees", -1, &service.FetchOptions{CacheControlHeaders: false})
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

	_, err := segmentFetcher.Fetch("employees", -1, &service.FetchOptions{CacheControlHeaders: false})
	if err == nil {
		t.Error("Error expected but not found")
	}
}

func TestSegmentChangesFetchWithFlagOptions(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var cacheControl string
	var queryParams url.Values

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheControl = r.Header.Get("Cache-Control")
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

	_, err := segmentFetcher.Fetch("employees", -1, &service.FetchOptions{CacheControlHeaders: true})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "no-cache" {
		t.Error("Wrong header sent")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	_, err = segmentFetcher.Fetch("employees", -1, &service.FetchOptions{CacheControlHeaders: false})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "" {
		t.Error("Cache control should not be present")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Has("till") {
		t.Error("Expected to not have till")
	}
	expectedTill := int64(10000)
	_, err = segmentFetcher.Fetch("employees", -1, &service.FetchOptions{CacheControlHeaders: true, ChangeNumber: &expectedTill})
	if err != nil {
		t.Error(err)
	}
	if cacheControl != "no-cache" {
		t.Error("Wrong header sent")
	}
	if !queryParams.Has("since") {
		t.Error("Expected to have since")
	}
	if queryParams.Get("till") != "10000" {
		t.Error("Expected to have till")
	}
}
