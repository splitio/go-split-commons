// Package api contains all functions and dtos Split APIs
package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/v4/conf"
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/service"
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
