// Package api contains all functions and dtos Split APIs
package api

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/v9/conf"
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/service"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestGet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		if r.Header.Get(CacheControlHeader) != CacheControlNoCache {
			t.Error("wrong header")
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/", service.MakeFlagRequestParams())
	if errg != nil {
		t.Error(errg)
	}

	if string(txt) != "Hello, client\n" {
		t.Error("Given message failed ")
	}
}

func TestGetSplitFetchOptions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		if r.Header.Get(CacheControlHeader) != CacheControlNoCache {
			t.Error("wrong header")
		}
		expected := "/test?s=v1.1&since=123456&rbSince=123456&sets=filter&till=2345"
		if r.URL.String() != expected {
			t.Error("wrong query params, expected ", expected, "got", r.URL.String())
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/test", service.MakeFlagRequestParams().WithChangeNumber(123456).WithTill(2345).WithFlagSetsFilter("filter").WithSpecVersion(common.StringRef("v1.1")).WithChangeNumberRB(123456))
	if errg != nil {
		t.Error(errg)
	}

	if string(txt) != "Hello, client\n" {
		t.Error("Given message failed ")
	}
}

func TestGetSegmentRequestParams(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		if r.Header.Get(CacheControlHeader) != CacheControlNoCache {
			t.Error("wrong header")
		}
		expected := "/test?since=123456&till=2345"
		if r.URL.String() != expected {
			t.Error("wrong query params, expected ", expected, "got", r.URL.String())
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/test", service.MakeSegmentRequestParams().WithChangeNumber(123456).WithTill(2345))
	if errg != nil {
		t.Error(errg)
	}

	if string(txt) != "Hello, client\n" {
		t.Error("Given message failed ")
	}
}

func TestGetAuthOptions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		if r.Header.Get(CacheControlHeader) != CacheControlNoCache {
			t.Error("wrong header")
		}
		expected := "/test?s=v1.1"
		if r.URL.String() != expected {
			t.Error("wrong query params, expected ", expected, "got", r.URL.String())
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/test", service.MakeAuthRequestParams(common.StringRef("v1.1")))
	if errg != nil {
		t.Error(errg)
	}

	if string(txt) != "Hello, client\n" {
		t.Error("Given message failed ")
	}
}

func TestGetGZIP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Encoding", "gzip")

		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		fmt.Fprintln(gzw, "Hello, client")
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/", nil)
	if errg != nil {
		t.Error(errg)
	}

	if string(txt) != "Hello, client\n" {
		t.Error("Given message failed ")
	}
}

func TestPost(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	errp := httpClient.Post("/", []byte("some text"), nil)
	if errp != nil {
		t.Error(errp)
	}
}
