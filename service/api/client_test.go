// Package api contains all functions and dtos Split APIs
package api

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

func TestGet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		if r.Header.Get("h1") != "v1" {
			t.Error("wrong header")
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpClient := NewHTTPClient("", conf.AdvancedConfig{}, ts.URL, logger, dtos.Metadata{})
	txt, errg := httpClient.Get("/", map[string]string{"h1": "v1"})
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
