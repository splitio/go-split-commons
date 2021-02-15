package sse

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-toolkit/v3/logging"
	"github.com/splitio/go-toolkit/v3/sse"
)

func TestStreamingError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}))
	defer ts.Close()

	mocked := &conf.AdvancedConfig{
		StreamingServiceURL: ts.URL,
	}

	mockedClient := NewStreamingClient(mocked, logger)

	streamingStatus := make(chan int, 1)
	go mockedClient.ConnectStreaming("someToken", streamingStatus, []string{}, func(sse.RawEvent) {
		t.Error("Should not execute callback")
	})
	msg := <-streamingStatus
	if msg != StatusConnectionFailed {
		t.Error("Unexpected error")
	}

	if mockedClient.IsRunning() {
		t.Error("It should not be running")
	}
}

func TestStreamingOk(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	sseMock, _ := ioutil.ReadFile("../../../testdata/sse.json")
	var mockedData map[string]interface{}
	_ = json.Unmarshal(sseMock, &mockedData)
	mockedStr, _ := json.Marshal(mockedData)

	streamingStatus := make(chan int, 10)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
	}))
	defer ts.Close()

	mocked := &conf.AdvancedConfig{
		StreamingServiceURL: ts.URL,
	}
	mockedClient := NewStreamingClient(mocked, logger)

	go func() {
		for {
			fmt.Println(<-streamingStatus)
		}
	}()

	var result sse.RawEvent
	mutexTest := sync.RWMutex{}
	go mockedClient.ConnectStreaming("someToken", streamingStatus, []string{}, func(e sse.RawEvent) {
		defer mutexTest.Unlock()
		mutexTest.Lock()
		result = e
	})

	time.Sleep(1000 * time.Millisecond)

	mockedClient.StopStreaming()
	if mockedClient.IsRunning() {
		t.Error("It should not be running")
	}

	mutexTest.RLock()
	if result.Data() != string(mockedStr) {
		t.Error("Unexpected data", result.Data(), "---", string(sseMock))
	}
	mutexTest.RUnlock()
}

func TestStreamingClientDisconnect(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		fmt.Fprint(w, "id: dsad23\n\n")
		flusher.Flush()
	}))
	defer ts.Close()

	mocked := &conf.AdvancedConfig{
		StreamingServiceURL: ts.URL,
	}
	mockedClient := NewStreamingClient(mocked, logger)

	streamingStatus := make(chan int, 1)
	go mockedClient.ConnectStreaming("someToken", streamingStatus, []string{}, func(e sse.RawEvent) {
		if e.ID() != "dsad23" {
			t.Error("invalid id")
		}
	})

	time.Sleep(1000 * time.Millisecond)

	status := <-streamingStatus
	if status != StatusFirstEventOk {
		t.Error("firts status should be event ok. Is: ", status)
	}

	status = <-streamingStatus
	if status != StatusDisconnected {
		t.Error("next status should be disconnected. Is: ", status)
	}
}
