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

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
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

	streamingStatus := make(chan int, 1)
	mockedClient := NewStreamingClient(mocked, streamingStatus, logger)

	go mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		t.Error("Should not execute callback")
	})
	msg := <-streamingStatus
	if msg != sse.ErrorInternal {
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

	streamingStatus := make(chan int, 1)

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
	mockedClient := NewStreamingClient(mocked, streamingStatus, logger)

	var result map[string]interface{}
	mutexTest := sync.RWMutex{}
	go mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		defer mutexTest.Unlock()
		mutexTest.Lock()
		result = e
	})

	status := <-streamingStatus
	if status != sse.OK {
		t.Error("It should not be error")

	}
	if !mockedClient.IsRunning() {
		t.Error("It should be running")
	}

	time.Sleep(100 * time.Millisecond)

	mockedClient.StopStreaming(true)
	if mockedClient.IsRunning() {
		t.Error("It should not be running")
	}

	mutexTest.RLock()
	if result["id"] != mockedData["id"] {
		t.Error("Unexpected id")
	}
	if result["clientId"] != mockedData["clientId"] {
		t.Error("Unexpected clientId")
	}
	if result["timestamp"] != mockedData["timestamp"] {
		t.Error("Unexpected timestamp")
	}
	if result["encoding"] != mockedData["encoding"] {
		t.Error("Unexpected encoding")
	}
	if result["channel"] != mockedData["channel"] {
		t.Error("Unexpected channel")
	}
	if result["data"] != mockedData["data"] {
		t.Error("Unexpected data")
	}
	mutexTest.RUnlock()
}
