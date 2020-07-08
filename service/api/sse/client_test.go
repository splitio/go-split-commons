package sse

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
)

func TestStreamingError(t *testing.T) {
	var sseErrorReceived int64
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

	go func() {
		msg := <-streamingStatus
		atomic.AddInt64(&sseErrorReceived, 1)
		if msg != sse.ErrorInternal {
			t.Error("Unexpected error")
		}
	}()

	mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		t.Error("Should not execute callback")
	})

	time.Sleep(200 * time.Millisecond)

	if mockedClient.IsRunning() {
		t.Error("It should not be running")
	}
	if atomic.LoadInt64(&sseErrorReceived) != 1 {
		t.Error("It should be one")
	}
}

func TestStreamingOk(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	sseMock, _ := ioutil.ReadFile("../../../testdata/sse.json")
	var mockedData map[string]interface{}
	_ = json.Unmarshal(sseMock, &mockedData)
	mockedStr, _ := json.Marshal(mockedData)

	streamingStatus := make(chan int, 1)
	var sseReadyReceived int64
	var sseErrorReceived int64

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
	mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		defer mutexTest.Unlock()
		mutexTest.Lock()
		result = e
	})

	go func() {
		for {
			status := <-streamingStatus
			switch status {
			case sse.OK:
				atomic.AddInt64(&sseReadyReceived, 1)
			default:
				atomic.AddInt64(&sseErrorReceived, 1)
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	if !mockedClient.IsRunning() {
		t.Error("It should be running")
	}

	mockedClient.StopStreaming()
	time.Sleep(1 * time.Second)
	if mockedClient.IsRunning() {
		t.Error("It should not be running")
	}
	if atomic.LoadInt64(&sseErrorReceived) != 0 {
		t.Error("It should not have error")
	}
	if atomic.LoadInt64(&sseReadyReceived) != 1 {
		t.Error("It should be ready")
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
