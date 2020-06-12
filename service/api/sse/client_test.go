package sse

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
)

func TestStreamingError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}))
	defer ts.Close()

	mocked := &conf.AdvancedConfig{
		StreamingURL: ts.URL,
	}
	mockedClient := NewStreamingClient(mocked, make(chan struct{}), logger)

	err := mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		t.Error("Should not execute callback")
	})
	if err == nil || err.Error() != "Could not connect to streaming" {
		t.Error("Unexpected error")
	}
}

func TestStreamingOk(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelError})
	sseMock, _ := ioutil.ReadFile("../../../testdata/sse.json")
	var mockedData map[string]interface{}
	_ = json.Unmarshal(sseMock, &mockedData)
	mockedStr, _ := json.Marshal(mockedData)

	var mockedClient *StreamingClient

	sseReady := make(chan struct{}, 1)
	var sseReadyReceived int64

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

		go func() {
			time.Sleep(50 * time.Millisecond)
			mockedClient.StopStreaming()
		}()
	}))
	defer ts.Close()

	mocked := &conf.AdvancedConfig{
		StreamingURL: ts.URL,
	}
	mockedClient = NewStreamingClient(mocked, sseReady, logger)

	var result map[string]interface{}

	go func() {
		for {
			select {
			case <-sseReady:
				atomic.AddInt64(&sseReadyReceived, 1)
			default:
			}
		}
	}()

	err := mockedClient.ConnectStreaming("someToken", []string{}, func(e map[string]interface{}) {
		result = e
	})

	if atomic.LoadInt64(&sseReadyReceived) != 1 {
		t.Error("It should be ready")
	}

	if err != nil {
		t.Error("It should not return error")
	}
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

}
