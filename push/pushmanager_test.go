package push

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
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/processor"
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestPushManagerError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 1000, SplitUpdateQueueSize: 5000,
	}
	_, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced)
	if err == nil || err.Error() != "Small size of segmentQueue" {
		t.Error("It should return err", err)
	}

	advanced.SegmentUpdateQueueSize = 5000
	advanced.SplitUpdateQueueSize = 1000
	_, err = NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced)
	if err == nil || err.Error() != "Small size of splitQueue" {
		t.Error("It should return err", err)
	}
}

func TestPushManager(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}
	push, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced)
	if err != nil {
		t.Error("It should not return err")
	}
	if push.IsRunning() {
		t.Error("It should not be running")
	}
}

func TestPushLogic(t *testing.T) {
	var shouldReceiveSegmentChange int64
	var shouldReceiveSplitChange int64

	logger := logging.NewLogger(&logging.LoggerOptions{LogLevel: logging.LevelDebug})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processorTest, err := processor.NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return error")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		for i := 0; i <= 1; i++ {
			switch i {
			case 0:
				sseMock, _ := ioutil.ReadFile("../testdata/sse.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 1:
				sseMock, _ := ioutil.ReadFile("../testdata/sse2.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			}
			flusher.Flush()
		}
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	sseReady := make(chan struct{}, 1)
	sseError := make(chan error, 1)
	mockedClient := sse.NewStreamingClient(advanced, sseReady, sseError, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		atomic.AddInt64(&shouldReceiveSegmentChange, 1)
		if segmentName != "TEST_SEGMENT" {
			t.Error("Wrong segment name received")
		}
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		atomic.AddInt64(&shouldReceiveSplitChange, 1)
		if *till != 1591996685190 {
			t.Error("Wrong changeNumber received")
		}
		return nil
	}, logger)

	mockedPush := PushManager{
		sseClient:     mockedClient,
		processor:     processorTest,
		logger:        logger,
		segmentWorker: segmentWorker,
		splitWorker:   splitWorker,
		sseReady:      sseReady,
		sseError:      sseError,
	}
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	err = mockedPush.Start("some", []string{})
	if err != nil {
		t.Error("It should not be error")
	}

	time.Sleep(1 * time.Second)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	if atomic.LoadInt64(&shouldReceiveSegmentChange) != 1 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveSplitChange) != 1 {
		t.Error("It should be one")
	}
}
