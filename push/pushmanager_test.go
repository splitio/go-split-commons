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
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
	sseStatus "github.com/splitio/go-toolkit/sse"
)

func TestPushManagerError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 1000, SplitUpdateQueueSize: 5000,
	}
	_, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int))
	if err == nil || err.Error() != "Small size of segmentQueue" {
		t.Error("It should return err", err)
	}

	advanced.SegmentUpdateQueueSize = 5000
	advanced.SplitUpdateQueueSize = 1000
	_, err = NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int))
	if err == nil || err.Error() != "Small size of splitQueue" {
		t.Error("It should return err", err)
	}
}

func TestPushManager(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}
	push, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int))
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
	var shouldReceiveReady int64
	var shouldReceiveError int64

	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	eventHandler := NewEventHandler(parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		for i := 0; i <= 2; i++ {
			switch i {
			case 0:
				sseMock, _ := ioutil.ReadFile("../testdata/sse.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 1:
				mockedData := make(map[string]interface{})
				mockedData["event"] = "keepalive"
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 2:
				sseMock, _ := ioutil.ReadFile("../testdata/sse2.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 3:
				sseMock, _ := ioutil.ReadFile("../testdata/sseError.json")
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

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

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

	managerStatus := make(chan int, 1)
	mockedPush := PushManager{
		sseClient:       mockedClient,
		eventHandler:    eventHandler,
		logger:          logger,
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
	}
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	go func() {
		msg := <-managerStatus
		switch msg {
		case Ready:
			atomic.AddInt64(&shouldReceiveReady, 1)
		case Error:
			atomic.AddInt64(&shouldReceiveError, 1)
		}
	}()

	mockedPush.Start("some", []string{})

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
	if atomic.LoadInt64(&shouldReceiveReady) != 1 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveError) != 0 {
		t.Error("It should not return error")
	}
}

func TestPushError(t *testing.T) {
	var shouldReceiveSegmentChange int64
	var shouldReceiveSplitChange int64
	var shouldReceiveReady int64
	var shouldReceiveError int64

	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	eventHandler := NewEventHandler(parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		flusher.Flush()
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		atomic.AddInt64(&shouldReceiveSegmentChange, 1)
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		atomic.AddInt64(&shouldReceiveSplitChange, 1)
		return nil
	}, logger)

	managerStatus := make(chan int, 1)
	mockedPush := PushManager{
		sseClient:       mockedClient,
		eventHandler:    eventHandler,
		logger:          logger,
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
	}
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	go func() {
		for {
			msg := <-managerStatus
			switch msg {
			case Ready:
				atomic.AddInt64(&shouldReceiveReady, 1)
			case Error:
				atomic.AddInt64(&shouldReceiveError, 1)
			}
		}
	}()

	mockedPush.Start("some", []string{})
	time.Sleep(100 * time.Millisecond)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorKeepAlive
	time.Sleep(100 * time.Millisecond)

	mockedPush.Start("some", []string{})
	time.Sleep(100 * time.Millisecond)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorUnexpected
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt64(&shouldReceiveSegmentChange) != 0 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveSplitChange) != 0 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveReady) != 2 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveError) != 2 {
		t.Error("It should return error")
	}
}
