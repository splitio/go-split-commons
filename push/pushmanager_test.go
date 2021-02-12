package push

/*

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-split-commons/v2/service/api/sse"
	authMocks "github.com/splitio/go-split-commons/v2/service/mocks"
	"github.com/splitio/go-split-commons/v2/storage/mocks"
	"github.com/splitio/go-toolkit/v3/datastructures/set"
	"github.com/splitio/go-toolkit/v3/logging"
)

func isValidChannels(t *testing.T, channelsString string) {
	result := strings.Split(channelsString, ",")
	channels := set.NewThreadSafeSet()
	for _, r := range result {
		channels.Add(r)
	}
	if result == nil || len(result) != 4 {
		t.Error("It should not be nil")
	}
	if !channels.Has("NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_segments") {
		t.Error("It should exist")
	}
	if !channels.Has("NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_splits") {
		t.Error("It should exist")
	}
	if !channels.Has("[?occupancy=metrics.publishers]control_pri") {
		t.Error("It should exist")
	}
	if !channels.Has("[?occupancy=metrics.publishers]control_sec") {
		t.Error("It should exist")
	}
}

func TestPushManagerError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 1000, SplitUpdateQueueSize: 5000,
	}
	_, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int), nil)
	if err == nil || err.Error() != "Small size of segmentQueue" {
		t.Error("It should return err", err)
	}

	advanced.SegmentUpdateQueueSize = 5000
	advanced.SplitUpdateQueueSize = 1000
	_, err = NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int), nil)
	if err == nil || err.Error() != "Small size of splitQueue" {
		t.Error("It should return err", err)
	}
}

func TestPushManager(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}
	push, err := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, advanced, make(chan int), nil)
	if err != nil {
		t.Error("It should not return err")
	}
	if push.IsRunning() {
		t.Error("It should not be running")
	}
}

func TestPushInvalidAuth(t *testing.T) {
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: true, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})

	status := make(chan int, 1)
	push, _ := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, &advanced, status, authMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, errors.New("some")
		},
	})

	push.Start()
	msg := <-status
	if msg != NonRetriableError {
		t.Error("It should be err")
	}
}

func TestPushSSEChannels(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		fmt.Fprintf(w, "data: %s\n\n", "{}")
		flusher.Flush()
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	keeper := NewKeeper(make(chan int, 1))
	eventHandler := NewEventHandler(keeper, parser, processor, logger)
	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 1)
	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
	}

	go mockedPush.Start()
	msg := <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}

	mockedPush.Stop()
	if mockedPush.IsRunning() {
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
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	keeper := NewKeeper(make(chan int, 1))
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		sseMock, _ := ioutil.ReadFile("../testdata/sse.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		mockedData = make(map[string]interface{})
		mockedData["event"] = "keepalive"
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		mockedData = make(map[string]interface{})
		sseMock, _ = ioutil.ReadFile("../testdata/sse2.json")
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
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
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
	}
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	go func() {
		msg := <-managerStatus
		switch msg {
		case Ready:
			atomic.AddInt64(&shouldReceiveReady, 1)
		case NonRetriableError:
			atomic.AddInt64(&shouldReceiveError, 1)
		}
	}()

	go mockedPush.Start()

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
		t.Error("It should be zero")
	}
}

func TestFeedbackLoop(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		sseMock, _ := ioutil.ReadFile("../testdata/occupancy.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(100 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/occupancy2.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(5 * time.Second)
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 1)

	status := atomic.Value{}
	status.Store(Ready)

	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             publishers,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
		status:                 status,
	}

	go mockedPush.Start()
	msg := <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}

	msg = <-managerStatus
	if msg != PushIsDown {
		t.Error("It should receive push down")
	}

	msg = <-managerStatus
	if msg != PushIsUp {
		t.Error("It should receive push up. Got: ", msg)
	}
}

func TestFeedbackLoopMultiPublishers(t *testing.T) {
	var shouldReceiveReady int64
	var shouldReceiveError int64
	var shouldReceiveSwitchToPolling int64
	var shouldReceiveSwitchToStreaming int64

	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		sseMock, _ := ioutil.ReadFile("../testdata/occupancy3.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(100 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/occupancy2.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(100 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/occupancy.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 1)

	go func() {
		for {
			msg := <-managerStatus
			switch msg {
			case Ready:
				atomic.AddInt64(&shouldReceiveReady, 1)
			case PushIsDown:
				atomic.AddInt64(&shouldReceiveSwitchToPolling, 1)
			case PushIsUp:
				atomic.AddInt64(&shouldReceiveSwitchToStreaming, 1)
			case NonRetriableError:
				atomic.AddInt64(&shouldReceiveError, 1)
			}
		}
	}()

	status := atomic.Value{}
	status.Store(Ready)

	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             publishers,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
		status:                 status,
	}

	go mockedPush.Start()

	time.Sleep(1 * time.Second)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}

	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}
	if atomic.LoadInt64(&shouldReceiveSwitchToPolling) != 0 {
		t.Error("It should be zero")
	}
	if atomic.LoadInt64(&shouldReceiveReady) != 1 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveError) != 0 {
		t.Error("It should not return error")
	}
}

func TestWorkers(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		fmt.Fprintf(w, "data: %s\n\n", "{}")
		flusher.Flush()
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 1)
	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             publishers,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
	}

	go mockedPush.Start()
	<-managerStatus

	mockedPush.StopWorkers()
	if mockedPush.segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if mockedPush.splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if !mockedPush.sseClient.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.StartWorkers()
	if !mockedPush.segmentWorker.IsRunning() {
		t.Error("It should be running")
	}
	if !mockedPush.splitWorker.IsRunning() {
		t.Error("It should be running")
	}
	if !mockedPush.sseClient.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.Stop()
	if mockedPush.segmentWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if mockedPush.splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}
	if mockedPush.sseClient.IsRunning() {
		t.Error("It should be stopped")
	}
}

func TestBackoffAuth(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	count := 0
	advanced := &conf.AdvancedConfig{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL
	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)
	segmentWorker, _ := NewSegmentUpdateWorker(make(chan dtos.SegmentChangeNotification, 5000), func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(make(chan dtos.SplitChangeNotification, 5000), func(till *int64) error {
		return nil
	}, logger)
	managerStatus := make(chan int, 100)
	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				if count == 2 {
					return &dtos.Token{
						Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
						PushEnabled: true,
					}, nil
				}
				count++
				return nil, dtos.HTTPError{
					Code:    500,
					Message: "500",
				}

			},
		},
		sseClient:              mockedClient,
		eventHandler:           nil,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             make(chan int, 1),
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
	}

	go mockedPush.Start()
	backoff := 0
	shouldListen := true
	for shouldListen {
		msg := <-managerStatus
		switch msg {
		case BackoffAuth:
			backoff++
		default:
			shouldListen = false
		}
	}
	if backoff != 2 {
		t.Error("It should call backoff twice")
	}
}

func TestBackoffSSE(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	count := 0
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, make(chan int, 1))
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 2 {
			fmt.Fprintln(w, "ok")
		} else {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		count++
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL
	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)
	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 100)
	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             make(chan int, 1),
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
	}

	go mockedPush.Start()
	backoff := 0
	shouldListen := true
	for shouldListen {
		msg := <-managerStatus
		switch msg {
		case BackoffSSE:
			backoff++
		default:
			shouldListen = false
		}
	}
	if backoff != 2 {
		t.Error("It should call backoff twice")
	}
}

func TestControlLogic(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := &conf.AdvancedConfig{
		SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000,
	}

	splitQueue := make(chan dtos.SplitChangeNotification, advanced.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, advanced.SegmentUpdateQueueSize)
	controlStatus := make(chan int, 1)
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger, controlStatus)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isValidChannels(t, r.URL.Query().Get("channels"))
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		sseMock, _ := ioutil.ReadFile("../testdata/occupancy2.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(50 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/streamingPaused.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(50 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/streamingResumed.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(50 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/streamingDisabled.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
		time.Sleep(5 * time.Second)
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL

	streamingStatus := make(chan int, 1)
	mockedClient := sse.NewStreamingClient(advanced, streamingStatus, logger)

	segmentWorker, _ := NewSegmentUpdateWorker(segmentQueue, func(segmentName string, till *int64) error {
		return nil
	}, logger)
	splitWorker, _ := NewSplitUpdateWorker(splitQueue, func(till *int64) error {
		return nil
	}, logger)

	managerStatus := make(chan int, 1)
	status := atomic.Value{}
	status.Store(Ready)
	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:              mockedClient,
		eventHandler:           eventHandler,
		logger:                 logger,
		segmentWorker:          segmentWorker,
		splitWorker:            splitWorker,
		managerStatus:          managerStatus,
		streamingStatus:        streamingStatus,
		publishers:             publishers,
		cancelAuthBackoff:      make(chan struct{}, 1),
		cancelSSEBackoff:       make(chan struct{}, 1),
		cancelTokenExpiration:  make(chan struct{}, 1),
		cancelStreamingWatcher: make(chan struct{}, 1),
		status:                 status,
		control:                controlStatus,
	}

	go mockedPush.Start()
	msg := <-managerStatus
	if msg != Ready {
		t.Error("It should receive Ready")
	}
	msg = <-managerStatus
	if msg != PushIsUp {
		t.Error("It should send publishers")
	}
	msg = <-managerStatus
	if msg != PushIsDown {
		t.Error("It should stop workers")
	}
	if mockedPush.status.Load().(int) != StreamingPaused {
		t.Error("It should be paused")
	}
	msg = <-managerStatus
	if msg != PushIsUp {
		t.Error("It should start workers")
	}
	if mockedPush.status.Load().(int) != StreamingResumed {
		t.Error("It should be resumed")
	}
	msg = <-managerStatus
	if msg != StreamingDisabled {
		t.Error("It should send StreamingDisabled")
	}
	mockedPush.Stop()
}
*/
