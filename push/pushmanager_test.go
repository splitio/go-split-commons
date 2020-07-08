package push

import (
	"encoding/json"
	"errors"
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
	authMocks "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
	sseStatus "github.com/splitio/go-toolkit/sse"
)

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
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})

	status := make(chan int, 1)
	push, _ := NewPushManager(logger, nil, nil, mocks.MockSplitStorage{}, &advanced, status, authMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, errors.New("some")
		},
	})

	push.Start()
	msg := <-status
	if msg != Error {
		t.Error("It should be err")
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
	keeper := NewKeeper(make(chan int, 1))
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

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
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
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

	mockedPush.Start()

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
	keeper := NewKeeper(make(chan int, 1))
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

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
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
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

	mockedPush.Start()
	time.Sleep(100 * time.Millisecond)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorKeepAlive
	time.Sleep(100 * time.Millisecond)

	mockedPush.Start()
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

func TestFeedbackLoop(t *testing.T) {
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
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		for i := 0; i < 2; i++ {
			switch i {
			case 0:
				sseMock, _ := ioutil.ReadFile("../testdata/occupancy.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 1:
				sseMock, _ := ioutil.ReadFile("../testdata/occupancy2.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			}
		}
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
			case Error:
				atomic.AddInt64(&shouldReceiveError, 1)
			}
		}
	}()

	mockedPush := PushManager{
		authClient: authMocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		sseClient:       mockedClient,
		eventHandler:    eventHandler,
		logger:          logger,
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
		publishers:      publishers,
	}

	mockedPush.Start()

	time.Sleep(1 * time.Second)
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}

	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}
	if atomic.LoadInt64(&shouldReceiveSwitchToPolling) != 1 {
		t.Error("It should be one")
	}
	if atomic.LoadInt64(&shouldReceiveSwitchToPolling) != 1 {
		t.Error("It should be one")
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
	processor, err := NewProcessor(segmentQueue, splitQueue, mocks.MockSplitStorage{}, logger)
	if err != nil {
		t.Error("It should not return error")
	}
	parser := NewNotificationParser(logger)
	if err != nil {
		t.Error("It should not return err")
	}
	publishers := make(chan int, 1)
	keeper := NewKeeper(publishers)
	eventHandler := NewEventHandler(keeper, parser, processor, logger)

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
		sseClient:       mockedClient,
		eventHandler:    eventHandler,
		logger:          logger,
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
		publishers:      publishers,
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
