// +build !race

package push

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api/sse"
	authMocks "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
	sseStatus "github.com/splitio/go-toolkit/sse"
)

func TestPushError(t *testing.T) {
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

	stopped := make(chan struct{}, 1)
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
		cancelStreamingWatcher: stopped,
	}
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	// Testing Reconnection KeepAlive
	go mockedPush.Start()
	msg := <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorKeepAlive
	msg = <-managerStatus
	if msg != Reconnect {
		t.Error("It should be reconnect")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	// Testing Reconnection ErrorInternal
	go mockedPush.Start()
	msg = <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorInternal
	msg = <-managerStatus
	if msg != Reconnect {
		t.Error("It should be reconnect")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	// Testing Reconnection ErrorReadingStream
	go mockedPush.Start()
	msg = <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorReadingStream
	msg = <-managerStatus
	if msg != Reconnect {
		t.Error("It should be reconnect")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	// Testing Error without reconnection
	go mockedPush.Start()
	msg = <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorOnClientCreation
	msg = <-managerStatus
	if msg != NonRetriableError {
		t.Error("It should be error")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}

	// Testing Error without reconnection
	go mockedPush.Start()
	msg = <-managerStatus
	if msg != Ready {
		t.Error("It should be ready")
	}
	if !mockedPush.IsRunning() {
		t.Error("It should be running")
	}
	streamingStatus <- sseStatus.ErrorRequestPerformed
	msg = <-managerStatus
	if msg != NonRetriableError {
		t.Error("It should be error")
	}
	mockedPush.Stop()
	if mockedPush.IsRunning() {
		t.Error("It should not be running")
	}
}
