package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v7/conf"
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/healthcheck/application"
	hcMock "github.com/splitio/go-split-commons/v7/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v7/push"
	pushMocks "github.com/splitio/go-split-commons/v7/push/mocks"
	apiMocks "github.com/splitio/go-split-commons/v7/service/mocks"
	storageMocks "github.com/splitio/go-split-commons/v7/storage/mocks"
	"github.com/splitio/go-split-commons/v7/synchronizer/mocks"
	"github.com/splitio/go-split-commons/v7/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSynchronizerErr(t *testing.T) {
	syncMock := &mocks.MockSynchronizer{
		SyncAllCall:                    func() error { return nil },
		StartPeriodicFetchingCall:      func() {},
		StopPeriodicFetchingCall:       func() {},
		StartPeriodicDataRecordingCall: func() {},
		StopPeriodicDataRecordingCall:  func() {},
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := hcMock.MockApplicationMonitor{}
	status := make(chan int, 1)
	_, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("It should not return err", err.Error())
	}

	myKey := "12345"
	_, err = NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, &myKey, appMonitor)
	if err == nil || err.Error() != "invalid ClientKey" {
		t.Error("It should return err", err.Error())
	}

	myKey = "1234"
	_, err = NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, &myKey, appMonitor)
	if err != nil {
		t.Error("It should not return err", err.Error())
	}
}

func TestStreamingDisabledInitOk(t *testing.T) {
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall: func() error {
			atomic.AddInt32(&syncAllCount, 1)
			return nil
		},
		StartPeriodicFetchingCall:      func() { atomic.AddInt32(&startPeriodicFetchingCount, 1) },
		StopPeriodicFetchingCall:       func() { atomic.AddInt32(&stopPeriodicFetchingCount, 1) },
		StartPeriodicDataRecordingCall: func() { atomic.AddInt32(&startPeriodicRecordingCount, 1) },
		StopPeriodicDataRecordingCall:  func() { atomic.AddInt32(&stopPeriodicRecordingCount, 1) },
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = false
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Polling {
				t.Error("It should record Streaming")
			}
		},
	}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}
	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager != nil {
		t.Error("push manager should be nil")
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	if atomic.LoadInt32(&syncAllCount) != 1 {
		t.Error("there should have been 1 syncAll call")
	}

	if atomic.LoadInt32(&startPeriodicFetchingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicFetching")
	}

	if atomic.LoadInt32(&startPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicRecording")
	}

	manager.Stop()
	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	if atomic.LoadInt32(&stopPeriodicFetchingCount) != 1 {
		t.Error("there should be 1 call to stopPeriodicFetching")
	}

	if atomic.LoadInt32(&stopPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to stopPeriodicRecording")
	}
}

func TestStreamingDisabledInitError(t *testing.T) {
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)
	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall: func() error {
			atomic.AddInt32(&syncAllCount, 1)
			return errors.New("some error")
		},
		StartPeriodicFetchingCall:      func() { atomic.AddInt32(&startPeriodicFetchingCount, 1) },
		StopPeriodicFetchingCall:       func() { atomic.AddInt32(&stopPeriodicFetchingCount, 1) },
		StartPeriodicDataRecordingCall: func() { atomic.AddInt32(&startPeriodicRecordingCount, 1) },
		StopPeriodicDataRecordingCall:  func() { atomic.AddInt32(&stopPeriodicRecordingCount, 1) },
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = false
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}
	status := make(chan int, 1)

	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager != nil {
		t.Error("push manager should be nil")
	}

	manager.Start()
	if manager.IsRunning() {
		t.Error("manager should not be running after an error")
	}

	message := <-status
	if message != Error {
		t.Error("first message should be SDK error")
	}

	if atomic.LoadInt32(&syncAllCount) != 1 {
		t.Error("there should have been 1 syncAll call")
	}

	if atomic.LoadInt32(&startPeriodicFetchingCount) != 0 {
		t.Error("there should be 1 call to startPeriodicFetching")
	}

	if atomic.LoadInt32(&startPeriodicRecordingCount) != 0 {
		t.Error("there should be 1 call to startPeriodicRecording")
	}

	manager.Stop()
	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	if atomic.LoadInt32(&stopPeriodicFetchingCount) != 0 {
		t.Error("there should be 1 call to stopPeriodicFetching")
	}

	if atomic.LoadInt32(&stopPeriodicRecordingCount) != 0 {
		t.Error("there should be 1 call to stopPeriodicRecording")
	}
}

func TestStreamingEnabledInitOk(t *testing.T) {
	called := 0
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall: func() error {
			atomic.AddInt32(&syncAllCount, 1)
			return nil
		},
		StartPeriodicFetchingCall:      func() { atomic.AddInt32(&startPeriodicFetchingCount, 1) },
		StopPeriodicFetchingCall:       func() { atomic.AddInt32(&stopPeriodicFetchingCount, 1) },
		StartPeriodicDataRecordingCall: func() { atomic.AddInt32(&startPeriodicRecordingCount, 1) },
		StopPeriodicDataRecordingCall:  func() { atomic.AddInt32(&stopPeriodicRecordingCount, 1) },
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Streaming {
					t.Error("It should receive polling")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingEnabled {
					t.Error("It should receive enabled")
				}
			}
			called++
		},
	}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager == nil {
		t.Error("push manager should NOT be nil")
	}

	// Replace push manager with a mock
	startCalls := int32(0)
	stopCalls := int32(0)
	startWorkersCalls := int32(0)
	manager.pushManager = &pushMocks.MockManager{
		NextRefreshCall: func() time.Time { return time.Now().Add(1 * time.Hour) },
		StartCall: func() error {
			go func() {
				atomic.AddInt32(&startCalls, 1)
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusUp
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusUp // simulate a token refresh
			}()
			return nil
		},
		StopCall: func() error {
			atomic.AddInt32(&stopCalls, 1)
			return nil
		},
		StartWorkersCall: func() { atomic.AddInt32(&startWorkersCalls, 1) },
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	time.Sleep(3 * time.Second) // wait 3 seconds so that the next "pushUP" (token refresh simulation) is sent

	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	if s := atomic.LoadInt32(&syncAllCount); s != 3 {
		// initial syncAll
		// streaming connect ok
		// token refresh
		t.Error("there should have been 3 syncAll calls. Got: ", s)
	}

	if atomic.LoadInt32(&startPeriodicFetchingCount) != 0 {
		t.Error("there should be 1 call to startPeriodicFetching")
	}

	if atomic.LoadInt32(&startPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicRecording")
	}

	if atomic.LoadInt32(&startCalls) != 1 {
		t.Error("push manager Start() shouldhave been called 1 time")
	}

	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Error("push manager Stop() shouldhave been called 1 time")
	}

	if atomic.LoadInt32(&stopPeriodicFetchingCount) != 3 {
		// first statusUp
		// token refresh
		// final shutdown
		t.Error("there should be 2 call to stopPeriodicFetching")
	}

	if atomic.LoadInt32(&stopPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to stopPeriodicRecording")
	}

	if atomic.LoadInt32(&startWorkersCalls) != 2 {
		t.Error("start workers shold have been called 2 times")
	}
}

func TestStreamingEnabledRetryableError(t *testing.T) {
	called := 0
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall: func() error {
			atomic.AddInt32(&syncAllCount, 1)
			return nil
		},
		StartPeriodicFetchingCall:      func() { atomic.AddInt32(&startPeriodicFetchingCount, 1) },
		StopPeriodicFetchingCall:       func() { atomic.AddInt32(&stopPeriodicFetchingCount, 1) },
		StartPeriodicDataRecordingCall: func() { atomic.AddInt32(&startPeriodicRecordingCount, 1) },
		StopPeriodicDataRecordingCall:  func() { atomic.AddInt32(&stopPeriodicRecordingCount, 1) },
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Streaming {
					t.Error("It should receive polling")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingEnabled {
					t.Error("It should receive enabled")
				}
			case 2:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Polling {
					t.Error("It should receive polling")
				}
			}
			called++
		},
	}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager == nil {
		t.Error("push manager should NOT be nil")
	}

	// Replace push manager with a mock
	startCalls := int32(0)
	stopCalls := int32(0)
	startWorkersCalls := int32(0)
	manager.pushManager = &pushMocks.MockManager{
		NextRefreshCall: func() time.Time { return time.Now().Add(1 * time.Hour) },
		StartCall: func() error {
			atomic.AddInt32(&startCalls, 1)
			if atomic.LoadInt32(&startCalls) == 1 {
				go func() {
					time.Sleep(1 * time.Second)
					manager.streamingStatus <- push.StatusUp
					time.Sleep(1 * time.Second)
					if atomic.LoadInt32(&startCalls) == 1 {
						manager.streamingStatus <- push.StatusRetryableError // send an error
					}
				}()
			} else if atomic.LoadInt32(&startCalls) == 2 {
				go func() {
					time.Sleep(1 * time.Second)
					manager.streamingStatus <- push.StatusUp
				}()

			} else {
				t.Error("pushManager.Start() called more times than necessary.")
			}

			return nil
		},
		StopCall: func() error {
			atomic.AddInt32(&stopCalls, 1)
			return nil
		},
		StartWorkersCall: func() { atomic.AddInt32(&startWorkersCalls, 1) },
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	time.Sleep(6 * time.Second) // wait 5 seconds. (init + error + backoff + next init)

	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	if s := atomic.LoadInt32(&syncAllCount); s != 4 {
		// Initial syncAll
		// One after push connects successfully
		// One after retryable error is received
		// One after we reconnect on the second start
		t.Error("there should have been 5 syncAll calls. Got: ", s)
	}

	if atomic.LoadInt32(&startPeriodicFetchingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicFetching")
	}

	if atomic.LoadInt32(&startPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicRecording")
	}

	if atomic.LoadInt32(&startCalls) != 2 {
		t.Error("push manager Start() shouldhave been called 1 time")
	}

	if atomic.LoadInt32(&stopCalls) != 2 {
		t.Error("push manager Stop() shouldhave been called 1 time")
	}

	if s := atomic.LoadInt32(&stopPeriodicFetchingCount); s != 3 {
		// after initial statusUp
		// after retryable error happens and second statusUp arrives
		// at shutdown
		t.Error("there should be 2 call to stopPeriodicFetching", s)
	}

	if atomic.LoadInt32(&stopPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to stopPeriodicRecording")
	}

	if atomic.LoadInt32(&startWorkersCalls) != 2 {
		t.Error("start workers shold have been called 2 times")
	}
}

func TestStreamingEnabledNonRetryableError(t *testing.T) {
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)
	called := 0

	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall: func() error {
			atomic.AddInt32(&syncAllCount, 1)
			return nil
		},
		StartPeriodicFetchingCall:      func() { atomic.AddInt32(&startPeriodicFetchingCount, 1) },
		StopPeriodicFetchingCall:       func() { atomic.AddInt32(&stopPeriodicFetchingCount, 1) },
		StartPeriodicDataRecordingCall: func() { atomic.AddInt32(&startPeriodicRecordingCount, 1) },
		StopPeriodicDataRecordingCall:  func() { atomic.AddInt32(&stopPeriodicRecordingCount, 1) },
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Streaming {
					t.Error("It should receive streaming")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingEnabled {
					t.Error("It should receive enabled")
				}
			case 2:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Polling {
					t.Error("It should receive polling")
				}
			case 3:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingDisabled {
					t.Error("It should receive disabled")
				}
			}
			called++
		},
	}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager == nil {
		t.Error("push manager should NOT be nil")
	}

	// Replace push manager with a mock
	startCalls := int32(0)
	stopCalls := int32(0)
	startWorkersCalls := int32(0)
	manager.pushManager = &pushMocks.MockManager{
		NextRefreshCall: func() time.Time { return time.Now().Add(1 * time.Hour) },
		StartCall: func() error {
			atomic.AddInt32(&startCalls, 1)
			if atomic.LoadInt32(&startCalls) == 1 {
				go func() {
					time.Sleep(1 * time.Second)
					manager.streamingStatus <- push.StatusUp
					time.Sleep(1 * time.Second)
					if atomic.LoadInt32(&startCalls) == 1 {
						manager.streamingStatus <- push.StatusNonRetryableError // send an error
					}
				}()
			} else {
				t.Error("pushManager.Start() called more times than necessary.")
			}
			return nil
		},
		StopCall: func() error {
			atomic.AddInt32(&stopCalls, 1)
			return nil
		},
		StartWorkersCall: func() { atomic.AddInt32(&startWorkersCalls, 1) },
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	time.Sleep(3 * time.Second) // wait 3 until retryable error happens, and then sse is restarted

	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	if s := atomic.LoadInt32(&syncAllCount); s != 3 {
		// Initial syncAll
		// One after push connects successfully
		// One after nonretryable error is received
		t.Error("there should have been 5 syncAll calls. Got: ", s)
	}

	if atomic.LoadInt32(&startPeriodicFetchingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicFetching")
	}

	if atomic.LoadInt32(&startPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to startPeriodicRecording")
	}

	if atomic.LoadInt32(&startCalls) != 1 {
		t.Error("push manager Start() shouldhave been called 1 time")
	}

	if atomic.LoadInt32(&stopCalls) != 2 {
		t.Error("push manager Stop() shouldhave been called 2 times")
	}

	if s := atomic.LoadInt32(&stopPeriodicFetchingCount); s != 2 {
		// after initial statusUp
		// at shutdown
		t.Error("there should be 2 call to stopPeriodicFetching", s)
	}

	if atomic.LoadInt32(&stopPeriodicRecordingCount) != 1 {
		t.Error("there should be 1 call to stopPeriodicRecording")
	}

	if atomic.LoadInt32(&startWorkersCalls) != 1 {
		t.Error("start workers shold have been called 2 times")
	}
}

func TestStreamingPaused(t *testing.T) {
	called := 0

	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 1 * time.Minute
		},
		SyncAllCall:                    func() error { return nil },
		StartPeriodicFetchingCall:      func() {},
		StopPeriodicFetchingCall:       func() {},
		StartPeriodicDataRecordingCall: func() {},
		StopPeriodicDataRecordingCall:  func() {},
	}
	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Streaming {
					t.Error("It should receive streaming")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingEnabled {
					t.Error("It should receive enabled")
				}
			case 2:
				if streamingEvent.Type != telemetry.EventTypeStreamingStatus || streamingEvent.Data != telemetry.StreamingPaused {
					t.Error("It should receive paused")
				}
			case 3:
				if streamingEvent.Type != telemetry.EventTypeSyncMode || streamingEvent.Data != telemetry.Polling {
					t.Error("It should receive polling")
				}
			}
			called++
		},
	}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &application.Dummy{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager == nil {
		t.Error("push manager should NOT be nil")
	}

	// Replace push manager with a mock
	manager.pushManager = &pushMocks.MockManager{
		NextRefreshCall: func() time.Time { return time.Now().Add(1 * time.Hour) },
		StartCall: func() error {
			go func() {
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusUp
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusDown
			}()
			return nil
		},
		StopCall:         func() error { return nil },
		StartWorkersCall: func() {},
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	time.Sleep(3 * time.Second) // wait 3 until retryable error happens, and then sse is restarted

	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager should not be running")
	}
}

func TestOccupancyFlicker(t *testing.T) {
	syncMock := &mocks.MockSynchronizer{
		RefreshRatesCall: func() (time.Duration, time.Duration) {
			return 1 * time.Minute, 2 * time.Minute
		},
		SyncAllCall:                    func() error { return nil },
		StartPeriodicFetchingCall:      func() {},
		StopPeriodicFetchingCall:       func() {},
		StartPeriodicDataRecordingCall: func() {},
		StopPeriodicDataRecordingCall:  func() {},
	}

	periodChanges := make(chan periodChange, 1000)

	logger := logging.NewLogger(nil)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.StreamingEnabled = true
	cfg.LargeSegment.RefreshRate = int((2 * time.Minute).Seconds())
	splitStorage := &storageMocks.MockSplitStorage{}
	telemetryStorage := storageMocks.MockTelemetryStorage{RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {}}
	authClient := &apiMocks.MockAuthClient{}
	appMonitor := &hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
		ResetCall: func(counterType int, newPeriod int) {
			periodChanges <- periodChange{ct: counterType, newp: newPeriod}
		},
	}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status, telemetryStorage, dtos.Metadata{}, nil, appMonitor)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if manager.pushManager == nil {
		t.Error("push manager should NOT be nil")
	}

	// Replace push manager with a mock
	startCalls := int32(0)
	stopCalls := int32(0)
	startWorkersCalls := int32(0)
	manager.pushManager = &pushMocks.MockManager{
		NextRefreshCall: func() time.Time { return time.Now().Add(1 * time.Hour) },
		StartCall: func() error {
			go func() {
				atomic.AddInt32(&startCalls, 1)
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusUp
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusDown // occupancy down
				time.Sleep(1 * time.Second)
				manager.streamingStatus <- push.StatusUp // occupancy back
			}()
			return nil
		},
		StopCall: func() error {
			atomic.AddInt32(&stopCalls, 1)
			return nil
		},
		StartWorkersCall: func() { atomic.AddInt32(&startWorkersCalls, 1) },
	}

	manager.Start()
	if !manager.IsRunning() {
		t.Error("manager should be running")
	}

	message := <-status
	if message != Ready {
		t.Error("first message should be SDK ready")
	}

	time.Sleep(4 * time.Second) // wait 4 seconds for all events to occur

	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager should not be running")
	}

	// Initial period update: streaming ready

	expected := 1*time.Hour + refreshTokenTolerance
	p, ok := getFromChan(periodChanges)
	if !ok || p.ct != application.Splits || !inRange(p.newp, expected) {
		// change must exist
		// must be of time split
		// new period should be ~1hour (the token refresh period)
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.Segments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.LargeSegments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	// -------

	// Second period update (when occupancy goes to 0)
	expected = 1*time.Minute + fetchTaskTolerance
	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.Splits || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	expected = 2*time.Minute + fetchTaskTolerance
	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.Segments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	expected = 2*time.Minute + fetchTaskTolerance
	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.LargeSegments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	// -----

	// Third updates (occupany back to >0)
	expected = 1*time.Hour + refreshTokenTolerance
	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.Splits || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.Segments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

	p, ok = getFromChan(periodChanges)
	if !ok || p.ct != application.LargeSegments || !inRange(p.newp, expected) {
		t.Errorf("wrong initial period change. found: %t type: %d, new period: %d, expected: %d", ok, p.ct, p.newp, expected)
	}

}

type periodChange struct {
	ct   int
	newp int
}

func getFromChan(c chan periodChange) (periodChange, bool) {
	select {
	case pc := <-c:
		return pc, true
	default:
		return periodChange{}, false
	}
}

func inRange(secs int, t time.Duration) bool {
	return secs-1 <= int(t.Seconds()) && int(t.Seconds()) <= secs+1
}
