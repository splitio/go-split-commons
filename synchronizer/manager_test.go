package synchronizer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/push"
	pushMocks "github.com/splitio/go-split-commons/v3/push/mocks"
	apiMocks "github.com/splitio/go-split-commons/v3/service/mocks"
	storageMocks "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/synchronizer/mocks"

	"github.com/splitio/go-toolkit/v4/logging"
)

func TestStreamingDisabledInitOk(t *testing.T) {
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
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
	authClient := &apiMocks.MockAuthClient{}
	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status)
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
	authClient := &apiMocks.MockAuthClient{}
	status := make(chan int, 1)

	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status)
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
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
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
	authClient := &apiMocks.MockAuthClient{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status)
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
	syncAllCount := int32(0)
	startPeriodicFetchingCount := int32(0)
	stopPeriodicFetchingCount := int32(0)
	startPeriodicRecordingCount := int32(0)
	stopPeriodicRecordingCount := int32(0)

	syncMock := &mocks.MockSynchronizer{
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
	authClient := &apiMocks.MockAuthClient{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status)
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
		StartCall: func() error {
			atomic.AddInt32(&startCalls, 1)
			if atomic.LoadInt32(&startCalls) == 1 {
				go func() {
					fmt.Println("start primera vez")
					time.Sleep(1 * time.Second)
					manager.streamingStatus <- push.StatusUp
					time.Sleep(1 * time.Second)
					if atomic.LoadInt32(&startCalls) == 1 {
						manager.streamingStatus <- push.StatusRetryableError // send an error
					}
				}()
			} else if atomic.LoadInt32(&startCalls) == 2 {
				go func() {
					fmt.Println("start segudna vez")
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

	syncMock := &mocks.MockSynchronizer{
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
	authClient := &apiMocks.MockAuthClient{}

	status := make(chan int, 1)
	manager, err := NewSynchronizerManager(syncMock, logger, cfg, authClient, splitStorage, status)
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
		StartCall: func() error {
			atomic.AddInt32(&startCalls, 1)
			if atomic.LoadInt32(&startCalls) == 1 {
				go func() {
					fmt.Println("start primera vez")
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
