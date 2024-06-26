package push

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
	pushMocks "github.com/splitio/go-split-commons/v6/push/mocks"
	"github.com/splitio/go-split-commons/v6/service/api/sse"
	sseMocks "github.com/splitio/go-split-commons/v6/service/api/sse/mocks"
	serviceMocks "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-split-commons/v6/telemetry"

	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
	rawSseMocks "github.com/splitio/go-toolkit/v5/sse/mocks"
)

func TestAuth500(t *testing.T) {
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, dtos.HTTPError{Code: 500}
		},
	}
	feedback := make(chan int64, 100)

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.TokenSync {
				t.Error("It should be token")
			}
			if status != 500 {
				t.Error("Status should be 500")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.Start()
	message := <-feedback
	if message != StatusRetryableError {
		t.Error("push manager should have proapgated a retryable error. Got: ", message)
	}

	if manager.nextRefresh != nil {
		t.Error("no next refresh should have been set if startup wasn't successful")
	}
}

func TestAuth401(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, &dtos.HTTPError{Code: 401}
		},
	}
	feedback := make(chan int64, 100)

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.TokenSync {
				t.Error("It should be token")
			}
			if status != 401 {
				t.Error("Status should be 401")
			}
		},
		RecordAuthRejectionsCall: func() { called++ },
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.Start()
	message := <-feedback
	if message != StatusNonRetryableError {
		t.Error("push manager should have proapgated a non-retryable error. Got: ", message)
	}

	if manager.nextRefresh != nil {
		t.Error("no next refresh should have been set if startup wasn't successful")
	}

	if called != 1 {
		t.Error("It should record an auth rejection")
	}
}

func TestAuthPushDisabled(t *testing.T) {
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return &dtos.Token{PushEnabled: false}, nil
		},
	}
	feedback := make(chan int64, 100)

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.Start()
	message := <-feedback
	if message != StatusNonRetryableError {
		t.Error("push manager should have proapgated a non-retryable error. Got: ", message)
	}

	if manager.nextRefresh != nil {
		t.Error("no next refresh should have been set if startup wasn't successful")
	}
}

func TestStreamingConnectionFails(t *testing.T) {
	before := time.Now().UTC()
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
			if tm.Before(before) {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			status <- sse.StatusConnectionFailed
		},
	}

	manager.Start()
	message := <-feedback
	if message != StatusRetryableError {
		t.Error("push manager should have proapgated a retryable error. Got: ", message)
	}

	if manager.nextRefresh != nil {
		t.Error("no next refresh should have been set if startup wasn't successful")
	}
}

func TestStreamingUnexpectedDisconnection(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}
	feedback := make(chan int64, 100)

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				time.Sleep(1 * time.Second)
				status <- sse.StatusDisconnected
			}()
		},
	}

	manager.Start()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	message = <-feedback
	if message != StatusRetryableError {
		t.Error("push manager should have proapgated a retryable error status. Got: ", message)
	}
}

func TestExpectedDisconnection(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}
	feedback := make(chan int64, 100)

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				time.Sleep(1 * time.Second)
				status <- sse.StatusDisconnected
			}()
		},
	}

	manager.Start()
	manager.statusTracker.NotifySSEShutdownExpected()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	select {
	case message = <-feedback:
		t.Error("should have gotten no message after an expected disconnection. Got: ", message)
	case <-time.After(2 * time.Second):
	}
}

func TestMultipleCallsToStartAndStop(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh || streamingEvent.Data != 3000000 {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	waiter := make(chan struct{}, 1)
	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				<-waiter
				status <- sse.StatusDisconnected
			}()
		},
		StopStreamingCall: func() {
			waiter <- struct{}{} // free "sse" goroutine to make it end
		},
	}

	if err := manager.Start(); err != nil {
		t.Error("first call to Start() should not return an error. Got:", err)
	}

	if err := manager.Start(); err == nil {
		t.Error("further calls to Start() should return an error.")
	}
	manager.statusTracker.NotifySSEShutdownExpected()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	if err := manager.Stop(); err != nil {
		t.Error("no error should be returned on the first call to .Stop(). Got:", err)
	}
	if err := manager.Stop(); err == nil {
		t.Error("an error should be returned on further calls to .Stop()")
	}
}

func TestUsageAndTokenRefresh(t *testing.T) {
	called := 0
	var tokenRefreshes int64
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}

	// This token has a low expiration time on purpose so that it triggers a refresh
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0NDYyNX0.TP0_iztPvEJcBfZjdATHc5_Yy41AYqqCptOiHpsfN-4`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() { atomic.AddInt64(&tokenRefreshes, 1) },
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	waiter := make(chan struct{}, 1)
	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				<-waiter
				status <- sse.StatusDisconnected
			}()
		}, StopStreamingCall: func() { waiter <- struct{}{} },
	}

	manager.Start()
	manager.statusTracker.NotifySSEShutdownExpected()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	manager.withRefreshTokenLock(func() {
		if manager.nextRefresh == nil {
			t.Error("a token refresh should have been scheduled after a successful connection.")
		}
	})

	message = <-feedback
	if message != StatusUp {
		t.Error("token should have refreshed and returned a new StatusUp message via the feedback loop. Got: ", message)
	}

	if atomic.LoadInt64(&tokenRefreshes) != 2 {
		t.Error("It should call tokenRefresh")
	}
}

func TestEventForwarding(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	waiter := make(chan struct{}, 1)
	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				<-waiter
				handler(&rawSseMocks.RawEventMock{IDCall: func() string { return "abc" }})
				<-waiter
				status <- sse.StatusDisconnected
			}()
		},
		StopStreamingCall: func() {
			waiter <- struct{}{}
		},
	}

	handled := int32(0)
	manager.parser = &pushMocks.NotificationParserMock{
		ParseAndForwardCall: func(e sse.IncomingMessage) (*int64, error) {
			if e.ID() != "abc" {
				t.Error("wrong id. expected abc. got: ", e.ID())
			}

			atomic.AddInt32(&handled, 1)

			return nil, nil
		},
	}

	manager.Start()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	waiter <- struct{}{} // free the goroutine to send an event to the parser
	time.Sleep(1 * time.Second)
	if h := atomic.LoadInt32(&handled); h != 1 {
		t.Error("handler should have been executed once. Was: ", h)
	}

	select {
	case message = <-feedback:
		t.Error("should have gotten no message after an expected disconnection. Got: ", message)
	case <-time.After(2 * time.Second):
	}
}

func TestEventForwardingReturnsError(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	waiter := make(chan struct{}, 1)
	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				<-waiter
				handler(&rawSseMocks.RawEventMock{IDCall: func() string { return "abc" }})
				<-waiter
				status <- sse.StatusDisconnected
			}()
		},
		StopStreamingCall: func() {
			waiter <- struct{}{}
		},
	}

	handled := int32(0)
	manager.parser = &pushMocks.NotificationParserMock{
		ParseAndForwardCall: func(e sse.IncomingMessage) (*int64, error) {
			if e.ID() != "abc" {
				t.Error("wrong id. expected abc. got: ", e.ID())
			}

			atomic.AddInt32(&handled, 1)

			return nil, fmt.Errorf("something")
		},
	}

	manager.Start()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	waiter <- struct{}{} // free the goroutine to send an event to the parser
	time.Sleep(1 * time.Second)
	if h := atomic.LoadInt32(&handled); h != 1 {
		t.Error("handler should have been executed once. Was: ", h)
	}

	message = <-feedback
	if message != StatusRetryableError {
		t.Error("should have gotten no message after an expected disconnection. Got: ", message)
	}
}

func TestEventForwardingReturnsNewStatus(t *testing.T) {
	called := 0
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	token := &dtos.Token{
		Token:       `eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MTMzNDUyMzAsImlhdCI6MTYxMzM0MTYzMH0.Z3jKyiJq6t00hWFV_xIlh5w4xAYF3Rj0gfcTxgLjcOc`,
		PushEnabled: true,
	}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) { return token, nil },
	}
	feedback := make(chan int64, 100)
	telemetryStorageMock := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordTokenRefreshesCall: func() {},
		RecordStreamingEventCall: func(streamingEvent *dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != telemetry.EventTypeTokenRefresh {
					t.Error("Should record next token refresh")
				}
			case 1:
				if streamingEvent.Type != telemetry.EventTypeSSEConnectionEstablished {
					t.Error("It should record connection established")
				}
			}
			called++
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock, dtos.Metadata{}, nil)
	if err != nil {
		t.Error("no error should be returned upon manager instantiation", err)
		return
	}

	waiter := make(chan struct{}, 1)
	manager.sseClient = &sseMocks.StreamingClientMock{
		ConnectStreamingCall: func(tok string, status chan int, channels []string, handler func(sse.IncomingMessage)) {
			if tok != token.Token {
				t.Error("incorrect token received.")
			}

			go func() {
				status <- sse.StatusFirstEventOk
				<-waiter
				handler(&rawSseMocks.RawEventMock{IDCall: func() string { return "abc" }})
				<-waiter
				status <- sse.StatusDisconnected
			}()
		},
		StopStreamingCall: func() {
			waiter <- struct{}{}
		},
	}

	handled := int32(0)
	manager.parser = &pushMocks.NotificationParserMock{
		ParseAndForwardCall: func(e sse.IncomingMessage) (*int64, error) {
			if e.ID() != "abc" {
				t.Error("wrong id. expected abc. got: ", e.ID())
			}

			atomic.AddInt32(&handled, 1)

			return common.Int64Ref(StatusNonRetryableError), nil
		},
	}

	manager.Start()
	message := <-feedback
	if message != StatusUp {
		t.Error("push manager should have proapgated a push up status. Got: ", message)
	}

	if manager.nextRefresh == nil {
		t.Error("a token refresh should have been scheduled after a successful connection.")
	}

	waiter <- struct{}{} // free the goroutine to send an event to the parser
	time.Sleep(1 * time.Second)
	if h := atomic.LoadInt32(&handled); h != 1 {
		t.Error("handler should have been executed once. Was: ", h)
	}

	message = <-feedback
	if message != StatusNonRetryableError {
		t.Error("should have gotten no message after an expected disconnection. Got: ", message)
	}
}
