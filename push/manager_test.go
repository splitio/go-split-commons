package push

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	pushMocks "github.com/splitio/go-split-commons/v3/push/mocks"
	"github.com/splitio/go-split-commons/v3/service/api/sse"
	sseMocks "github.com/splitio/go-split-commons/v3/service/api/sse/mocks"
	serviceMocks "github.com/splitio/go-split-commons/v3/service/mocks"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/telemetry"

	"github.com/splitio/go-toolkit/v4/common"
	"github.com/splitio/go-toolkit/v4/logging"
	rawSseMocks "github.com/splitio/go-toolkit/v4/sse/mocks"
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

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage)
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
	cfg := &conf.AdvancedConfig{
		SplitUpdateQueueSize:   10000,
		SegmentUpdateQueueSize: 10000,
	}
	logger := logging.NewLogger(nil)
	synchronizer := &pushMocks.LocalSyncMock{}
	authMock := &serviceMocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, dtos.HTTPError{Code: 401}
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
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage)
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
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}
	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryMockStorage)
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
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}
	feedback := make(chan int64, 100)

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}
	feedback := make(chan int64, 100)

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
}

func TestEventForwarding(t *testing.T) {
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency int64) {
			if resource != telemetry.TokenSync {
				t.Error("Resource should be token")
			}
		},
	}

	manager, err := NewManager(logger, synchronizer, cfg, feedback, authMock, telemetryStorageMock)
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

/*
import (
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/push/mocks"
	"github.com/splitio/go-split-commons/v3/service/api"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestPushManagerCustom(t *testing.T) {
	syncMock := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(x *int64) error {
			fmt.Printf("split sync con '%d'\n", *x)
			return nil
		},
		LocalKillCall: func(n string, df string, cn int64) {
			fmt.Printf("local kill con (%s:%s:%d)\n", n, df, cn)
		},
		SynchronizeSegmentCall: func(n string, cn *int64) error {
			fmt.Printf("segment sync con (%s:%d)\n", n, cn)
			return nil
		},
	}
	logger := logging.NewLogger(&logging.LoggerOptions{
		LogLevel:            logging.LevelInfo,
		StandardLoggerFlags: log.Llongfile,
	})
	feedback := make(chan int64, 100)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.SdkURL = "https://sdk.split-stage.io/"
	cfg.AuthServiceURL = "https://auth.split-stage.io"
	authAPI := api.NewAuthAPIClient("", cfg, logger, dtos.Metadata{})
	manager, err := NewManager(logger, syncMock, &cfg, feedback, authAPI)
	if err != nil {
		t.Error("no error expected. Got: ", err)
		return
	}

	go func() {
		for {
			fmt.Println("Goroutines: ", runtime.NumGoroutine())
			time.Sleep(5 * time.Second)
		}
	}()

	go func() {
		for {
			f := <-feedback
			fmt.Println("feedback: ", f)
			if f == StatusUp {
				manager.StartWorkers()
			}
		}
	}()

	manager.Start()

	for {
	}
}
*/
