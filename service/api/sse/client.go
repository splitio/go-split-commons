package sse

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/splitio/go-split-commons/v9/conf"
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/service/api"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/sse"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
	gtSync "github.com/splitio/go-toolkit/v5/sync"
)

const (
	version   = "1.1"
	keepAlive = 70
)

// Client state constants for atomic state management
const (
	StateIdle      int32 = 0  // Client is idle/not started
	StateRunning   int32 = 1  // Client is running
	StateDestroyed int32 = -1 // Client is destroyed/stopped
)

// StreamingClient interface
type StreamingClient interface {
	ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage))
	StopStreaming()
	IsRunning() bool
}

// StreamingClientImpl struct
type StreamingClientImpl struct {
	sseClient        *sse.Client
	logger           logging.LoggerInterface
	lifecycle        lifecycle.Manager
	metadata         dtos.Metadata
	clientKey        *string
	state            atomic.Int32  // Atomic state: 0=Idle, 1=Running, -1=Destroyed
	goroutineStarted chan struct{} // Signals when goroutine has started
}

// Status constants
const (
	StatusConnectionFailed = iota
	StatusUnderlyingClientInUse
	StatusFirstEventOk
	StatusDisconnected
)

// IncomingMessage is an alias of sse.RawEvent
type IncomingMessage = sse.RawEvent

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata, clientKey *string) *StreamingClientImpl {
	sseClient, _ := sse.NewClient(cfg.StreamingServiceURL, keepAlive, cfg.HTTPTimeout, logger)
	client := &StreamingClientImpl{
		sseClient: sseClient,
		logger:    logger,
		metadata:  metadata,
		clientKey: clientKey,
	}
	client.state.Store(StateIdle)
	client.lifecycle.Setup()
	return client
}

// ConnectStreaming connects to streaming
func (s *StreamingClientImpl) ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage)) {
	fmt.Println("VAMOS A VERRRRRRRR")
	// Atomic state check: Only proceed if state is Idle (0)
	if s.state.Load() != StateIdle {
		s.logger.Info("Client is not in idle state (already running or destroyed). Ignoring")
		return
	}

	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Connection is already in process/running. Ignoring")
		s.state.Store(StateIdle) // Reset state since lifecycle check failed
		return
	}

	// Now that lifecycle check passed, atomically transition to Running
	// This ensures we only set Running if goroutine will actually be spawned
	if !s.state.CompareAndSwap(StateIdle, StateRunning) {
		s.logger.Info("Client destroyed during initialization. Aborting")
		// Lifecycle was started but we're not spawning goroutine, need to complete it
		s.lifecycle.ShutdownComplete()
		return
	}

	params := make(map[string]string)
	params["channels"] = strings.Join(append(channelList), ",")
	params["accessToken"] = token
	params["v"] = version

	// Channel to signal goroutine has started (prevents race with StopStreaming)
	// Must be created AFTER lifecycle check to ensure goroutine will be spawned
	goroutineStarted := make(chan struct{})
	s.goroutineStarted = goroutineStarted

	go func() {
		// Signal that goroutine has started executing
		close(goroutineStarted)

		defer func() {
			s.lifecycle.ShutdownComplete()
			// Reset to idle if not destroyed
			if s.state.Load() != StateDestroyed {
				s.state.Store(StateIdle)
			}
		}()

		// Early exit if client was destroyed while goroutine was starting
		if s.state.Load() <= StateIdle {
			s.logger.Info("Client state is not valid (destroyed or idle). Exiting goroutine")
			return
		}

		if !s.lifecycle.InitializationComplete() {
			return
		}

		// Helper to send status without blocking if destroyed
		sendStatus := func(status int) {
			currentState := s.state.Load()
			if currentState == StateDestroyed {
				// Client is being destroyed, don't block
				select {
				case streamingStatus <- status:
				default:
					s.logger.Debug("Client destroyed, skipping status send")
				}
				return
			}
			// Normal operation: blocking send is safe
			streamingStatus <- status
		}

		// Final check before starting connection - prevent race if Destroy was called
		if s.state.Load() != StateRunning {
			s.logger.Info("Client destroyed before connection started. Exiting goroutine")
			return
		}

		firstEventReceived := gtSync.NewAtomicBool(false)
		out := s.sseClient.Do(params, api.AddMetadataToHeaders(s.metadata, nil, s.clientKey), func(m IncomingMessage) {
			if firstEventReceived.TestAndSet() && !m.IsError() {
				sendStatus(StatusFirstEventOk)
			}
			handleIncomingMessage(m)
		})

		if out == nil { // all good
			sendStatus(StatusDisconnected)
			return
		}

		// Something didn'g go as expected
		s.lifecycle.AbnormalShutdown()

		asConnectionFailedError := &sse.ErrConnectionFailed{}
		if errors.As(out, &asConnectionFailedError) {
			sendStatus(StatusConnectionFailed)
			return
		}

		switch out {
		case sse.ErrNotIdle:
			// If this happens we have a bug
			sendStatus(StatusUnderlyingClientInUse)
		case sse.ErrReadingStream:
			sendStatus(StatusDisconnected)
		case sse.ErrTimeout:
			sendStatus(StatusDisconnected)
		default:
		}
	}()
}

// StopStreaming stops streaming
func (s *StreamingClientImpl) StopStreaming() {
	// Check old state before destroying - we need to know if a goroutine was spawned
	oldState := s.state.Swap(StateDestroyed)

	// Always shutdown the underlying SSE client to unblock any in-flight connections
	s.sseClient.Shutdown(true)

	// If we were running, a goroutine exists and will call ShutdownComplete()
	if oldState == StateRunning {
		// Wait for goroutine to actually start before waiting for completion
		// This prevents race where goroutine is queued but not started
		if s.goroutineStarted != nil {
			<-s.goroutineStarted
		}
		// Try to begin shutdown (might fail if goroutine still initializing)
		s.lifecycle.BeginShutdown()
		// But always wait for the goroutine to complete
		s.lifecycle.AwaitShutdownComplete()
		s.logger.Info("Stopped streaming")
		return
	}

	// No goroutine was spawned, safe to return
	s.logger.Info("SSE client wrapper not running. Ignoring")
}

// IsRunning returns true if the client is running
func (s *StreamingClientImpl) IsRunning() bool {
	return s.lifecycle.IsRunning() && s.state.Load() == StateRunning
}
