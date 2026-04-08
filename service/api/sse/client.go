package sse

import (
	"errors"
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
	sseClient *sse.Client
	logger    logging.LoggerInterface
	lifecycle lifecycle.Manager
	metadata  dtos.Metadata
	clientKey *string
	state     atomic.Int32 // Atomic state: 0=Idle, 1=Running, -1=Destroyed
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
	if !s.state.CompareAndSwap(StateIdle, StateRunning) {
		s.logger.Info("Client is not in idle state (already running or destroyed). Ignoring")
		return
	}
	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Connection is already in process/running. Ignoring")
		s.state.Store(StateIdle) // Reset state since lifecycle check failed
		return
	}

	params := make(map[string]string)
	params["channels"] = strings.Join(append(channelList), ",")
	params["accessToken"] = token
	params["v"] = version

	go func() {
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
		// Final check before starting connection - prevent race if Destroy was called
		if s.state.Load() != StateRunning {
			s.logger.Info("Client destroyed before connection started. Exiting goroutine")
			return
		}
		firstEventReceived := gtSync.NewAtomicBool(false)
		out := s.sseClient.Do(params, api.AddMetadataToHeaders(s.metadata, nil, s.clientKey), func(m IncomingMessage) {
			if firstEventReceived.TestAndSet() && !m.IsError() {
				streamingStatus <- StatusFirstEventOk
			}
			handleIncomingMessage(m)
		})

		if out == nil { // all good
			streamingStatus <- StatusDisconnected
			return
		}

		// Something didn'g go as expected
		s.lifecycle.AbnormalShutdown()

		asConnectionFailedError := &sse.ErrConnectionFailed{}
		if errors.As(out, &asConnectionFailedError) {
			streamingStatus <- StatusConnectionFailed
			return
		}

		switch out {
		case sse.ErrNotIdle:
			// If this happens we have a bug
			streamingStatus <- StatusUnderlyingClientInUse
		case sse.ErrReadingStream:
			streamingStatus <- StatusDisconnected
		case sse.ErrTimeout:
			streamingStatus <- StatusDisconnected
		default:
		}
	}()
}

// StopStreaming stops streaming
func (s *StreamingClientImpl) StopStreaming() {
	// Set atomic state to destroyed immediately to prevent new goroutines
	s.state.Store(StateDestroyed)
	if !s.lifecycle.BeginShutdown() {
		s.logger.Info("SSE client wrapper not running. Ignoring")
		return
	}
	s.sseClient.Shutdown(true)
	s.lifecycle.AwaitShutdownComplete()
	s.logger.Info("Stopped streaming")
}

// IsRunning returns true if the client is running
func (s *StreamingClientImpl) IsRunning() bool {
	return s.lifecycle.IsRunning() && s.state.Load() == StateRunning
}
