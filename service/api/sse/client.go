package sse

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-toolkit/v3/logging"
	"github.com/splitio/go-toolkit/v3/sse"
	gtSync "github.com/splitio/go-toolkit/v3/sync"
)

const (
	version   = "1.1"
	keepAlive = 70
)

// StreamingClient interface
type StreamingClient interface {
	ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage))
	StopStreaming()
	IsRunning() bool
}

// StreamingClientImpl struct
type StreamingClientImpl struct {
	mutex     *sync.RWMutex
	sseClient *sse.Client
	running   *gtSync.AtomicBool
	logger    logging.LoggerInterface
	stopped   chan struct{}
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
func NewStreamingClient(cfg *conf.AdvancedConfig, logger logging.LoggerInterface) *StreamingClientImpl {
	sseClient, _ := sse.NewClient(cfg.StreamingServiceURL, keepAlive, logger)
	running := atomic.Value{}
	running.Store(false)

	return &StreamingClientImpl{
		mutex:     &sync.RWMutex{},
		sseClient: sseClient,
		logger:    logger,
		running:   gtSync.NewAtomicBool(false),
		stopped:   make(chan struct{}, 1),
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClientImpl) ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage)) {

	if !s.running.TestAndSet() {
		s.logger.Info("Connection is already in process/running. Ignoring")
		return
	}

	params := make(map[string]string)
	params["channels"] = strings.Join(append(channelList), ",")
	params["accessToken"] = token
	params["v"] = version

	go func() {
		defer func() { s.stopped <- struct{}{} }()
		defer s.running.Unset()
		firstEventReceived := gtSync.NewAtomicBool(false)
		out := s.sseClient.Do(params, func(m IncomingMessage) {
			if firstEventReceived.TestAndSet() && !m.IsError() {
				streamingStatus <- StatusFirstEventOk
			}
			handleIncomingMessage(m)
		})

		if out == nil { // all good
			streamingStatus <- StatusDisconnected
			return
		}

		asConnectionFailedError := &sse.ErrConnectionFailed{}
		if errors.As(out, &asConnectionFailedError) {
			streamingStatus <- StatusConnectionFailed
			return
		}

		switch out {
		case sse.ErrAlreadyRunning:
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
	if !s.running.IsSet() {
		s.logger.Info("SSE client wrapper not running. Ignoring")
		return
	}
	s.sseClient.Shutdown(true)
	<-s.stopped
	s.logger.Info("Stopped streaming")
}

// IsRunning returns true if the client is running
func (s *StreamingClientImpl) IsRunning() bool {
	return s.running.IsSet()
}
